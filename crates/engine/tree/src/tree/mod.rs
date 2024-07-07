use crate::{backfill::BackfillAction, engine::DownloadRequest};
use reth_beacon_consensus::{ForkchoiceStateTracker, InvalidHeaderCache, OnForkChoiceUpdated};
use reth_blockchain_tree::{
    error::InsertBlockErrorKind, BlockAttachment, BlockBuffer, BlockStatus,
};
use reth_blockchain_tree_api::{error::InsertBlockError, InsertPayloadOk};
use reth_consensus::{Consensus, PostExecutionInput};
use reth_engine_primitives::EngineTypes;
use reth_errors::{ConsensusError, ProviderResult};
use reth_evm::execute::{BlockExecutorProvider, Executor};
use reth_payload_primitives::PayloadTypes;
use reth_payload_validator::ExecutionPayloadValidator;
use reth_primitives::{
    Address, Block, BlockNumber, Receipts, Requests, SealedBlock, SealedBlockWithSenders, B256,
    U256,
};
use reth_provider::{BlockReader, ExecutionOutcome, StateProvider, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_rpc_types::{
    engine::{
        CancunPayloadFields, ForkchoiceState, PayloadStatus, PayloadStatusEnum,
        PayloadValidationError,
    },
    ExecutionPayload,
};
use reth_trie::{updates::TrieUpdates, HashedPostState};
use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    sync::Arc,
};
use tracing::*;

mod memory_overlay;
pub use memory_overlay::MemoryOverlayStateProvider;

/// Represents an executed block stored in-memory.
#[derive(Clone, Debug)]
pub struct ExecutedBlock {
    block: Arc<SealedBlock>,
    senders: Arc<Vec<Address>>,
    execution_output: Arc<ExecutionOutcome>,
    hashed_state: Arc<HashedPostState>,
    trie: Arc<TrieUpdates>,
}

impl ExecutedBlock {
    /// Returns a reference to the executed block.
    pub(crate) fn block(&self) -> &SealedBlock {
        &self.block
    }

    /// Returns a reference to the block's senders
    pub(crate) fn senders(&self) -> &Vec<Address> {
        &self.senders
    }

    /// Returns a reference to the block's execution outcome
    pub(crate) fn execution_outcome(&self) -> &ExecutionOutcome {
        &self.execution_output
    }

    /// Returns a reference to the hashed state result of the execution outcome
    pub(crate) fn hashed_state(&self) -> &HashedPostState {
        &self.hashed_state
    }

    /// Returns a reference to the trie updates for the block
    pub(crate) fn trie_updates(&self) -> &TrieUpdates {
        &self.trie
    }
}

/// Keeps track of the state of the tree.
#[derive(Debug)]
pub struct TreeState {
    /// All executed blocks by hash.
    blocks_by_hash: HashMap<B256, ExecutedBlock>,
    /// Executed blocks grouped by their respective block number.
    blocks_by_number: BTreeMap<BlockNumber, Vec<ExecutedBlock>>,
}

impl TreeState {
    fn block_by_hash(&self, hash: B256) -> Option<Arc<SealedBlock>> {
        self.blocks_by_hash.get(&hash).map(|b| b.block.clone())
    }

    /// Insert executed block into the state.
    fn insert_executed(&mut self, executed: ExecutedBlock) {
        self.blocks_by_number.entry(executed.block.number).or_default().push(executed.clone());
        let existing = self.blocks_by_hash.insert(executed.block.hash(), executed);
        debug_assert!(existing.is_none(), "inserted duplicate block");
    }

    /// Remove blocks before specified block number.
    pub(crate) fn remove_before(&mut self, block_number: BlockNumber) {
        while self
            .blocks_by_number
            .first_key_value()
            .map(|entry| entry.0 < &block_number)
            .unwrap_or_default()
        {
            let (_, to_remove) = self.blocks_by_number.pop_first().unwrap();
            for block in to_remove {
                let block_hash = block.block.hash();
                let removed = self.blocks_by_hash.remove(&block_hash);
                debug_assert!(
                    removed.is_some(),
                    "attempted to remove non-existing block {block_hash}"
                );
            }
        }
    }
}

/// Tracks the state of the engine api internals.
///
/// This type is shareable.
#[derive(Debug)]
pub struct EngineApiTreeState {
    /// Tracks the state of the blockchain tree.
    tree_state: TreeState,
    /// Tracks the received forkchoice state updates received by the CL.
    forkchoice_state_tracker: ForkchoiceStateTracker,
    /// Buffer of detached blocks.
    buffer: BlockBuffer,
    /// Tracks the header of invalid payloads that were rejected by the engine because they're
    /// invalid.
    invalid_headers: InvalidHeaderCache,
}

/// The type responsible for processing engine API requests.
///
/// TODO: design: should the engine handler functions also accept the response channel or return the
/// result and the caller redirects the response
pub trait EngineApiTreeHandler: Send + Sync {
    /// The engine type that this handler is for.
    type Engine: EngineTypes;

    /// Invoked when previously requested blocks were downloaded.
    fn on_downloaded(&mut self, blocks: Vec<SealedBlockWithSenders>) -> Option<TreeEvent>;

    /// When the Consensus layer receives a new block via the consensus gossip protocol,
    /// the transactions in the block are sent to the execution layer in the form of a
    /// [`ExecutionPayload`]. The Execution layer executes the transactions and validates the
    /// state in the block header, then passes validation data back to Consensus layer, that
    /// adds the block to the head of its own blockchain and attests to it. The block is then
    /// broadcast over the consensus p2p network in the form of a "Beacon block".
    ///
    /// These responses should adhere to the [Engine API Spec for
    /// `engine_newPayload`](https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#specification).
    ///
    /// This returns a [`PayloadStatus`] that represents the outcome of a processed new payload and
    /// returns an error if an internal error occurred.
    fn on_new_payload(
        &mut self,
        payload: ExecutionPayload,
        cancun_fields: Option<CancunPayloadFields>,
    ) -> ProviderResult<TreeOutcome<PayloadStatus>>;

    /// Invoked when we receive a new forkchoice update message. Calls into the blockchain tree
    /// to resolve chain forks and ensure that the Execution Layer is working with the latest valid
    /// chain.
    ///
    /// These responses should adhere to the [Engine API Spec for
    /// `engine_forkchoiceUpdated`](https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#specification-1).
    ///
    /// Returns an error if an internal error occurred like a database error.
    fn on_forkchoice_updated(
        &mut self,
        state: ForkchoiceState,
        attrs: Option<<Self::Engine as PayloadTypes>::PayloadAttributes>,
    ) -> TreeOutcome<Result<OnForkChoiceUpdated, String>>;
}

/// The outcome of a tree operation.
#[derive(Debug)]
pub struct TreeOutcome<T> {
    /// The outcome of the operation.
    pub outcome: T,
    /// An optional event to tell the caller to do something.
    pub event: Option<TreeEvent>,
}

impl<T> TreeOutcome<T> {
    /// Create new tree outcome.
    pub const fn new(outcome: T) -> Self {
        Self { outcome, event: None }
    }

    /// Set event on the outcome.
    pub fn with_event(mut self, event: TreeEvent) -> Self {
        self.event = Some(event);
        self
    }
}

/// Events that can be emitted by the [`EngineApiTreeHandler`].
#[derive(Debug)]
pub enum TreeEvent {
    /// Tree action is needed.
    TreeAction(TreeAction),
    /// Backfill action is needed.
    BackfillAction(BackfillAction),
    /// Block download is needed.
    Download(DownloadRequest),
}

/// The actions that can be performed on the tree.
#[derive(Debug)]
pub enum TreeAction {
    /// Make target canonical.
    MakeCanonical(B256),
}

#[derive(Debug)]
pub struct EngineApiTreeHandlerImpl<P, E, T: EngineTypes> {
    provider: P,
    executor_provider: E,
    consensus: Arc<dyn Consensus>,
    payload_validator: ExecutionPayloadValidator,
    state: EngineApiTreeState,
    /// (tmp) The flag indicating whether the pipeline is active.
    is_pipeline_active: bool,
    _marker: PhantomData<T>,
}

impl<P, E, T> EngineApiTreeHandlerImpl<P, E, T>
where
    P: BlockReader + StateProviderFactory,
    E: BlockExecutorProvider,
    T: EngineTypes,
{
    /// Return block from database or in-memory state by hash.
    fn block_by_hash(&self, hash: B256) -> ProviderResult<Option<Block>> {
        // check database first
        let mut block = self.provider.block_by_hash(hash)?;
        if block.is_none() {
            // Note: it's fine to return the unsealed block because the caller already has
            // the hash
            block = self
                .state
                .tree_state
                .block_by_hash(hash)
                // TODO: clone for compatibility. should we return an Arc here?
                .map(|block| block.as_ref().clone().unseal());
        }
        Ok(block)
    }

    /// Return state provider with reference to in-memory blocks that overlay database state.
    fn state_provider(
        &self,
        hash: B256,
    ) -> ProviderResult<MemoryOverlayStateProvider<Box<dyn StateProvider>>> {
        let mut in_memory = Vec::new();
        let mut parent_hash = hash;
        while let Some(executed) = self.state.tree_state.blocks_by_hash.get(&parent_hash) {
            parent_hash = executed.block.parent_hash;
            in_memory.insert(0, executed.clone());
        }

        let historical = self.provider.state_by_block_hash(parent_hash)?;
        Ok(MemoryOverlayStateProvider::new(in_memory, historical))
    }

    /// Return the parent hash of the lowest buffered ancestor for the requested block, if there
    /// are any buffered ancestors. If there are no buffered ancestors, and the block itself does
    /// not exist in the buffer, this returns the hash that is passed in.
    ///
    /// Returns the parent hash of the block itself if the block is buffered and has no other
    /// buffered ancestors.
    fn lowest_buffered_ancestor_or(&self, hash: B256) -> B256 {
        self.state
            .buffer
            .lowest_ancestor(&hash)
            .map(|block| block.parent_hash)
            .unwrap_or_else(|| hash)
    }

    /// If validation fails, the response MUST contain the latest valid hash:
    ///
    ///   - The block hash of the ancestor of the invalid payload satisfying the following two
    ///     conditions:
    ///     - It is fully validated and deemed VALID
    ///     - Any other ancestor of the invalid payload with a higher blockNumber is INVALID
    ///   - 0x0000000000000000000000000000000000000000000000000000000000000000 if the above
    ///     conditions are satisfied by a `PoW` block.
    ///   - null if client software cannot determine the ancestor of the invalid payload satisfying
    ///     the above conditions.
    fn latest_valid_hash_for_invalid_payload(
        &mut self,
        parent_hash: B256,
    ) -> ProviderResult<Option<B256>> {
        // Check if parent exists in side chain or in canonical chain.
        if self.block_by_hash(parent_hash)?.is_some() {
            return Ok(Some(parent_hash))
        }

        // iterate over ancestors in the invalid cache
        // until we encounter the first valid ancestor
        let mut current_hash = parent_hash;
        let mut current_header = self.state.invalid_headers.get(&current_hash);
        while let Some(header) = current_header {
            current_hash = header.parent_hash;
            current_header = self.state.invalid_headers.get(&current_hash);

            // If current_header is None, then the current_hash does not have an invalid
            // ancestor in the cache, check its presence in blockchain tree
            if current_header.is_none() && self.block_by_hash(current_hash)?.is_some() {
                return Ok(Some(current_hash))
            }
        }
        Ok(None)
    }

    /// Prepares the invalid payload response for the given hash, checking the
    /// database for the parent hash and populating the payload status with the latest valid hash
    /// according to the engine api spec.
    fn prepare_invalid_response(&mut self, mut parent_hash: B256) -> ProviderResult<PayloadStatus> {
        // Edge case: the `latestValid` field is the zero hash if the parent block is the terminal
        // PoW block, which we need to identify by looking at the parent's block difficulty
        if let Some(parent) = self.block_by_hash(parent_hash)? {
            if !parent.is_zero_difficulty() {
                parent_hash = B256::ZERO;
            }
        }

        let valid_parent_hash = self.latest_valid_hash_for_invalid_payload(parent_hash)?;
        Ok(PayloadStatus::from_status(PayloadStatusEnum::Invalid {
            validation_error: PayloadValidationError::LinksToRejectedPayload.to_string(),
        })
        .with_latest_valid_hash(valid_parent_hash.unwrap_or_default()))
    }

    /// Checks if the given `check` hash points to an invalid header, inserting the given `head`
    /// block into the invalid header cache if the `check` hash has a known invalid ancestor.
    ///
    /// Returns a payload status response according to the engine API spec if the block is known to
    /// be invalid.
    fn check_invalid_ancestor_with_head(
        &mut self,
        check: B256,
        head: B256,
    ) -> ProviderResult<Option<PayloadStatus>> {
        // check if the check hash was previously marked as invalid
        let Some(header) = self.state.invalid_headers.get(&check) else { return Ok(None) };

        // populate the latest valid hash field
        let status = self.prepare_invalid_response(header.parent_hash)?;

        // insert the head block into the invalid header cache
        self.state.invalid_headers.insert_with_invalid_ancestor(head, header);

        Ok(Some(status))
    }

    /// Validate if block is correct and satisfies all the consensus rules that concern the header
    /// and block body itself.
    fn validate_block(&self, block: &SealedBlockWithSenders) -> Result<(), ConsensusError> {
        if let Err(e) = self.consensus.validate_header_with_total_difficulty(block, U256::MAX) {
            error!(
                ?block,
                "Failed to validate total difficulty for block {}: {e}",
                block.header.hash()
            );
            return Err(e)
        }

        if let Err(e) = self.consensus.validate_header(block) {
            error!(?block, "Failed to validate header {}: {e}", block.header.hash());
            return Err(e)
        }

        if let Err(e) = self.consensus.validate_block_pre_execution(block) {
            error!(?block, "Failed to validate block {}: {e}", block.header.hash());
            return Err(e)
        }

        Ok(())
    }

    fn buffer_block_without_senders(&mut self, block: SealedBlock) -> Result<(), InsertBlockError> {
        match block.try_seal_with_senders() {
            Ok(block) => self.buffer_block(block),
            Err(block) => Err(InsertBlockError::sender_recovery_error(block)),
        }
    }

    fn buffer_block(&mut self, block: SealedBlockWithSenders) -> Result<(), InsertBlockError> {
        if let Err(err) = self.validate_block(&block) {
            return Err(InsertBlockError::consensus_error(err, block.block))
        }
        self.state.buffer.insert_block(block);
        Ok(())
    }

    fn insert_block_without_senders(
        &mut self,
        block: SealedBlock,
    ) -> Result<InsertPayloadOk, InsertBlockError> {
        match block.try_seal_with_senders() {
            Ok(block) => self.insert_block(block),
            Err(block) => Err(InsertBlockError::sender_recovery_error(block)),
        }
    }

    fn insert_block(
        &mut self,
        block: SealedBlockWithSenders,
    ) -> Result<InsertPayloadOk, InsertBlockError> {
        self.insert_block_inner(block.clone())
            .map_err(|kind| InsertBlockError::new(block.block, kind))
    }

    fn insert_block_inner(
        &mut self,
        block: SealedBlockWithSenders,
    ) -> Result<InsertPayloadOk, InsertBlockErrorKind> {
        if self.block_by_hash(block.hash())?.is_some() {
            let attachment = BlockAttachment::Canonical; // TODO: remove or revise attachment
            return Ok(InsertPayloadOk::AlreadySeen(BlockStatus::Valid(attachment)))
        }

        // validate block consensus rules
        self.validate_block(&block)?;

        let state_provider = self.state_provider(block.parent_hash).unwrap();
        let executor = self.executor_provider.executor(StateProviderDatabase::new(&state_provider));

        let block_number = block.number;
        let block_hash = block.hash();
        let block = block.unseal();
        let output = executor.execute((&block, U256::MAX).into()).unwrap();
        self.consensus.validate_block_post_execution(
            &block,
            PostExecutionInput::new(&output.receipts, &output.requests),
        )?;

        let hashed_state = HashedPostState::from_bundle_state(&output.state.state);

        // TODO: compute and validate state root
        let trie_output = TrieUpdates::default();

        let executed = ExecutedBlock {
            block: Arc::new(block.block.seal(block_hash)),
            senders: Arc::new(block.senders),
            execution_output: Arc::new(ExecutionOutcome::new(
                output.state,
                Receipts::from(output.receipts),
                block_number,
                vec![Requests::from(output.requests)],
            )),
            hashed_state: Arc::new(hashed_state),
            trie: Arc::new(trie_output),
        };
        self.state.tree_state.insert_executed(executed);

        let attachment = BlockAttachment::Canonical; // TODO: remove or revise attachment
        Ok(InsertPayloadOk::Inserted(BlockStatus::Valid(attachment)))
    }
}

impl<P, E, T> EngineApiTreeHandler for EngineApiTreeHandlerImpl<P, E, T>
where
    P: BlockReader + StateProviderFactory + Clone,
    E: BlockExecutorProvider,
    T: EngineTypes,
{
    type Engine = T;

    fn on_downloaded(&mut self, _blocks: Vec<SealedBlockWithSenders>) -> Option<TreeEvent> {
        todo!()
    }

    fn on_new_payload(
        &mut self,
        payload: ExecutionPayload,
        cancun_fields: Option<CancunPayloadFields>,
    ) -> ProviderResult<TreeOutcome<PayloadStatus>> {
        // Ensures that the given payload does not violate any consensus rules that concern the
        // block's layout, like:
        //    - missing or invalid base fee
        //    - invalid extra data
        //    - invalid transactions
        //    - incorrect hash
        //    - the versioned hashes passed with the payload do not exactly match transaction
        //      versioned hashes
        //    - the block does not contain blob transactions if it is pre-cancun
        //
        // This validates the following engine API rule:
        //
        // 3. Given the expected array of blob versioned hashes client software **MUST** run its
        //    validation by taking the following steps:
        //
        //   1. Obtain the actual array by concatenating blob versioned hashes lists
        //      (`tx.blob_versioned_hashes`) of each [blob
        //      transaction](https://eips.ethereum.org/EIPS/eip-4844#new-transaction-type) included
        //      in the payload, respecting the order of inclusion. If the payload has no blob
        //      transactions the expected array **MUST** be `[]`.
        //
        //   2. Return `{status: INVALID, latestValidHash: null, validationError: errorMessage |
        //      null}` if the expected and the actual arrays don't match.
        //
        // This validation **MUST** be instantly run in all cases even during active sync process.
        let parent_hash = payload.parent_hash();
        let block = match self
            .payload_validator
            .ensure_well_formed_payload(payload, cancun_fields.into())
        {
            Ok(block) => block,
            Err(error) => {
                error!(target: "engine::tree", %error, "Invalid payload");
                // we need to convert the error to a payload status (response to the CL)

                let latest_valid_hash =
                    if error.is_block_hash_mismatch() || error.is_invalid_versioned_hashes() {
                        // Engine-API rules:
                        // > `latestValidHash: null` if the blockHash validation has failed (<https://github.com/ethereum/execution-apis/blob/fe8e13c288c592ec154ce25c534e26cb7ce0530d/src/engine/shanghai.md?plain=1#L113>)
                        // > `latestValidHash: null` if the expected and the actual arrays don't match (<https://github.com/ethereum/execution-apis/blob/fe8e13c288c592ec154ce25c534e26cb7ce0530d/src/engine/cancun.md?plain=1#L103>)
                        None
                    } else {
                        self.latest_valid_hash_for_invalid_payload(parent_hash)?
                    };

                let status = PayloadStatusEnum::from(error);
                return Ok(TreeOutcome::new(PayloadStatus::new(status, latest_valid_hash)))
            }
        };

        let block_hash = block.hash();
        let mut lowest_buffered_ancestor = self.lowest_buffered_ancestor_or(block_hash);
        if lowest_buffered_ancestor == block_hash {
            lowest_buffered_ancestor = block.parent_hash;
        }

        // now check the block itself
        if let Some(status) =
            self.check_invalid_ancestor_with_head(lowest_buffered_ancestor, block_hash)?
        {
            return Ok(TreeOutcome::new(status))
        }

        let status = if self.is_pipeline_active {
            self.buffer_block_without_senders(block).unwrap();
            PayloadStatus::from_status(PayloadStatusEnum::Syncing)
        } else {
            let mut latest_valid_hash = None;
            let status = match self.insert_block_without_senders(block).unwrap() {
                InsertPayloadOk::Inserted(BlockStatus::Valid(_)) |
                InsertPayloadOk::AlreadySeen(BlockStatus::Valid(_)) => {
                    latest_valid_hash = Some(block_hash);
                    PayloadStatusEnum::Valid
                }
                InsertPayloadOk::Inserted(BlockStatus::Disconnected { .. }) |
                InsertPayloadOk::AlreadySeen(BlockStatus::Disconnected { .. }) => {
                    // TODO: isn't this check redundant?
                    // check if the block's parent is already marked as invalid
                    // if let Some(status) = self
                    //     .check_invalid_ancestor_with_head(block.parent_hash, block.hash())
                    //     .map_err(|error| {
                    //         InsertBlockError::new(block, InsertBlockErrorKind::Provider(error))
                    //     })?
                    // {
                    //     return Ok(status)
                    // }

                    // not known to be invalid, but we don't know anything else
                    PayloadStatusEnum::Syncing
                }
            };
            PayloadStatus::new(status, latest_valid_hash)
        };

        let mut outcome = TreeOutcome::new(status);
        if outcome.outcome.is_valid() {
            if let Some(target) = self.state.forkchoice_state_tracker.sync_target_state() {
                if target.head_block_hash == block_hash {
                    outcome = outcome
                        .with_event(TreeEvent::TreeAction(TreeAction::MakeCanonical(block_hash)));
                }
            }
        }
        Ok(outcome)
    }

    fn on_forkchoice_updated(
        &mut self,
        state: ForkchoiceState,
        attrs: Option<<Self::Engine as PayloadTypes>::PayloadAttributes>,
    ) -> TreeOutcome<Result<OnForkChoiceUpdated, String>> {
        todo!()
    }
}
