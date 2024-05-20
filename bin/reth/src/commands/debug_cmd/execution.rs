//! Command for debugging execution.

use crate::{
    args::{
        get_secret_key,
        utils::{chain_help, genesis_value_parser, SUPPORTED_CHAINS},
        DatabaseArgs, NetworkArgs,
    },
    dirs::{DataDirPath, MaybePlatformPath},
    macros::block_executor,
    utils::get_single_header,
};
use clap::Parser;
use futures::stream::select as stream_select;
use reth_beacon_consensus::EthBeaconConsensus;
use reth_cli_runner::CliContext;
use reth_config::{config::EtlConfig, Config};
use reth_consensus::Consensus;
use reth_db::{database::Database, init_db, DatabaseEnv};
use reth_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use reth_exex::ExExManagerHandle;
use reth_fs_util as fs;
use reth_interfaces::p2p::{bodies::client::BodiesClient, headers::client::HeadersClient};
use reth_network::{NetworkEvents, NetworkHandle};
use reth_network_api::NetworkInfo;
use reth_node_core::init::init_genesis;
use reth_primitives::{
    stage::StageId, BlockHashOrNumber, BlockNumber, ChainSpec, PruneModes, B256,
};
use reth_provider::{
    BlockExecutionWriter, HeaderSyncMode, ProviderFactory, StageCheckpointReader,
    StaticFileProviderFactory,
};
use reth_stages::{
    sets::DefaultStages,
    stages::{ExecutionStage, ExecutionStageThresholds},
    Pipeline, StageSet,
};
use reth_static_file::StaticFileProducer;
use reth_tasks::TaskExecutor;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// `reth debug execution` command
#[derive(Debug, Parser)]
pub struct Command {
    /// The path to the data dir for all reth files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/reth/` or `$HOME/.local/share/reth/`
    /// - Windows: `{FOLDERID_RoamingAppData}/reth/`
    /// - macOS: `$HOME/Library/Application Support/reth/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    datadir: MaybePlatformPath<DataDirPath>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        long_help = chain_help(),
        default_value = SUPPORTED_CHAINS[0],
        value_parser = genesis_value_parser
    )]
    chain: Arc<ChainSpec>,

    #[command(flatten)]
    network: NetworkArgs,

    #[command(flatten)]
    db: DatabaseArgs,

    /// The maximum block height.
    #[arg(long)]
    pub to: u64,

    /// The block interval for sync and unwind.
    /// Defaults to `1000`.
    #[arg(long, default_value = "1000")]
    pub interval: u64,
}

impl Command {
    fn build_pipeline<DB, Client>(
        &self,
        config: &Config,
        client: Client,
        consensus: Arc<dyn Consensus>,
        provider_factory: ProviderFactory<DB>,
        task_executor: &TaskExecutor,
        static_file_producer: StaticFileProducer<DB>,
    ) -> eyre::Result<Pipeline<DB>>
    where
        DB: Database + Unpin + Clone + 'static,
        Client: HeadersClient + BodiesClient + Clone + 'static,
    {
        // building network downloaders using the fetch client
        let header_downloader = ReverseHeadersDownloaderBuilder::new(config.stages.headers)
            .build(client.clone(), Arc::clone(&consensus))
            .into_task_with(task_executor);

        let body_downloader = BodiesDownloaderBuilder::new(config.stages.bodies)
            .build(client, Arc::clone(&consensus), provider_factory.clone())
            .into_task_with(task_executor);

        let stage_conf = &config.stages;
        let prune_modes = config.prune.clone().map(|prune| prune.segments).unwrap_or_default();

        let (tip_tx, tip_rx) = watch::channel(B256::ZERO);
        let executor = block_executor!(self.chain.clone());

        let header_mode = HeaderSyncMode::Tip(tip_rx);
        let pipeline = Pipeline::builder()
            .with_tip_sender(tip_tx)
            .add_stages(
                DefaultStages::new(
                    provider_factory.clone(),
                    header_mode,
                    Arc::clone(&consensus),
                    header_downloader,
                    body_downloader,
                    executor.clone(),
                    stage_conf.clone(),
                    prune_modes.clone(),
                )
                .set(ExecutionStage::new(
                    executor,
                    ExecutionStageThresholds {
                        max_blocks: None,
                        max_changes: None,
                        max_cumulative_gas: None,
                        max_duration: None,
                    },
                    stage_conf.execution_external_clean_threshold(),
                    prune_modes,
                    ExExManagerHandle::empty(),
                )),
            )
            .build(provider_factory, static_file_producer);

        Ok(pipeline)
    }

    async fn build_network(
        &self,
        config: &Config,
        task_executor: TaskExecutor,
        db: Arc<DatabaseEnv>,
        network_secret_path: PathBuf,
        default_peers_path: PathBuf,
    ) -> eyre::Result<NetworkHandle> {
        let secret_key = get_secret_key(&network_secret_path)?;
        let network = self
            .network
            .network_config(config, self.chain.clone(), secret_key, default_peers_path)
            .with_task_executor(Box::new(task_executor))
            .listener_addr(SocketAddr::new(self.network.addr, self.network.port))
            .discovery_addr(SocketAddr::new(
                self.network.discovery.addr,
                self.network.discovery.port,
            ))
            .build(ProviderFactory::new(
                db,
                self.chain.clone(),
                self.datadir.unwrap_or_chain_default(self.chain.chain).static_files(),
            )?)
            .start_network()
            .await?;
        info!(target: "reth::cli", peer_id = %network.peer_id(), local_addr = %network.local_addr(), "Connected to P2P network");
        debug!(target: "reth::cli", peer_id = ?network.peer_id(), "Full peer ID");
        Ok(network)
    }

    async fn fetch_block_hash<Client: HeadersClient>(
        &self,
        client: Client,
        block: BlockNumber,
    ) -> eyre::Result<B256> {
        info!(target: "reth::cli", ?block, "Fetching block from the network.");
        loop {
            match get_single_header(&client, BlockHashOrNumber::Number(block)).await {
                Ok(tip_header) => {
                    info!(target: "reth::cli", ?block, "Successfully fetched block");
                    return Ok(tip_header.hash())
                }
                Err(error) => {
                    error!(target: "reth::cli", ?block, %error, "Failed to fetch the block. Retrying...");
                }
            }
        }
    }

    /// Execute `execution-debug` command
    pub async fn execute(self, ctx: CliContext) -> eyre::Result<()> {
        let mut config = Config::default();

        let data_dir = self.datadir.unwrap_or_chain_default(self.chain.chain);
        let db_path = data_dir.db();

        // Make sure ETL doesn't default to /tmp/, but to whatever datadir is set to
        if config.stages.etl.dir.is_none() {
            config.stages.etl.dir = Some(EtlConfig::from_datadir(data_dir.data_dir()));
        }

        fs::create_dir_all(&db_path)?;
        let db = Arc::new(init_db(db_path, self.db.database_args())?);
        let provider_factory =
            ProviderFactory::new(db.clone(), self.chain.clone(), data_dir.static_files())?;

        debug!(target: "reth::cli", chain=%self.chain.chain, genesis=?self.chain.genesis_hash(), "Initializing genesis");
        init_genesis(provider_factory.clone())?;

        let consensus: Arc<dyn Consensus> =
            Arc::new(EthBeaconConsensus::new(Arc::clone(&self.chain)));

        // Configure and build network
        let network_secret_path =
            self.network.p2p_secret_key.clone().unwrap_or_else(|| data_dir.p2p_secret());
        let network = self
            .build_network(
                &config,
                ctx.task_executor.clone(),
                db.clone(),
                network_secret_path,
                data_dir.known_peers(),
            )
            .await?;

        let static_file_producer = StaticFileProducer::new(
            provider_factory.clone(),
            provider_factory.static_file_provider(),
            PruneModes::default(),
        );

        // Configure the pipeline
        let fetch_client = network.fetch_client().await?;
        let mut pipeline = self.build_pipeline(
            &config,
            fetch_client.clone(),
            Arc::clone(&consensus),
            provider_factory.clone(),
            &ctx.task_executor,
            static_file_producer,
        )?;

        let provider = provider_factory.provider()?;

        let latest_block_number =
            provider.get_stage_checkpoint(StageId::Finish)?.map(|ch| ch.block_number);
        if latest_block_number.unwrap_or_default() >= self.to {
            info!(target: "reth::cli", latest = latest_block_number, "Nothing to run");
            return Ok(())
        }

        let pipeline_events = pipeline.events();
        let events = stream_select(
            reth_node_events::node::handle_broadcast_stream(network.event_listener()),
            reth_node_events::node::handle_broadcast_stream(pipeline_events),
        );
        ctx.task_executor.spawn_critical(
            "events task",
            reth_node_events::node::handle_events(
                Some(network.clone()),
                latest_block_number,
                events,
                db.clone(),
            ),
        );

        let mut current_max_block = latest_block_number.unwrap_or_default();
        while current_max_block < self.to {
            let next_block = current_max_block + 1;
            let target_block = self.to.min(current_max_block + self.interval);
            let target_block_hash =
                self.fetch_block_hash(fetch_client.clone(), target_block).await?;

            // Run the pipeline
            info!(target: "reth::cli", from = next_block, to = target_block, tip = ?target_block_hash, "Starting pipeline");
            pipeline.set_tip(target_block_hash);
            let result = pipeline.run_loop().await?;
            trace!(target: "reth::cli", from = next_block, to = target_block, tip = ?target_block_hash, ?result, "Pipeline finished");

            // Unwind the pipeline without committing.
            {
                provider_factory
                    .provider_rw()?
                    .take_block_and_execution_range(next_block..=target_block)?;
            }

            // Update latest block
            current_max_block = target_block;
        }

        Ok(())
    }
}
