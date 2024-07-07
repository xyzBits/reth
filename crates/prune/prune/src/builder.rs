use crate::{segments::SegmentSet, Pruner};
use reth_chainspec::MAINNET;
use reth_config::PruneConfig;
use reth_db_api::database::Database;
use reth_exex_types::FinishedExExHeight;
use reth_provider::ProviderFactory;
use reth_prune_types::PruneModes;
use std::time::Duration;
use tokio::sync::watch;

/// Contains the information required to build a pruner
#[derive(Debug, Clone)]
pub struct PrunerBuilder {
    /// Minimum pruning interval measured in blocks.
    block_interval: usize,
    /// Pruning configuration for every part of the data that can be pruned.
    segments: PruneModes,
    /// The number of blocks that can be re-orged.
    max_reorg_depth: usize,
    /// The delete limit for pruner, per block. In the actual pruner run it will be multiplied by
    /// the amount of blocks between pruner runs to account for the difference in amount of new
    /// data coming in.
    prune_delete_limit: usize,
    /// Time a pruner job can run before timing out.
    timeout: Option<Duration>,
    /// The finished height of all `ExEx`'s.
    finished_exex_height: watch::Receiver<FinishedExExHeight>,
}

impl PrunerBuilder {
    /// Default timeout for a prune run.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(100);

    /// Creates a new [`PrunerBuilder`] from the given [`PruneConfig`].
    pub fn new(pruner_config: PruneConfig) -> Self {
        Self::default()
            .block_interval(pruner_config.block_interval)
            .segments(pruner_config.segments)
    }

    /// Sets the minimum pruning interval measured in blocks.
    pub const fn block_interval(mut self, block_interval: usize) -> Self {
        self.block_interval = block_interval;
        self
    }

    /// Sets the configuration for every part of the data that can be pruned.
    pub fn segments(mut self, segments: PruneModes) -> Self {
        self.segments = segments;
        self
    }

    /// Sets the number of blocks that can be re-orged.
    pub const fn max_reorg_depth(mut self, max_reorg_depth: usize) -> Self {
        self.max_reorg_depth = max_reorg_depth;
        self
    }

    /// Sets the delete limit for pruner, per block.
    pub const fn prune_delete_limit(mut self, prune_delete_limit: usize) -> Self {
        self.prune_delete_limit = prune_delete_limit;
        self
    }

    /// Sets the timeout for pruner, per run.
    ///
    /// CAUTION: Account and Storage History prune segments treat this timeout as a soft limit,
    /// meaning they can go beyond it.
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the receiver for the finished height of all `ExEx`'s.
    pub fn finished_exex_height(
        mut self,
        finished_exex_height: watch::Receiver<FinishedExExHeight>,
    ) -> Self {
        self.finished_exex_height = finished_exex_height;
        self
    }

    /// Builds a [Pruner] from the current configuration.
    pub fn build<DB: Database>(self, provider_factory: ProviderFactory<DB>) -> Pruner<DB> {
        let segments = SegmentSet::<DB>::from_prune_modes(self.segments);

        Pruner::new(
            provider_factory,
            segments.into_vec(),
            self.block_interval,
            self.prune_delete_limit,
            self.max_reorg_depth,
            self.timeout,
            self.finished_exex_height,
        )
    }
}

impl Default for PrunerBuilder {
    fn default() -> Self {
        Self {
            block_interval: 5,
            segments: PruneModes::none(),
            max_reorg_depth: 64,
            prune_delete_limit: MAINNET.prune_delete_limit,
            timeout: None,
            finished_exex_height: watch::channel(FinishedExExHeight::NoExExs).1,
        }
    }
}
