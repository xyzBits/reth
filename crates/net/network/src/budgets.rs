/// Default budget to try and drain pending messages from [`NetworkHandle`](crate::NetworkHandle)
/// channel.
pub const DEFAULT_BUDGET_TRY_DRAIN_NETWORK_HANDLE_CHANNEL: u32 = 4 * 1024;

/// Default budget to try and flush pending pool imports to pool. This number reflects the number
/// of transactions that can be queued for import to pool in each iteration of the loop in the
/// [`TransactionsManager`](crate::TransactionsManager) future. Default is 3 billion pending pool
/// imports.
pub const DEFAULT_BUDGET_TRY_DRAIN_PENDING_POOL_IMPORTS: u32 = 3 * 1000000000;

/// Default budget to try and stream hashes of successfully imported transactions from the pool.
/// Default is naturally same as the number of transactions to attempt importing,
/// [`DEFAULT_BUDGET_TRY_DRAIN_PENDING_POOL_IMPORTS`], so 3 billion pool imports.
pub const DEFAULT_BUDGET_TRY_DRAIN_POOL_IMPORTS: u32 = 3 * 1000000000;

/// Default budget to try and drain stream of
/// [`NetworkTransactionEvent`](crate::transactions::NetworkTransactionEvent)s from
/// [`NetworkManager`](crate::NetworkManager).
pub const DEFAULT_BUDGET_TRY_DRAIN_NETWORK_TRANSACTION_EVENTS: u32 = 4 * 1024;
