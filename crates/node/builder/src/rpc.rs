//! Builder support for rpc components.

use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use futures::TryFutureExt;
use reth_network::NetworkHandle;
use reth_node_api::FullNodeComponents;
use reth_node_core::{node_config::NodeConfig, rpc::api::EngineApiServer};
use reth_payload_builder::PayloadBuilderHandle;
use reth_rpc::eth::EthApi;
use reth_rpc_builder::{
    auth::{AuthRpcModule, AuthServerHandle},
    config::RethRpcServerConfig,
    EthApiBuild, RpcModuleBuilder, RpcRegistryInner, RpcServerHandle, TransportRpcModules,
};
use reth_rpc_layer::JwtSecret;
use reth_tasks::TaskExecutor;
use reth_tracing::tracing::{debug, info};

/// Contains the handles to the spawned RPC servers.
///
/// This can be used to access the endpoints of the servers.
#[derive(Debug, Clone)]
pub struct RethRpcServerHandles {
    /// The regular RPC server handle to all configured transports.
    pub rpc: RpcServerHandle,
    /// The handle to the auth server (engine API)
    pub auth: AuthServerHandle,
}

/// Contains hooks that are called during the rpc setup.
pub(crate) struct RpcHooks<Node: FullNodeComponents> {
    pub(crate) on_rpc_started: Box<dyn OnRpcStarted<Node>>,
    pub(crate) extend_rpc_modules: Box<dyn ExtendRpcModules<Node>>,
}

impl<Node: FullNodeComponents> RpcHooks<Node> {
    /// Creates a new, empty [`RpcHooks`] instance for the given node type.
    pub(crate) fn new() -> Self {
        Self { on_rpc_started: Box::<()>::default(), extend_rpc_modules: Box::<()>::default() }
    }

    /// Sets the hook that is run once the rpc server is started.
    pub(crate) fn set_on_rpc_started<F>(&mut self, hook: F) -> &mut Self
    where
        F: OnRpcStarted<Node> + 'static,
    {
        self.on_rpc_started = Box::new(hook);
        self
    }

    /// Sets the hook that is run once the rpc server is started.
    #[allow(unused)]
    pub(crate) fn on_rpc_started<F>(mut self, hook: F) -> Self
    where
        F: OnRpcStarted<Node> + 'static,
    {
        self.set_on_rpc_started(hook);
        self
    }

    /// Sets the hook that is run to configure the rpc modules.
    pub(crate) fn set_extend_rpc_modules<F>(&mut self, hook: F) -> &mut Self
    where
        F: ExtendRpcModules<Node> + 'static,
    {
        self.extend_rpc_modules = Box::new(hook);
        self
    }

    /// Sets the hook that is run to configure the rpc modules.
    #[allow(unused)]
    pub(crate) fn extend_rpc_modules<F>(mut self, hook: F) -> Self
    where
        F: ExtendRpcModules<Node> + 'static,
    {
        self.set_extend_rpc_modules(hook);
        self
    }
}

impl<Node: FullNodeComponents> fmt::Debug for RpcHooks<Node> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcHooks")
            .field("on_rpc_started", &"...")
            .field("extend_rpc_modules", &"...")
            .finish()
    }
}

/// Event hook that is called once the rpc server is started.
pub trait OnRpcStarted<Node: FullNodeComponents>: Send {
    /// The hook that is called once the rpc server is started.
    fn on_rpc_started(
        self: Box<Self>,
        ctx: RpcContext<'_, Node>,
        handles: RethRpcServerHandles,
    ) -> eyre::Result<()>;
}

impl<Node, F> OnRpcStarted<Node> for F
where
    F: FnOnce(RpcContext<'_, Node>, RethRpcServerHandles) -> eyre::Result<()> + Send,
    Node: FullNodeComponents,
{
    fn on_rpc_started(
        self: Box<Self>,
        ctx: RpcContext<'_, Node>,
        handles: RethRpcServerHandles,
    ) -> eyre::Result<()> {
        (*self)(ctx, handles)
    }
}

impl<Node: FullNodeComponents> OnRpcStarted<Node> for () {
    fn on_rpc_started(
        self: Box<Self>,
        _: RpcContext<'_, Node>,
        _: RethRpcServerHandles,
    ) -> eyre::Result<()> {
        Ok(())
    }
}

/// Event hook that is called when the rpc server is started.
pub trait ExtendRpcModules<Node: FullNodeComponents>: Send {
    /// The hook that is called once the rpc server is started.
    fn extend_rpc_modules(self: Box<Self>, ctx: RpcContext<'_, Node>) -> eyre::Result<()>;
}

impl<Node, F> ExtendRpcModules<Node> for F
where
    F: FnOnce(RpcContext<'_, Node>) -> eyre::Result<()> + Send,
    Node: FullNodeComponents,
{
    fn extend_rpc_modules(self: Box<Self>, ctx: RpcContext<'_, Node>) -> eyre::Result<()> {
        (*self)(ctx)
    }
}

impl<Node: FullNodeComponents> ExtendRpcModules<Node> for () {
    fn extend_rpc_modules(self: Box<Self>, _: RpcContext<'_, Node>) -> eyre::Result<()> {
        Ok(())
    }
}

/// Helper wrapper type to encapsulate the [`RpcRegistryInner`] over components trait.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct RpcRegistry<Node: FullNodeComponents> {
    pub(crate) registry: RpcRegistryInner<
        Node::Provider,
        Node::Pool,
        NetworkHandle,
        TaskExecutor,
        Node::Provider,
        EthApi<Node::Provider, Node::Pool, NetworkHandle, Node::Evm>,
    >,
}

impl<Node: FullNodeComponents> Deref for RpcRegistry<Node> {
    type Target = RpcRegistryInner<
        Node::Provider,
        Node::Pool,
        NetworkHandle,
        TaskExecutor,
        Node::Provider,
        EthApi<Node::Provider, Node::Pool, NetworkHandle, Node::Evm>,
    >;

    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

impl<Node: FullNodeComponents> DerefMut for RpcRegistry<Node> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.registry
    }
}

impl<Node: FullNodeComponents> Clone for RpcRegistry<Node> {
    fn clone(&self) -> Self {
        Self { registry: self.registry.clone() }
    }
}

/// Helper container to encapsulate [`RpcRegistryInner`], [`TransportRpcModules`] and
/// [`AuthRpcModule`].
///
/// This can be used to access installed modules, or create commonly used handlers like
/// [`reth_rpc::eth::EthApi`], and ultimately merge additional rpc handler into the configured
/// transport modules [`TransportRpcModules`] as well as configured authenticated methods
/// [`AuthRpcModule`].
#[allow(missing_debug_implementations)]
pub struct RpcContext<'a, Node: FullNodeComponents> {
    /// The node components.
    pub(crate) node: Node,

    /// Gives access to the node configuration.
    pub(crate) config: &'a NodeConfig,

    /// A Helper type the holds instances of the configured modules.
    ///
    /// This provides easy access to rpc handlers, such as [`RpcRegistryInner::eth_api`].
    pub registry: &'a mut RpcRegistry<Node>,
    /// Holds installed modules per transport type.
    ///
    /// This can be used to merge additional modules into the configured transports (http, ipc,
    /// ws). See [`TransportRpcModules::merge_configured`]
    pub modules: &'a mut TransportRpcModules,
    /// Holds jwt authenticated rpc module.
    ///
    /// This can be used to merge additional modules into the configured authenticated methods
    pub auth_module: &'a mut AuthRpcModule,
}

impl<'a, Node: FullNodeComponents> RpcContext<'a, Node> {
    /// Returns the config of the node.
    pub const fn config(&self) -> &NodeConfig {
        self.config
    }

    /// Returns a reference to the configured node.
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the transaction pool instance.
    pub fn pool(&self) -> &Node::Pool {
        self.node.pool()
    }

    /// Returns provider to interact with the node.
    pub fn provider(&self) -> &Node::Provider {
        self.node.provider()
    }

    /// Returns the handle to the network
    pub fn network(&self) -> &NetworkHandle {
        self.node.network()
    }

    /// Returns the handle to the payload builder service
    pub fn payload_builder(&self) -> &PayloadBuilderHandle<Node::Engine> {
        self.node.payload_builder()
    }
}

/// Launch the rpc servers.
pub(crate) async fn launch_rpc_servers<Node, Engine>(
    node: Node,
    engine_api: Engine,
    config: &NodeConfig,
    jwt_secret: JwtSecret,
    hooks: RpcHooks<Node>,
) -> eyre::Result<(RethRpcServerHandles, RpcRegistry<Node>)>
where
    Node: FullNodeComponents + Clone,
    Engine: EngineApiServer<Node::Engine>,
{
    let RpcHooks { on_rpc_started, extend_rpc_modules } = hooks;

    let auth_config = config.rpc.auth_server_config(jwt_secret)?;
    let module_config = config.rpc.transport_rpc_module_config();
    debug!(target: "reth::cli", http=?module_config.http(), ws=?module_config.ws(), "Using RPC module config");

    let (mut modules, mut auth_module, registry) = RpcModuleBuilder::default()
        .with_provider(node.provider().clone())
        .with_pool(node.pool().clone())
        .with_network(node.network().clone())
        .with_events(node.provider().clone())
        .with_executor(node.task_executor().clone())
        .with_evm_config(node.evm_config().clone())
        .build_with_auth_server(module_config, engine_api, EthApiBuild::build);

    let mut registry = RpcRegistry { registry };
    let ctx = RpcContext {
        node: node.clone(),
        config,
        registry: &mut registry,
        modules: &mut modules,
        auth_module: &mut auth_module,
    };

    extend_rpc_modules.extend_rpc_modules(ctx)?;

    let server_config = config.rpc.rpc_server_config();
    let launch_rpc = modules.clone().start_server(server_config).map_ok(|handle| {
        if let Some(path) = handle.ipc_endpoint() {
            info!(target: "reth::cli", %path, "RPC IPC server started");
        }
        if let Some(addr) = handle.http_local_addr() {
            info!(target: "reth::cli", url=%addr, "RPC HTTP server started");
        }
        if let Some(addr) = handle.ws_local_addr() {
            info!(target: "reth::cli", url=%addr, "RPC WS server started");
        }
        handle
    });

    let launch_auth = auth_module.clone().start_server(auth_config).map_ok(|handle| {
        let addr = handle.local_addr();
        if let Some(ipc_endpoint) = handle.ipc_endpoint() {
            info!(target: "reth::cli", url=%addr, ipc_endpoint=%ipc_endpoint,"RPC auth server started");
        } else {
            info!(target: "reth::cli", url=%addr, "RPC auth server started");
        }
        handle
    });

    // launch servers concurrently
    let (rpc, auth) = futures::future::try_join(launch_rpc, launch_auth).await?;
    let handles = RethRpcServerHandles { rpc, auth };

    let ctx = RpcContext {
        node,
        config,
        registry: &mut registry,
        modules: &mut modules,
        auth_module: &mut auth_module,
    };

    on_rpc_started.on_rpc_started(ctx, handles.clone())?;

    Ok((handles, registry))
}
