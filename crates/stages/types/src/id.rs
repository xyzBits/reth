/// Stage IDs for all known stages.
///
/// For custom stages, use [`StageId::Other`]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageId {
    /// Static File stage in the process.
    #[deprecated(
        note = "Static Files are generated outside of the pipeline and do not require a separate stage"
    )]
    StaticFile,
    /// Header stage in the process.
    Headers,
    /// Bodies stage in the process.
    Bodies,
    /// Sender recovery stage in the process.
    SenderRecovery,
    /// Execution stage in the process.
    Execution,
    /// Merkle unwind stage in the process.
    MerkleUnwind,
    /// Account hashing stage in the process.
    AccountHashing,
    /// Storage hashing stage in the process.
    StorageHashing,
    /// Merkle execute stage in the process.
    MerkleExecute,
    /// Transaction lookup stage in the process.
    TransactionLookup,
    /// Index storage history stage in the process.
    IndexStorageHistory,
    /// Index account history stage in the process.
    IndexAccountHistory,
    /// Finish stage in the process.
    Finish,
    /// Other custom stage with a provided string identifier.
    Other(&'static str),
}

impl StageId {
    /// All supported Stages
    pub const ALL: [Self; 12] = [
        Self::Headers,
        Self::Bodies,
        Self::SenderRecovery,
        Self::Execution,
        Self::MerkleUnwind,
        Self::AccountHashing,
        Self::StorageHashing,
        Self::MerkleExecute,
        Self::TransactionLookup,
        Self::IndexStorageHistory,
        Self::IndexAccountHistory,
        Self::Finish,
    ];

    /// Stages that require state.
    pub const STATE_REQUIRED: [Self; 7] = [
        Self::Execution,
        Self::MerkleUnwind,
        Self::AccountHashing,
        Self::StorageHashing,
        Self::MerkleExecute,
        Self::IndexStorageHistory,
        Self::IndexAccountHistory,
    ];

    /// Return stage id formatted as string.
    pub const fn as_str(&self) -> &str {
        match self {
            #[allow(deprecated)]
            Self::StaticFile => "StaticFile",
            Self::Headers => "Headers",
            Self::Bodies => "Bodies",
            Self::SenderRecovery => "SenderRecovery",
            Self::Execution => "Execution",
            Self::MerkleUnwind => "MerkleUnwind",
            Self::AccountHashing => "AccountHashing",
            Self::StorageHashing => "StorageHashing",
            Self::MerkleExecute => "MerkleExecute",
            Self::TransactionLookup => "TransactionLookup",
            Self::IndexAccountHistory => "IndexAccountHistory",
            Self::IndexStorageHistory => "IndexStorageHistory",
            Self::Finish => "Finish",
            Self::Other(s) => s,
        }
    }

    /// Returns true if it's a downloading stage [`StageId::Headers`] or [`StageId::Bodies`]
    pub const fn is_downloading_stage(&self) -> bool {
        matches!(self, Self::Headers | Self::Bodies)
    }

    /// Returns `true` if it's [`TransactionLookup`](StageId::TransactionLookup) stage.
    pub const fn is_tx_lookup(&self) -> bool {
        matches!(self, Self::TransactionLookup)
    }

    /// Returns true indicating if it's the finish stage [`StageId::Finish`]
    pub const fn is_finish(&self) -> bool {
        matches!(self, Self::Finish)
    }
}

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_id_as_string() {
        assert_eq!(StageId::Headers.to_string(), "Headers");
        assert_eq!(StageId::Bodies.to_string(), "Bodies");
        assert_eq!(StageId::SenderRecovery.to_string(), "SenderRecovery");
        assert_eq!(StageId::Execution.to_string(), "Execution");
        assert_eq!(StageId::MerkleUnwind.to_string(), "MerkleUnwind");
        assert_eq!(StageId::AccountHashing.to_string(), "AccountHashing");
        assert_eq!(StageId::StorageHashing.to_string(), "StorageHashing");
        assert_eq!(StageId::MerkleExecute.to_string(), "MerkleExecute");
        assert_eq!(StageId::IndexAccountHistory.to_string(), "IndexAccountHistory");
        assert_eq!(StageId::IndexStorageHistory.to_string(), "IndexStorageHistory");
        assert_eq!(StageId::TransactionLookup.to_string(), "TransactionLookup");
        assert_eq!(StageId::Finish.to_string(), "Finish");

        assert_eq!(StageId::Other("Foo").to_string(), "Foo");
    }

    #[test]
    fn is_downloading_stage() {
        assert!(StageId::Headers.is_downloading_stage());
        assert!(StageId::Bodies.is_downloading_stage());

        assert!(!StageId::Execution.is_downloading_stage());
    }

    // Multiple places around the codebase assume headers is the first stage.
    // Feel free to remove this test if the assumption changes.
    #[test]
    fn stage_all_headers_first() {
        assert_eq!(*StageId::ALL.first().unwrap(), StageId::Headers);
    }
}
