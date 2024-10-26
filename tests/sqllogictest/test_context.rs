use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_functions_extra::register_all_extra_functions;
use std::path::Path;
use tempfile::TempDir;

pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        register_all_extra_functions(&ctx);
        Self { ctx }
    }

    /// Create a SessionContext, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub async fn try_new_for_test_file(relative_path: &Path) -> Option<Self> {
        let config = SessionConfig::new();

        let test_ctx = TestContext::new(SessionContext::new_with_config(config));

        Some(test_ctx)
    }

    /// Returns a reference to the internal SessionContext
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}
