pub mod test_context;
pub mod utils;

use crate::utils::TestExecution;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use futures::stream::StreamExt;
use log::info;
// use sqllogictest::strict_column_validator;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use test_context::TestContext;

const TEST_DIRECTORY: &str = "./tests/sqllogictest/";

/// Represents a parsed test file
#[derive(Debug)]
struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    fn new(path: PathBuf) -> Self {
        let relative_path = PathBuf::from(path.to_string_lossy().strip_prefix(TEST_DIRECTORY).unwrap_or(""));

        Self { path, relative_path }
    }

    fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }
}

#[tokio::test]
async fn sqllogictest() {
    // let test_files = read_test_files().unwrap();
    let errors = futures::stream::iter(read_test_files().unwrap())
        .map(|test_file| {
            SpawnedTask::spawn(async move {
                run_test_file(test_file).await?;

                Ok(()) as Result<()>
            })
            .join()
        })
        .collect()
        .await;
}

fn read_test_files() -> Result<Box<dyn Iterator<Item = TestFile>>> {
    Ok(Box::new(
        read_dir_recursive(TEST_DIRECTORY)?
            .into_iter()
            .map(TestFile::new)
            .filter(|f| f.is_slt_file()),
    ))
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref())?;
    Ok(dst)
}

/// Append all paths recursively to dst
fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) -> Result<()> {
    let entries = std::fs::read_dir(path).map_err(|e| exec_datafusion_err!("Error reading directory {path:?}: {e}"))?;
    for entry in entries {
        let path = entry
            .map_err(|e| exec_datafusion_err!("Error reading entry in directory {path:?}: {e}"))?
            .path();

        if path.is_dir() {
            read_dir_recursive_impl(dst, &path)?;
        } else {
            dst.push(path);
        }
    }

    Ok(())
}

async fn run_test_file(test_file: TestFile) -> Result<()> {
    let TestFile { path, relative_path } = test_file;
    info!("Running with DataFusion runner: {}", path.display());
    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(TestExecution::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
        ))
    });
    // runner.with_column_validator(strict_column_validator);
    runner
        .run_file_async(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
