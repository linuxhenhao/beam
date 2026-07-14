mod bootstrap;
mod validation;

pub use bootstrap::{
    BootstrapWorkflowRunInput, RunChatBinding, WorkflowOutputRef, WorkflowRunBootstrap,
    bootstrap_workflow_run, mint_workflow_run_id, read_workflow_definition_from_path,
};
pub use validation::normalize_workflow_params;

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod bootstrap_tests;
#[cfg(test)]
#[path = "coercion_tests.rs"]
mod coercion_tests;
#[cfg(test)]
#[path = "format_tests.rs"]
mod format_tests;
#[cfg(test)]
#[path = "params_tests.rs"]
mod params_tests;
