use super::*;

pub(crate) async fn list_workflow_definitions_api(
    State(_state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let definitions = list_workflow_definitions().await.map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "definitions": definitions })))
}

pub(crate) async fn get_workflow_definition_api(
    State(_state): State<AppState>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    if workflow_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_id".to_string()));
    }
    match load_workflow_catalog_definition(&workflow_id)
        .await
        .map_err(internal_error)?
    {
        Some(found) => Ok(Json(serde_json::to_value(found).map_err(internal_error)?)),
        None => Err((StatusCode::NOT_FOUND, "unknown_workflow".to_string())),
    }
}

pub(crate) async fn trigger_workflow_definition_run_api(
    State(state): State<AppState>,
    AxumPath(workflow_id): AxumPath<String>,
    Json(req): Json<WorkflowRunTriggerBody>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    if workflow_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_id".to_string()));
    }
    let chat_binding = req
        .chat_binding
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing_chat_binding".to_string()))?;
    let def_path = load_workflow_definition_path(&workflow_id)
        .await
        .map_err(internal_error)?;
    let raw_def = tokio::fs::read_to_string(&def_path)
        .await
        .map_err(internal_error)?;
    let params = req.params;
    let bootstrap = bootstrap_and_start_workflow_run(
        &state,
        &workflow_id,
        &raw_def,
        &params,
        "dashboard",
        Some(chat_binding),
    )
    .await
    .map_err(internal_error)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ok": true,
            "runId": bootstrap.run_id,
            "workflowId": bootstrap.workflow_id,
            "revisionId": bootstrap.revision_id,
            "status": "running",
            "lastSeq": 2,
        })),
    ))
}

pub(crate) async fn list_workflow_runs_api(
    State(state): State<AppState>,
    Query(query): Query<WorkflowRunsQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let all = query.all.as_deref() == Some("1");
    let statuses = query.status.as_ref().map(|value| {
        value
            .split(',')
            .map(|part| part.trim().to_ascii_lowercase())
            .filter(|part| !part.is_empty())
            .collect::<HashSet<_>>()
    });
    let runs = list_workflow_runs(&state.paths, all, statuses)
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({ "runs": runs })))
}

pub(crate) async fn get_workflow_run_snapshot_api(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(snapshot) = read_run_snapshot(&state.paths.workflow_run_dir(&run_id))
        .await
        .map_err(internal_error)?
    else {
        return Err((StatusCode::NOT_FOUND, "workflow run not found".to_string()));
    };
    Ok(Json(
        serde_json::to_value(snapshot).map_err(internal_error)?,
    ))
}

pub(crate) async fn get_workflow_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(snapshot) = read_run_snapshot(&state.paths.workflow_run_dir(&run_id))
        .await
        .map_err(internal_error)?
    else {
        return Err((StatusCode::NOT_FOUND, "workflow run not found".to_string()));
    };
    Ok(Json(
        serde_json::to_value(snapshot).map_err(internal_error)?,
    ))
}

pub(crate) async fn get_workflow_run_events(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Query(query): Query<WorkflowWindowQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(events) =
        read_run_events_pure(&state.paths.workflow_run_dir(&run_id)).map_err(internal_error)?
    else {
        return Err((StatusCode::NOT_FOUND, "workflow run not found".to_string()));
    };
    let window = read_event_window(
        &events,
        EventWindowOpts {
            tail: query.tail,
            before_seq: query.before_seq,
            after_seq: query.after_seq,
            limit: query.limit,
        },
    );
    Ok(Json(serde_json::json!({
        "runId": run_id,
        "events": window.events,
        "oldestSeq": window.oldest_seq,
        "newestSeq": window.newest_seq,
        "totalCount": window.total_count,
        "hasOlder": window.has_older,
        "hasNewer": window.has_newer,
    })))
}

pub(crate) async fn trigger_workflow_run(
    State(state): State<AppState>,
    AxumPath(workflow_id): AxumPath<String>,
    Json(req): Json<WorkflowRunRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let def_path = load_workflow_definition_path(&workflow_id)
        .await
        .map_err(internal_error)?;
    let raw_def = tokio::fs::read_to_string(&def_path)
        .await
        .map_err(internal_error)?;
    let params: BTreeMap<String, Value> = req
        .raw_params
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect();
    let bootstrap = bootstrap_and_start_workflow_run(
        &state,
        &workflow_id,
        &raw_def,
        &params,
        req.initiator.as_deref().unwrap_or("dashboard"),
        req.chat_binding.clone(),
    )
    .await
    .map_err(internal_error)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ok": true,
            "runId": bootstrap.run_id,
            "workflowId": bootstrap.workflow_id,
            "revisionId": bootstrap.revision_id,
            "status": "running",
            "lastSeq": 2,
        })),
    ))
}
