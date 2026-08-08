use super::*;

pub(crate) async fn list_webhook_triggers(State(state): State<AppState>) -> Json<Value> {
    let records = read_webhook_trigger_records(&state.paths)
        .await
        .unwrap_or_default();
    Json(serde_json::json!({ "records": records }))
}

pub(crate) async fn api_trigger(
    State(state): State<AppState>,
    Json(raw): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let request: ApiTriggerRequest = serde_json::from_value(raw)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid JSON body".to_string()))?;
    if request.envelope.trusted {
        return Err((
            StatusCode::BAD_REQUEST,
            "envelope.trusted must be false".to_string(),
        ));
    }
    if request.target.kind != "turn" && request.target.kind != "workflow" {
        return Err((
            StatusCode::BAD_REQUEST,
            "target.kind must be turn or workflow".to_string(),
        ));
    }
    let Some(bot_id) = request.target.bot_id.clone() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "target.botId is required".to_string(),
        ));
    };
    let Some(bot) = state.bots.get(&bot_id).cloned() else {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "unknown bot".to_string()));
    };
    let trigger_id = new_trigger_log_id();
    let prompt = build_untrusted_event_prompt(&request, &trigger_id);
    let prompt_preview = if prompt.len() > 4000 {
        format!("{}\n...[truncated]", &prompt[..4000])
    } else {
        prompt.clone()
    };
    if request.options.dry_run.unwrap_or(false) {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "triggerId": trigger_id,
                "action": "dry_run",
                "target": {
                    "kind": request.target.kind,
                    "chatId": request.target.chat_id,
                    "sessionId": request.target.session_id,
                    "workflowId": request.target.workflow_id,
                },
                "message": "dry run",
                "promptPreview": prompt_preview,
            })),
        ));
    }

    if request.target.kind == "workflow" {
        let Some(workflow_id) = request.target.workflow_id.clone() else {
            return Err((
                StatusCode::BAD_REQUEST,
                "workflow target requires workflowId".to_string(),
            ));
        };
        let Some(chat_id) = request.target.chat_id.clone() else {
            return Err((
                StatusCode::BAD_REQUEST,
                "workflow target requires chatId".to_string(),
            ));
        };
        let event_json = serde_json::to_string(&serde_json::json!({
            "triggerId": trigger_id,
            "source": {
                "type": request.source.source_type,
                "connectorId": request.source.connector_id,
                "requestId": request.source.request_id,
                "receivedAt": request.source.received_at,
            },
            "envelope": {
                "format": request.envelope.format,
                "sourceName": request.envelope.source_name,
                "trusted": request.envelope.trusted,
                "headers": request.envelope.headers,
                "payload": request.envelope.payload,
                "rawText": request.envelope.raw_text,
            },
            "options": {
                "dryRun": request.options.dry_run,
                "dedupKey": request.options.dedup_key,
                "status": request.options.status,
            },
        }))
        .unwrap_or_else(|_| "{}".to_string());
        let def_path = load_workflow_definition_path(&workflow_id)
            .await
            .map_err(internal_error)?;
        let raw_def = tokio::fs::read_to_string(&def_path)
            .await
            .map_err(internal_error)?;
        let bootstrap = bootstrap_and_start_workflow_run(
            &state,
            &workflow_id,
            &raw_def,
            &BTreeMap::from([(String::from("event"), Value::String(event_json))]),
            "external",
            Some(RunChatBinding {
                chat_id: chat_id.clone(),
                lark_app_id: bot_id.clone(),
            }),
        )
        .await
        .map_err(internal_error)?;
        return Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({
                "ok": true,
                "triggerId": trigger_id,
                "action": "queued",
                "target": {
                    "kind": "workflow",
                    "workflowRunId": bootstrap.run_id,
                    "chatId": chat_id,
                },
                "message": format!("workflow \"{}\" run {} started", workflow_id, bootstrap.run_id),
            })),
        ));
    }

    let existing_session =
        {
            let sessions = state.sessions.lock().await;
            request
                .target
                .session_id
                .as_deref()
                .and_then(|session_id| {
                    sessions
                        .get(session_id)
                        .cloned()
                        .filter(|session| session.status == SessionStatus::Active)
                })
                .or_else(|| {
                    request.target.chat_id.as_deref().and_then(|chat_id| {
                        find_active_session_by_chat(&sessions, &bot_id, chat_id)
                    })
                })
        };
    if let Some(session) = existing_session {
        let chat_id = session.chat_id.clone();
        let _ = send_input(
            State(state.clone()),
            AxumPath(session.session_id.clone()),
            Json(SessionInputRequest {
                content: prompt.clone(),
                raw: false,
            }),
        )
        .await?;
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "triggerId": trigger_id,
                "action": "delivered",
                "target": {
                    "kind": "turn",
                    "sessionId": session.session_id,
                    "chatId": chat_id,
                },
                "message": "delivered to existing session",
                "promptPreview": prompt_preview,
            })),
        ));
    }

    let Some(chat_id) = request.target.chat_id.clone() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "turn target requires chatId or an active sessionId".to_string(),
        ));
    };

    let working_dir = expand_tilde(&bot.working_dir.clone().unwrap_or_else(|| {
        state
            .config
            .daemon
            .working_dirs
            .first()
            .cloned()
            .unwrap_or_else(|| ".".to_string())
    }));
    let summary = create_session_internal(
        &state,
        build_session_create_spec_from_bot(
            &bot,
            api_trigger_title(&request),
            chat_id.clone(),
            Some("group".to_string()),
            chat_id.clone(),
            None,
            SessionScope::Chat,
            None,
            working_dir.clone(),
            prompt,
            bot_id.clone(),
            None,
            None,
            None,
        ),
    )
    .await
    .map_err(internal_error)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ok": true,
            "triggerId": trigger_id,
            "action": "queued",
            "target": {
                "kind": "turn",
                "sessionId": summary.session_id,
                "chatId": chat_id,
            },
            "message": "queued new session turn",
            "promptPreview": prompt_preview,
        })),
    ))
}

pub(crate) async fn handle_webhook_trigger(
    State(state): State<AppState>,
    AxumPath(connector_id): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let connector = get_connector(&state.paths, &connector_id)
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown connector".to_string()))?;
    if !connector.enabled {
        return Err((
            StatusCode::NOT_FOUND,
            "unknown or disabled connector".to_string(),
        ));
    }
    if !rate_allowed(&connector) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "connector rate limit exceeded".to_string(),
        ));
    }
    if body.len() as u64 > connector.prompt_envelope.max_body_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large".to_string(),
        ));
    }

    let request_body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let signature = headers
        .get(connector.verify.signature_header.as_str())
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let timestamp = headers
        .get(connector.verify.timestamp_header.as_str())
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let nonce = headers
        .get(connector.verify.nonce_header.as_str())
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    if signature.is_empty() || timestamp.is_empty() || nonce.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            "missing signature, timestamp, or nonce header".to_string(),
        ));
    }
    if !timestamp_ok(&timestamp, connector.verify.tolerance_seconds) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "timestamp outside tolerance window".to_string(),
        ));
    }
    if !claim_nonce(&connector.id, &nonce, connector.verify.tolerance_seconds) {
        return Err((StatusCode::CONFLICT, "nonce replay detected".to_string()));
    }
    let Some(secret) =
        get_webhook_secret(&state.paths, &connector.verify.secret_ref).map_err(internal_error)?
    else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "signature verification failed".to_string(),
        ));
    };
    if !verify_webhook_signature(&secret, &timestamp, &body, &signature) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "signature verification failed".to_string(),
        ));
    }

    let trigger_id = new_trigger_log_id();
    let source_name = if connector.prompt_envelope.source_name.trim().is_empty() {
        connector.name.clone()
    } else {
        connector.prompt_envelope.source_name.clone()
    };
    if let Some(extractors) = connector.lifecycle_extractors.as_ref() {
        let (dedup_key, extracted_status) = extract_webhook_lifecycle(&request_body, extractors)
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        let begun = begin_webhook_lifecycle_firing(&state.paths, &connector.id, &dedup_key)
            .map_err(internal_error)?;
        let _ = begun;
        if extracted_status == "resolved" {
            let _ = resolve_webhook_lifecycle_group(&state.paths, &connector.id, &dedup_key)
                .map_err(internal_error)?;
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({
                    "ok": true,
                    "triggerId": trigger_id,
                    "action": "ignored",
                    "lifecycle": { "dedupKey": dedup_key, "status": extracted_status, "action": "resolved" },
                })),
            ));
        }
        let Some(chat_id) = dynamic_chat_id(&query, &headers, &request_body)
            .or_else(|| connector.target.chat_id.clone())
        else {
            return Ok((
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "ok": true,
                    "triggerId": trigger_id,
                    "action": "ignored",
                    "lifecycle": { "dedupKey": dedup_key, "status": extracted_status, "action": "creating" },
                })),
            ));
        };
        let trigger = ApiTriggerRequest {
            source: ApiTriggerSource {
                source_type: "webhook".to_string(),
                connector_id: Some(connector.id.clone()),
                request_id: Some(nonce.clone()),
                received_at: Some(now_iso()),
            },
            target: ApiTriggerTarget {
                kind: connector.target.kind.clone(),
                bot_id: Some(connector.target.bot_id.clone()),
                chat_id: Some(chat_id.clone()),
                session_id: None,
                workflow_id: connector.target.workflow_id.clone(),
            },
            envelope: ApiTriggerEnvelope {
                format: "beam.webhook.v1".to_string(),
                source_name: source_name.clone(),
                trusted: false,
                headers: Some(pick_allowed_headers(
                    &headers,
                    &connector.prompt_envelope.header_allowlist,
                )),
                payload: Some(request_body.clone()),
                raw_text: connector
                    .prompt_envelope
                    .include_raw_text
                    .then(|| String::from_utf8_lossy(&body).to_string()),
            },
            options: ApiTriggerOptions {
                dry_run: Some(false),
                dedup_key: Some(dedup_key.clone()),
                status: Some(extracted_status.clone()),
            },
        };
        return api_trigger(
            State(state.clone()),
            Json(serde_json::to_value(trigger).unwrap_or(Value::Null)),
        )
        .await;
    }

    let chat_id = connector
        .target
        .chat_id
        .clone()
        .or_else(|| dynamic_chat_id(&query, &headers, &request_body))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "target chatId is required".to_string(),
            )
        })?;

    if let Some(allowed) =
        (!connector.target.allow_chats.is_empty()).then_some(&connector.target.allow_chats)
        && !allowed.iter().any(|value| value == &chat_id)
    {
        return Err((
            StatusCode::FORBIDDEN,
            "chatId is not allowed for this connector".to_string(),
        ));
    }

    let trigger = ApiTriggerRequest {
        source: ApiTriggerSource {
            source_type: "webhook".to_string(),
            connector_id: Some(connector.id.clone()),
            request_id: Some(nonce.clone()),
            received_at: Some(now_iso()),
        },
        target: ApiTriggerTarget {
            kind: connector.target.kind.clone(),
            bot_id: Some(connector.target.bot_id.clone()),
            chat_id: Some(chat_id.clone()),
            session_id: None,
            workflow_id: connector.target.workflow_id.clone(),
        },
        envelope: ApiTriggerEnvelope {
            format: "beam.webhook.v1".to_string(),
            source_name,
            trusted: false,
            headers: Some(pick_allowed_headers(
                &headers,
                &connector.prompt_envelope.header_allowlist,
            )),
            payload: Some(request_body),
            raw_text: connector
                .prompt_envelope
                .include_raw_text
                .then(|| String::from_utf8_lossy(&body).to_string()),
        },
        options: ApiTriggerOptions {
            dry_run: Some(false),
            dedup_key: None,
            status: None,
        },
    };
    api_trigger(
        State(state),
        Json(serde_json::to_value(trigger).unwrap_or(Value::Null)),
    )
    .await
}

pub(crate) async fn connectors(State(state): State<AppState>) -> Json<Value> {
    Json(serde_json::json!({
        "connectors": list_connectors(&state.paths).unwrap_or_default(),
    }))
}

pub(crate) async fn connector_stats(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let since = query.get("since").map(String::as_str);
    let raw_stats = summarize_trigger_logs(&state.paths, None, since).unwrap_or_default();
    let by_id: HashMap<String, TriggerLogStats> = raw_stats
        .iter()
        .filter_map(|stat| stat.connector_id.clone().map(|id| (id, stat.clone())))
        .collect();
    let connectors = list_connectors(&state.paths).unwrap_or_default();
    let known: std::collections::HashSet<String> = connectors
        .iter()
        .map(|connector| connector.id.clone())
        .collect();
    let mut stats: Vec<Value> = connectors
        .iter()
        .map(|connector| {
            let stat = by_id
                .get(&connector.id)
                .cloned()
                .unwrap_or_else(|| TriggerLogStats {
                    connector_id: Some(connector.id.clone()),
                    ..Default::default()
                });
            serde_json::json!({
                "name": connector.name,
                "enabled": connector.enabled,
                "connectorId": connector.id,
                "total": stat.total,
                "ok": stat.ok,
                "error": stat.error,
                "actions": stat.actions,
                "errorCodes": stat.error_codes,
                "lastTriggeredAt": stat.last_triggered_at,
                "lastOkAt": stat.last_ok_at,
                "lastErrorAt": stat.last_error_at,
                "lastError": stat.last_error,
                "lastErrorCode": stat.last_error_code,
            })
        })
        .collect();
    for stat in raw_stats {
        if let Some(connector_id) = stat.connector_id.clone()
            && !known.contains(&connector_id)
        {
            stats.push(serde_json::to_value(stat).unwrap_or(Value::Null));
        }
    }
    Json(serde_json::json!({ "stats": stats }))
}

pub(crate) async fn create_connector(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let provided_secret = body
        .get("secret")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string());
    let generated_secret = provided_secret
        .is_none()
        .then(generate_webhook_secret_plaintext);
    let secret_record = create_webhook_secret(
        &state.paths,
        provided_secret
            .as_deref()
            .or(generated_secret.as_deref())
            .unwrap(),
    )
    .map_err(internal_error)?;
    match normalize_connector_input(&body, None, None, Some(&secret_record.ref_name)) {
        Ok(connector) => {
            let connector = upsert_connector(&state.paths, connector).map_err(internal_error)?;
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "ok": true,
                    "connector": connector,
                    "secretRef": secret_record.ref_name,
                    "secret": generated_secret,
                    "webhookUrl": format!("/webhook/{}", connector.id),
                })),
            ))
        }
        Err(error) => {
            let _ = delete_webhook_secret(&state.paths, &secret_record.ref_name);
            Err((StatusCode::BAD_REQUEST, error))
        }
    }
}

pub(crate) async fn get_connector_api(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let connector = get_connector(&state.paths, &id)
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown_connector".to_string()))?;
    Ok(Json(serde_json::json!({ "connector": connector })))
}

pub(crate) async fn update_connector_api(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let prior = get_connector(&state.paths, &id)
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown_connector".to_string()))?;
    let mut secret_ref = prior.verify.secret_ref.clone();
    let mut generated_secret: Option<String> = None;
    if let Some(secret) = body
        .get("secret")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
    {
        secret_ref = set_webhook_secret(&state.paths, &secret_ref, secret)
            .map_err(internal_error)?
            .ref_name;
        generated_secret = Some(secret.to_string());
    } else if body
        .get("rotateSecret")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let secret = generate_webhook_secret_plaintext();
        secret_ref = set_webhook_secret(&state.paths, &secret_ref, &secret)
            .map_err(internal_error)?
            .ref_name;
        generated_secret = Some(secret);
    }
    let connector = normalize_connector_input(&body, Some(&id), Some(&prior), Some(&secret_ref))
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let connector = upsert_connector(&state.paths, connector).map_err(internal_error)?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "connector": connector,
            "secretRef": secret_ref,
            "secret": generated_secret,
        })),
    ))
}

pub(crate) async fn patch_connector_api(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let prior = get_connector(&state.paths, &id)
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown_connector".to_string()))?;
    let enabled = body
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(prior.enabled);
    let connector = upsert_connector(
        &state.paths,
        ConnectorDefinition {
            enabled,
            updated_at: now_iso(),
            ..prior
        },
    )
    .map_err(internal_error)?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "connector": connector })),
    ))
}

pub(crate) async fn delete_connector_api(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    Ok(Json(serde_json::json!({
        "ok": true,
        "deleted": delete_connector(&state.paths, &id).map_err(internal_error)?,
    })))
}

pub(crate) async fn list_webhook_secrets_api(State(state): State<AppState>) -> Json<Value> {
    Json(
        serde_json::json!({ "secrets": list_webhook_secret_refs(&state.paths).unwrap_or_default() }),
    )
}

pub(crate) async fn create_webhook_secret_api(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let secret = body
        .get("secret")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(generate_webhook_secret_plaintext);
    let record = create_webhook_secret(&state.paths, &secret).map_err(internal_error)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "ok": true, "secretRef": record.ref_name, "secret": secret })),
    ))
}

pub(crate) async fn update_webhook_secret_api(
    State(state): State<AppState>,
    AxumPath(ref_id): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let secret = body
        .get("secret")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(generate_webhook_secret_plaintext);
    let record = set_webhook_secret(&state.paths, &ref_id, &secret).map_err(internal_error)?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "secretRef": record.ref_name, "secret": secret })),
    ))
}

pub(crate) async fn delete_webhook_secret_api(
    State(state): State<AppState>,
    AxumPath(ref_id): AxumPath<String>,
) -> Json<Value> {
    Json(
        serde_json::json!({ "ok": true, "deleted": delete_webhook_secret(&state.paths, &ref_id).unwrap_or(false) }),
    )
}

pub(crate) async fn trigger_logs_api(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    let connector_id = query.get("connectorId").map(String::as_str);
    let status = query.get("status").map(String::as_str);
    let error_code = query.get("errorCode").map(String::as_str);
    let since = query.get("since").map(String::as_str);
    Json(serde_json::json!({
        "logs": list_trigger_logs(&state.paths, limit, connector_id, status, error_code, since).unwrap_or_default(),
    }))
}

pub(crate) async fn prune_trigger_logs_api(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let retention_days = body.get("retentionDays").and_then(Value::as_u64);
    let max_entries = body
        .get("maxEntries")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    let result =
        prune_trigger_logs(&state.paths, retention_days, max_entries).map_err(internal_error)?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "before": result.before,
        "after": result.after,
        "deleted": result.deleted,
    })))
}
