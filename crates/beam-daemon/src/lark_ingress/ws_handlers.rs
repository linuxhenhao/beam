use super::*;

pub(crate) struct LarkWsEventHandler {
    pub(crate) state: AppState,
    pub(crate) app_id: String,
    pub(crate) event_type: &'static str,
}

impl EventHandler for LarkWsEventHandler {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn handle(
        &self,
        event: Event,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EventHandlerResult> + Send + '_>> {
        let state = self.state.clone();
        let app_id = self.app_id.clone();
        Box::pin(async move {
            let payload = serde_json::to_value(event)
                .map_err(|err| feishu_core::Error::SerializationError(err.to_string()))?;
            match handle_lark_event_payload(state, app_id, payload, None).await {
                Ok(_) => Ok(None),
                Err((_status, err)) => Err(feishu_core::Error::InvalidEventFormat(err)),
            }
        })
    }
}

pub(crate) struct LarkWsCardActionEventHandler {
    pub(crate) state: AppState,
    pub(crate) app_id: String,
    pub(crate) event_type: &'static str,
}

impl EventHandler for LarkWsCardActionEventHandler {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn handle(
        &self,
        event: Event,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EventHandlerResult> + Send + '_>> {
        let state = self.state.clone();
        let app_id = self.app_id.clone();
        Box::pin(async move {
            let raw = event.event.unwrap_or_default();
            let payload = normalize_lark_ws_card_action_from_raw(raw)?;

            let Json(response) = handle_lark_card_action_payload(&state, &app_id, payload)
                .await
                .map_err(|(_status, err)| feishu_core::Error::InvalidEventFormat(err))?;
            let body = serde_json::to_vec(&response)
                .map_err(|err| feishu_core::Error::SerializationError(err.to_string()))?;
            Ok(Some(EventResp::ok(body)))
        })
    }
}

pub(crate) fn spawn_lark_ws_clients(state: &AppState) {
    for bot in state.bots.values() {
        let config = feishu_core::Config::builder(&bot.lark_app_id, &bot.lark_app_secret)
            .request_timeout(Duration::from_secs(15))
            .build();
        let mut dispatcher_config = EventDispatcherConfig::new().skip_signature_verification(true);
        if let Some(token) = &bot.lark_verification_token {
            dispatcher_config = dispatcher_config.verification_token(token.clone());
        }
        if let Some(key) = &bot.lark_encrypt_key {
            dispatcher_config = dispatcher_config.encrypt_key(key.clone());
        }
        let dispatcher = EventDispatcher::new(dispatcher_config, config.logger.clone());
        let handler = LarkWsEventHandler {
            state: state.clone(),
            app_id: bot.lark_app_id.clone(),
            event_type: "im.message.receive_v1",
        };
        let card_handler = LarkWsCardActionEventHandler {
            state: state.clone(),
            app_id: bot.lark_app_id.clone(),
            event_type: "card.action.trigger",
        };
        let app_id = bot.lark_app_id.clone();
        tokio::spawn(async move {
            dispatcher.register_handler(Box::new(handler)).await;
            dispatcher.register_handler(Box::new(card_handler)).await;
            match StreamClient::builder(config)
                .stream_config(StreamConfig::default())
                .event_dispatcher(dispatcher)
                .build()
            {
                Ok(client) => {
                    eprintln!("lark ws starting for {}", app_id);
                    if let Err(err) = client.start().await {
                        eprintln!("lark ws stopped for {}: {}", app_id, err);
                    }
                }
                Err(err) => eprintln!("lark ws init failed for {}: {}", app_id, err),
            }
        });
    }
}

pub(crate) fn normalize_lark_ws_card_action(action: CardAction) -> Value {
    let mut payload = serde_json::to_value(&action).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(object) = payload.as_object_mut() {
        if let Some(open_id) = action.open_id.filter(|value| !value.trim().is_empty()) {
            object.insert(
                "operator".to_string(),
                serde_json::json!({ "open_id": open_id }),
            );
        }
        if let Some(message_id) = action
            .open_message_id
            .filter(|value| !value.trim().is_empty())
        {
            object.insert(
                "context".to_string(),
                serde_json::json!({ "open_message_id": message_id }),
            );
        }
    }
    payload
}

pub(crate) fn normalize_lark_ws_card_action_from_raw(
    raw: Value,
) -> Result<Value, feishu_core::Error> {
    let form_value_snapshot = raw.pointer("/action/form_value").cloned();
    let operator_snapshot = raw.pointer("/operator").cloned();
    let operator_id_snapshot = raw.pointer("/operator_id").cloned();
    let context_snapshot = raw.pointer("/context").cloned();

    let card_action: CardAction = serde_json::from_value(raw)
        .map_err(|err| feishu_core::Error::InvalidEventFormat(err.to_string()))?;
    let mut payload = normalize_lark_ws_card_action(card_action);

    if let Some(fv) = form_value_snapshot
        && let Some(action) = payload.pointer_mut("/action")
        && let Some(obj) = action.as_object_mut()
    {
        obj.insert("form_value".to_string(), fv);
    }

    if let Some(op) = operator_snapshot
        && let Some(obj) = payload.as_object_mut()
    {
        obj.insert("operator".to_string(), op);
    }

    if let Some(op_id) = operator_id_snapshot
        && let Some(obj) = payload.as_object_mut()
        && !obj.contains_key("operator")
    {
        obj.insert("operator_id".to_string(), op_id);
    }

    if let Some(ctx) = context_snapshot
        && let Some(obj) = payload.as_object_mut()
    {
        obj.insert("context".to_string(), ctx);
    }

    Ok(payload)
}
