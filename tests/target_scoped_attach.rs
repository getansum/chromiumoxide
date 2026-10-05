//! Loopback CDP контракт: без Chrome, реальных профилей и sleep.
use async_tungstenite::tungstenite::Message;
use chromiumoxide::Browser;
use chromiumoxide::cdp::browser_protocol::{browser, emulation, page, target};
use chromiumoxide::handler::{ConnectionPolicy, HandlerConfig, viewport::Viewport};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Success,
    Missing,
    Unsupported,
    AttachError,
    AttachPending,
    InitError,
    Eof,
    Destroyed,
    Detached,
}

fn info(id: &str, ty: &str) -> Value {
    json!({"targetId": id, "type": ty, "title": id, "url": "about:blank", "attached": false,
        "canAccessOpener": false})
}

fn event(method: &str, params: Value, session: Option<&str>) -> Value {
    let mut value = json!({"method": method, "params": params});
    if let Some(id) = session {
        value["sessionId"] = json!(id);
    }
    value
}

async fn send(
    ws: &mut async_tungstenite::WebSocketStream<
        async_tungstenite::tokio::TokioAdapter<tokio::net::TcpStream>,
    >,
    value: Value,
) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

struct Fixture {
    browser: Browser,
    handler: JoinHandle<Vec<String>>,
    server: JoinHandle<Vec<Value>>,
    events: mpsc::UnboundedSender<Value>,
    observed: mpsc::UnboundedReceiver<Value>,
}

impl Fixture {
    async fn new(scenario: Scenario, reverse: bool, legacy: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (events, mut rx) = mpsc::unbounded_channel();
        let (observed_tx, observed) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = async_tungstenite::tokio::accept_async(stream)
                .await
                .unwrap();
            let mut record = Vec::new();
            let mut targets = vec![
                info("other", "page"),
                info(
                    "selected",
                    if matches!(scenario, Scenario::Unsupported) {
                        "service_worker"
                    } else {
                        "page"
                    },
                ),
            ];
            if matches!(scenario, Scenario::Missing) {
                targets.pop();
            }
            if reverse {
                targets.reverse();
            }
            loop {
                tokio::select! {
                    biased;
                    Some(value) = rx.recv() => send(&mut ws, value).await,
                    msg = ws.next() => {
                        let Some(msg) = msg else { break };
                        let msg = msg.unwrap();
                        if matches!(msg, Message::Close(_)) { ws.flush().await.unwrap(); break; }
                        let Message::Text(text) = msg else { continue };
                        let call: Value = serde_json::from_str(&text).unwrap();
                        record.push(call.clone());
                        observed_tx.send(call.clone()).unwrap();
                        let method = call["method"].as_str().unwrap();
                        let mut result = json!({});
                        let mut error = false;
                        match method {
                            "Target.setDiscoverTargets" => {
                                for info in &targets {
                                    send(&mut ws, event("Target.targetCreated", json!({"targetInfo": info}), None)).await;
                                }
                            }
                            "Target.getTargets" => result = json!({"targetInfos": targets}),
                            "Target.attachToTarget" => {
                                let id = call["params"]["targetId"].as_str().unwrap();
                                let session = format!("session-{id}");
                                result = json!({"sessionId": session});
                                error = matches!(scenario, Scenario::AttachError);
                                // Настоящий CDP обычно присылает и event, и response.
                                send(&mut ws, event("Target.attachedToTarget", json!({"sessionId": session,
                                    "targetInfo": info(id, "page"), "waitingForDebugger": false}), None)).await;
                                if matches!(scenario, Scenario::AttachPending) { continue; }
                            }
                            "Page.getFrameTree" => {
                                result = json!({"frameTree": {"frame": {"id":"frame", "loaderId":"loader",
                                    "url":"about:blank", "domainAndRegistry":"", "securityOrigin":"://", "mimeType":"text/html",
                                    "secureContextType":"Secure", "crossOriginIsolatedContextType":"NotIsolated", "gatedAPIFeatures":[]}}});
                                error = matches!(scenario, Scenario::InitError);
                                if matches!(scenario, Scenario::Eof) { ws.close(None).await.unwrap(); break; }
                                if matches!(scenario, Scenario::Destroyed | Scenario::Detached) {
                                    let value = if matches!(scenario, Scenario::Destroyed) {
                                        event("Target.targetDestroyed", json!({"targetId":"selected"}), None)
                                    } else {
                                        event("Target.detachedFromTarget", json!({"sessionId":"session-selected", "targetId":"selected"}), None)
                                    };
                                    send(&mut ws, value).await;
                                    // Намеренно не отвечаем: pending init обязан завершиться.
                                    continue;
                                }
                            }
                            "Page.addScriptToEvaluateOnNewDocument" => result = json!({"identifier":"script"}),
                            "Page.createIsolatedWorld" => result = json!({"executionContextId":1}),
                            "Page.navigate" => result = json!({"frameId":"frame"}),
                            _ => {}
                        }
                        let mut response = json!({"id":call["id"], "result":result});
                        if error {
                            response.as_object_mut().unwrap().remove("result");
                            response["error"] = json!({"code":-32000,"message":"synthetic rejection"});
                        }
                        if let Some(session) = call.get("sessionId") { response["sessionId"] = session.clone(); }
                        send(&mut ws, response).await;
                        if method == "Browser.close" { ws.close(None).await.unwrap(); break; }
                    }
                }
            }
            record
        });
        let config = HandlerConfig {
            connection_policy: if legacy {
                ConnectionPolicy::Legacy
            } else {
                ConnectionPolicy::TargetScoped("selected".to_owned().into())
            },
            viewport: Some(Viewport::default()),
            ..HandlerConfig::default()
        };
        let (browser, mut handler) = Browser::connect_with_config(url, config).await.unwrap();
        let handler = tokio::spawn(async move {
            let mut errors = Vec::new();
            while let Some(result) = handler.next().await {
                if let Err(error) = result {
                    errors.push(error.to_string());
                }
            }
            // Подтверждаем fused completion, затем освобождаем транспорт.
            assert!(handler.next().await.is_none());
            errors
        });
        Self {
            browser,
            handler,
            server,
            events,
            observed,
        }
    }

    async fn finish(self) -> (Vec<Value>, Vec<String>) {
        drop(self.browser);
        let errors = self.handler.await.unwrap();
        let record = self.server.await.unwrap();
        (record, errors)
    }
}

fn assert_safe(record: &[Value]) {
    for call in record {
        let method = call["method"].as_str().unwrap();
        assert!(
            !matches!(
                method,
                "Browser.close"
                    | "Target.closeTarget"
                    | "Page.close"
                    | "Target.setAutoAttach"
                    | "Runtime.runIfWaitingForDebugger"
                    | "Page.navigate"
                    | "Browser.setWindowBounds"
                    | "Security.setIgnoreCertificateErrors"
            ),
            "{call}"
        );
        assert!(!method.starts_with("Emulation."), "{call}");
        if method == "Target.attachToTarget" {
            assert_eq!(call["params"]["targetId"], "selected");
        }
        if let Some(session) = call.get("sessionId") {
            assert_eq!(session, "session-selected", "{call}");
        }
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    // Только страховка зависания, не способ доказать отсутствие команд.
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("synthetic suite зависла")
}

#[tokio::test]
async fn selected_target_routing_disconnect_and_drop() {
    bounded(async {
        for reverse in [false, true] {
            let mut f = Fixture::new(Scenario::Success, reverse, false).await;
            let infos = f.browser.fetch_targets().await.unwrap();
            assert_eq!(infos.len(), 1);
            assert_eq!(infos[0].target_id.as_ref(), "selected");
            let selected = f
                .browser
                .get_page("selected".to_owned().into())
                .await
                .unwrap();
            assert!(f.browser.get_page("other".to_owned().into()).await.is_err());
            assert!(
                f.browser
                    .close()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("policy")
            );
            assert!(f.browser.new_page("about:blank").await.is_err());
            assert!(
                f.browser
                    .create_browser_context(target::CreateBrowserContextParams::default())
                    .await
                    .is_err()
            );
            assert!(
                f.browser
                    .execute(browser::CloseParams::default())
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(browser::CloseParams::default())
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(target::CloseTargetParams::new("other".to_owned()))
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(target::CloseTargetParams::new("selected".to_owned()))
                    .await
                    .is_err()
            );
            assert!(
                f.browser
                    .execute(target::AttachToTargetParams::new("selected".to_owned()))
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(target::AttachToTargetParams::new("other".to_owned()))
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(target::SetAutoAttachParams::new(true, true))
                    .await
                    .is_err()
            );
            assert!(
                selected
                    .execute(page::CloseParams::default())
                    .await
                    .is_err()
            );
            // Позднее повторное discovery и чужие child/session события.
            for id in ["other", "selected", "child"] {
                f.events
                    .send(event(
                        "Target.targetCreated",
                        json!({"targetInfo":info(id,"page")}),
                        None,
                    ))
                    .unwrap();
            }
            for parent in [None, Some("session-selected"), Some("foreign-session")] {
                f.events
                    .send(event(
                        "Target.attachedToTarget",
                        json!({"sessionId":"child-session",
                    "targetInfo":info("child","service_worker"), "waitingForDebugger":true}),
                        parent,
                    ))
                    .unwrap();
            }
            // Сервер обрабатывает events до ответа на следующий metadata barrier.
            f.browser.fetch_targets().await.unwrap();
            assert_eq!(f.browser.pages().await.unwrap().len(), 1);
            assert!(selected.clone().close().await.is_err());
            drop(selected);
            if reverse {
                f.browser.disconnect().await.unwrap();
            }
            let (record, errors) = f.finish().await;
            assert!(errors.is_empty(), "{errors:?}");
            assert_safe(&record);
            assert_eq!(
                record
                    .iter()
                    .filter(|c| c["method"] == "Target.attachToTarget")
                    .count(),
                1
            );
            assert!(record.iter().any(|c| c["method"] == "Page.getFrameTree"));
        }
    })
    .await;
}

#[tokio::test]
async fn errors_complete_callers_and_handler_without_destructive_sends() {
    bounded(async {
        for scenario in [
            Scenario::Missing,
            Scenario::Unsupported,
            Scenario::AttachError,
            Scenario::InitError,
            Scenario::Eof,
            Scenario::Destroyed,
            Scenario::Detached,
        ] {
            let mut f = Fixture::new(scenario, false, false).await;
            let error = f.browser.fetch_targets().await.unwrap_err().to_string();
            if matches!(scenario, Scenario::AttachError | Scenario::InitError) {
                assert!(
                    error.contains("synthetic rejection"),
                    "{scenario:?}: {error}"
                );
            }
            let (record, errors) = f.finish().await;
            assert!(!errors.is_empty(), "{scenario:?}");
            assert_safe(&record);
        }
    })
    .await;
}

#[tokio::test]
async fn cleanup_during_pending_attach_is_non_destructive() {
    bounded(async {
        for explicit in [false, true] {
            let mut f = Fixture::new(Scenario::AttachPending, false, false).await;
            loop {
                let call = f.observed.recv().await.unwrap();
                if call["method"] == "Target.attachToTarget" {
                    break;
                }
            }
            if explicit {
                f.browser.disconnect().await.unwrap();
            }
            let (record, errors) = f.finish().await;
            assert!(errors.is_empty(), "{errors:?}");
            assert_safe(&record);
            assert!(!record.iter().any(|call| call.get("sessionId").is_some()));
        }
    })
    .await;
}

#[tokio::test]
async fn explicit_selected_page_emulation_remains_available() {
    bounded(async {
        let mut f = Fixture::new(Scenario::Success, false, false).await;
        f.browser.fetch_targets().await.unwrap();
        let selected = f
            .browser
            .get_page("selected".to_owned().into())
            .await
            .unwrap();
        selected
            .execute(emulation::SetTouchEmulationEnabledParams::new(true))
            .await
            .unwrap();
        f.browser.disconnect().await.unwrap();
        drop(selected);
        let (record, errors) = f.finish().await;
        assert!(errors.is_empty());
        assert_eq!(
            record
                .iter()
                .filter(|c| c["method"] == "Emulation.setTouchEmulationEnabled")
                .count(),
            1
        );
        let init: Vec<_> = record
            .into_iter()
            .filter(|c| c["method"] != "Emulation.setTouchEmulationEnabled")
            .collect();
        assert_safe(&init);
    })
    .await;
}

#[tokio::test]
async fn legacy_discovery_auto_attach_and_close_are_preserved() {
    bounded(async {
        assert_eq!(
            HandlerConfig::default().connection_policy,
            ConnectionPolicy::Legacy
        );
        let mut f = Fixture::new(Scenario::Success, false, true).await;
        assert_eq!(f.browser.fetch_targets().await.unwrap().len(), 2);
        // В legacy fetch не барьер init: ждём реальные auto-attach sends.
        let mut sessions = std::collections::HashSet::new();
        while sessions.len() < 2 {
            let call = f.observed.recv().await.unwrap();
            if call["method"] == "Target.setAutoAttach" {
                assert_eq!(call["params"]["autoAttach"], true);
                assert_eq!(call["params"]["waitForDebuggerOnStart"], true);
                sessions.insert(call["sessionId"].clone().to_string());
            }
        }
        f.browser.close().await.unwrap();
        let (record, _) = f.finish().await;
        assert!(record.iter().any(|c| c["method"] == "Browser.close"));
        assert_eq!(sessions.len(), 2);
        for id in ["other", "selected"] {
            assert!(
                record.iter().any(
                    |c| c["method"] == "Target.attachToTarget" && c["params"]["targetId"] == id
                )
            );
        }
    })
    .await;
}
