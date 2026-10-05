use super::*;
use async_tungstenite::tungstenite::Message as WsMessage;
use futures::SinkExt;
use futures::channel::{mpsc, oneshot};
use serde_json::json;
use std::future::Future;

/// Реальный транспорт; только now эвикции управляемый. Timeout не ждём.
#[tokio::test]
async fn internal_timeout_finishes_fetch_and_transport() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (seen_tx, mut seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = async_tungstenite::tokio::accept_async(stream)
                .await
                .unwrap();
            let mut seen = Some(seen_tx);
            let mut record = Vec::new();
            while let Some(msg) = ws.next().await {
                match msg.unwrap() {
                    WsMessage::Text(text) => {
                        let call: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if call["method"] == "Target.attachToTarget" {
                            seen.take().unwrap().send(()).unwrap();
                        } else {
                            ws.send(WsMessage::Text(
                                json!({"id":call["id"],"result":{}}).to_string().into(),
                            ))
                            .await
                            .unwrap();
                        }
                        record.push(call);
                    }
                    WsMessage::Close(_) => {
                        ws.flush().await.unwrap();
                        break;
                    }
                    other => panic!("{other:?}"),
                }
            }
            record
        });
        let conn = Connection::connect(url).await.unwrap();
        let (_tx, rx) = mpsc::channel(1);
        let mut handler = Handler::new(
            conn,
            rx,
            HandlerConfig {
                connection_policy: ConnectionPolicy::TargetScoped("selected".to_owned().into()),
                ..HandlerConfig::default()
            },
        );
        let info: TargetInfo = serde_json::from_value(json!({"targetId":"selected", "type":"page",
            "title":"", "url":"about:blank", "attached":false,"canAccessOpener":false}))
        .unwrap();
        handler.on_target_created(EventTargetCreated {
            target_info: info.clone(),
        });
        let (fetch_tx, fetch_rx) = oneshot::channel();
        handler.scoped_fetches.push((fetch_tx, vec![info]));
        futures::future::poll_fn(|cx| {
            if Pin::new(&mut seen_rx).poll(cx).is_ready() {
                return Poll::Ready(());
            }
            assert!(Pin::new(&mut handler).poll_next(cx).is_pending());
            Poll::Pending
        })
        .await;
        for method in [
            "Page.enable",
            "Browser.close",
            "Target.closeTarget",
            "Target.sendMessageToTarget",
        ] {
            let (tx, rx) = oneshot::channel();
            let msg = CommandMessage {
                method: method.into(),
                session_id: Some("foreign-session".to_owned().into()),
                params: json!({"targetId":"other", "message":"{}"}),
                sender: tx,
            };
            handler
                .submit_external_command(msg, Instant::now())
                .unwrap();
            assert!(matches!(rx.await.unwrap(), Err(CdpError::Policy(_))));
        }
        let started = handler
            .pending_commands
            .values()
            .find(|(_, method, _)| method.as_ref() == "Target.attachToTarget")
            .unwrap()
            .2;
        handler.evict_timed_out_commands(started + handler.config.request_timeout);
        assert!(!handler.disconnecting);
        handler.evict_timed_out_commands(
            started + handler.config.request_timeout + Duration::from_nanos(1),
        );
        assert!(
            fetch_rx
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("timeout")
        );
        assert!(
            handler
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("timeout")
        );
        assert!(handler.next().await.is_none());
        drop(handler);
        let record = server.await.unwrap();
        assert!(
            record
                .iter()
                .any(|c| c["method"] == "Target.attachToTarget")
        );
        for call in record {
            assert!(
                matches!(
                    call["method"].as_str(),
                    Some("Target.setDiscoverTargets" | "Target.attachToTarget")
                ),
                "{call}"
            );
            if call["method"] == "Target.attachToTarget" {
                assert_eq!(call["params"]["targetId"], "selected");
            }
        }
    })
    .await
    .expect("timeout suite зависла");
}
