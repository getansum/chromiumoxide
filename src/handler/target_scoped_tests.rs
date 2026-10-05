use super::*;
use futures::task::noop_waker;
use serde_json::json;

fn target(safe: bool) -> Target {
    let info = serde_json::from_value(json!({"targetId":"selected", "type":"page", "title":"",
        "url":"about:blank", "attached":false, "canAccessOpener":false}))
    .unwrap();
    Target::new(
        info,
        TargetConfig {
            non_destructive: safe,
            ..TargetConfig::default()
        },
        BrowserContext::default(),
    )
}

#[test]
fn initialization_timeout_boundary_preserves_owned_cleanup() {
    let now = Instant::now();
    let timeout = Duration::from_millis(10);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for safe in [false, true] {
        let mut target = target(safe);
        target.init_state = TargetInit::InitializingPage(CommandChain::new(
            vec![("Log.enable".into(), json!({}))],
            timeout,
        ));
        assert!(matches!(
            target.poll(&mut cx, now),
            Some(TargetEvent::Request(_))
        ));
        assert!(target.poll(&mut cx, now + timeout).is_none());
        let failure = target
            .poll(&mut cx, now + timeout + Duration::from_nanos(1))
            .unwrap();
        match failure {
            TargetEvent::InitializationFailed(reason) => {
                assert!(safe);
                assert!(reason.contains("timed out"));
            }
            TargetEvent::Request(req) => {
                assert!(!safe);
                assert_eq!(req.method.as_ref(), "Target.closeTarget");
                assert_eq!(req.params["targetId"], "selected");
            }
            other => panic!("{other:?}"),
        }
        assert!(target.poll(&mut cx, now + timeout * 2).is_none());
        assert!(target.get_or_create_page().is_none());
    }
}

#[test]
fn cdp_rejection_is_not_successful_initialization() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for safe in [false, true] {
        let mut target = target(safe);
        target.init_state = TargetInit::InitializingPage(CommandChain::default());
        let response =
            serde_json::from_value(json!({"id":1,"error":{"code":-32000,"message":"denied init"}}))
                .unwrap();
        target.on_response(response, "Log.enable");
        assert!(!target.is_initialized());
        let failure = target.poll(&mut cx, Instant::now()).unwrap();
        match failure {
            TargetEvent::InitializationFailed(reason) => {
                assert!(safe);
                assert!(reason.contains("denied init"));
            }
            TargetEvent::Request(req) => {
                assert!(!safe);
                assert_eq!(req.method.as_ref(), "Target.closeTarget");
            }
            other => panic!("{other:?}"),
        }
    }
}
