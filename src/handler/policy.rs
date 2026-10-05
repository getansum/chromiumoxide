use chromiumoxide_cdp::cdp::browser_protocol::target::{SessionId, TargetId};

use crate::error::{CdpError, Result};

/// Граница управления соединения. Legacy сохраняет поведение connect/launch.
///
/// Полный lifecycle выбранной страницы (handler опрашивается до завершения):
/// ```no_run
/// use chromiumoxide::{Browser, error::CdpError};
/// use chromiumoxide::handler::{ConnectionPolicy, HandlerConfig};
/// use chromiumoxide::cdp::browser_protocol::target::TargetId;
/// use futures::StreamExt;
///
/// async fn inspect_selected(url: String, id: TargetId)
///     -> Result<(), Box<dyn std::error::Error + Send + Sync>>
/// {
///     let config = HandlerConfig {
///         connection_policy: ConnectionPolicy::TargetScoped(id.clone()),
///         ..HandlerConfig::default()
///     };
///     let (mut browser, mut handler) = Browser::connect_with_config(url, config).await?;
///     let driver = tokio::spawn(async move {
///         let mut first_error = None;
///         while let Some(result) = handler.next().await {
///             if let Err(error) = result {
///                 if first_error.is_none() { first_error = Some(error); }
///             }
///         }
///         match first_error { Some(error) => Err(error), None => Ok(()) }
///     });
///     let result: Result<(), CdpError> = async {
///         browser.fetch_targets().await?; // Барьер успешного init.
///         let page = browser.get_page(id).await?; // Никакого first-page fallback.
///         let _actual_url = page.url().await?;
///         Ok(())
///     }.await;
///     let disconnected = browser.disconnect().await;
///     drop(browser);
///     let driven = driver.await?; // Не abort: ждём завершения транспорта.
///     result?;
///     disconnected?;
///     driven?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ConnectionPolicy {
    #[default]
    Legacy,
    /// Управлять только указанной существующей страницей, без права закрытия.
    TargetScoped(TargetId),
}

impl ConnectionPolicy {
    pub fn target_id(&self) -> Option<&TargetId> {
        match self {
            Self::Legacy => None,
            Self::TargetScoped(id) => Some(id),
        }
    }

    pub(crate) fn check(
        &self,
        method: &str,
        session: Option<&SessionId>,
        selected_session: Option<&SessionId>,
        params: &serde_json::Value,
        internal: bool,
    ) -> Result<()> {
        let Some(target) = self.target_id() else {
            return Ok(());
        };
        // Browser/Target команды остаются глобальными даже с sessionId.
        let allowed = if method.starts_with("Target.") || method.starts_with("Browser.") {
            session.is_none()
                && match method {
                    "Target.setDiscoverTargets" => internal,
                    "Target.attachToTarget" => {
                        internal
                            && params.get("targetId").and_then(|v| v.as_str())
                                == Some(target.as_ref())
                            && params.get("flatten").and_then(|v| v.as_bool()) == Some(true)
                    }
                    "Target.getTargets" | "Browser.getVersion" => true,
                    _ => false,
                }
        } else {
            // Никаких browser-wide команд без выбранной session; запрещены
            // домены глобального управления, включая raw CDP обходы Page.close.
            let domain = method.split_once('.').map(|(domain, _)| domain);
            session.is_some()
                && session == selected_session
                && matches!(
                    domain,
                    Some(
                        "Accessibility"
                            | "Animation"
                            | "Audits"
                            | "Autofill"
                            | "CSS"
                            | "DOM"
                            | "DOMDebugger"
                            | "DOMSnapshot"
                            | "DOMStorage"
                            | "Debugger"
                            | "Emulation"
                            | "EventBreakpoints"
                            | "Fetch"
                            | "IndexedDB"
                            | "Input"
                            | "Inspector"
                            | "LayerTree"
                            | "Log"
                            | "Media"
                            | "Network"
                            | "Overlay"
                            | "Page"
                            | "Performance"
                            | "PerformanceTimeline"
                            | "Preload"
                            | "Profiler"
                            | "Runtime"
                            | "Schema"
                            | "Security"
                            | "WebAudio"
                            | "WebAuthn"
                    )
                )
                && !matches!(
                    method,
                    "Page.close"
                        | "Page.crash"
                        | "Network.clearBrowserCache"
                        | "Network.clearBrowserCookies"
                        | "Security.setIgnoreCertificateErrors"
                )
        };
        if allowed {
            Ok(())
        } else {
            Err(CdpError::Policy(format!(
                "{method}: target {target:?}, session {session:?}"
            )))
        }
    }
}
