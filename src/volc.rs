//! VolcEngine streaming ASR — the same speech engine that powers the
//! `pi-voice-input` pi extension.
//!
//! Credentials are read from the extension's config file
//! (`~/.pi/agent/voice-input.config.json` by default, override with
//! `VT_VOLC_CONFIG`) so both tools share a single API key.
//!
//! Unlike the extension (which uses the one-shot endpoint after recording),
//! this module talks to the *streaming* endpoint and emits partial
//! transcripts while you speak, which are typed live at the cursor.

use anyhow::{bail, Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::asr::resample_to_16k;

/// 豆包语音 (speech console) gateway: speech-console API key, bills the
/// speech product.
const WS_BASE_SPEECH: &str = "wss://openspeech.bytedance.com/api/v3/sauc/";
/// 火山方舟 Agent/Coding Plan gateway: authenticated with the *Ark* API key
/// and billed against the plan (超额后付费). Serves model 2.0 only, and only
/// the `bigmodel_async` / `bigmodel_nostream` endpoints.
const WS_BASE_PLAN: &str = "wss://openspeech.bytedance.com/api/v3/plan/sauc/";
/// 豆包流式语音识别模型 2.0 (Seed-ASR): noticeably better on English and
/// Chinese/English code-switching, proper nouns and context. Tried first.
const RESOURCE_ID_V2: &str = "volc.seedasr.sauc.duration";
/// 豆包流式语音识别模型 1.0: fallback when the key/app is not provisioned
/// for 2.0 (the handshake is refused with 401/403 in that case).
const RESOURCE_ID_V1: &str = "volc.bigasr.sauc.duration";
/// Bidirectional streaming, optimized: only replies when the text changes
/// and is the only endpoint that supports two-pass (`enable_nonstream`).
const ENDPOINT_ASYNC: &str = "bigmodel_async";
/// Legacy bidirectional streaming: one reply per audio packet.
const ENDPOINT_STREAM: &str = "bigmodel";
/// Audio is streamed in 200 ms packets: VolcEngine documents 200 ms as the
/// sweet spot for the bidirectional endpoints (100 ms still works but
/// costs accuracy and RTF).
const SEGMENT_MS: usize = 200;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const FINAL_TIMEOUT: Duration = Duration::from_secs(30);

// VolcEngine v3 binary framing (same wire protocol as pi-voice-input).
const MSG_CLIENT_FULL_REQUEST: u8 = 0b0001;
const MSG_CLIENT_AUDIO_ONLY: u8 = 0b0010;
const MSG_SERVER_FULL_RESPONSE: u8 = 0b1001;
const MSG_SERVER_ERROR: u8 = 0b1111;
const FLAG_POS_SEQUENCE: u8 = 0b0001;
const FLAG_NEG_WITH_SEQUENCE: u8 = 0b0011;
const FLAG_SERVER_LAST: u8 = 0b0010;
const SERIALIZATION_NONE: u8 = 0b0000;
const SERIALIZATION_JSON: u8 = 0b0001;
const COMPRESSION_GZIP: u8 = 0b0001;

pub struct VolcConfig {
    /// Speech-console API key (may be empty when only the Ark key is set).
    pub api_key: String,
    /// 火山方舟 API key (`VT_ARK_PLAN_API_KEY`, else `VT_ARK_API_KEY`);
    /// enables the plan gateway.
    pub ark_api_key: String,
    pub boosting_table_id: String,
    pub config_path: PathBuf,
    pub tuning: VolcTuning,
}

/// One way to reach the recognizer: gateway + credential + model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub gateway: &'static str,
    pub base: String,
    pub api_key: String,
    pub resource_id: String,
}

impl VolcConfig {
    /// Routes to try, in order. Auto: Ark plan (2.0) when an Ark key is
    /// set, then the speech console with 2.0, then 1.0. `VT_VOLC_PLAN=0`
    /// skips the plan, `VT_VOLC_PLAN=1` uses only the plan.
    pub fn routes(&self) -> Vec<Route> {
        let t = &self.tuning;
        let pinned = (!t.resource_id.is_empty()).then(|| t.resource_id.clone());
        let mut out = Vec::new();

        // The plan gateway has no legacy `bigmodel` endpoint.
        let plan_possible = !self.ark_api_key.is_empty() && t.endpoint != ENDPOINT_STREAM;
        if plan_possible && t.plan != Some(false) {
            out.push(Route {
                gateway: "ark-plan",
                base: WS_BASE_PLAN.into(),
                api_key: self.ark_api_key.clone(),
                resource_id: pinned.clone().unwrap_or_else(|| RESOURCE_ID_V2.into()),
            });
        }
        if !self.api_key.is_empty() && t.plan != Some(true) {
            let base = if t.ws_base.is_empty() {
                WS_BASE_SPEECH.to_string()
            } else {
                t.ws_base.clone()
            };
            let ids: Vec<String> = match &pinned {
                Some(id) => vec![id.clone()],
                None => vec![RESOURCE_ID_V2.into(), RESOURCE_ID_V1.into()],
            };
            for id in ids {
                out.push(Route {
                    gateway: "speech",
                    base: base.clone(),
                    api_key: self.api_key.clone(),
                    resource_id: id,
                });
            }
        }
        out
    }
}

/// Recognition tuning, all from env so it can be changed without touching
/// the shared pi-voice-input config file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolcTuning {
    /// `X-Api-Resource-Id`. Empty = auto: try the 2.0 model first and fall
    /// back to 1.0 when the handshake is refused.
    pub resource_id: String,
    /// `bigmodel_async` (default) or `bigmodel`.
    pub endpoint: String,
    /// Two-pass recognition: live partials come from the streaming model,
    /// then each finished sentence is re-recognized by the more accurate
    /// non-streaming model. Only honoured on `bigmodel_async`.
    pub two_pass: bool,
    /// Inline hotwords (proper nouns, product names, jargon). The streaming
    /// endpoints accept ~100 tokens, so keep the list short.
    pub hotwords: Vec<String>,
    /// Free-text context for the recognizer ("I am a programmer, I mix
    /// Chinese and English, common terms: Rust, Wayland, ...").
    pub context: String,
    /// Silence (ms) after which a sentence is finalized (`definite`) and,
    /// with two-pass on, re-recognized. `None` = server default (800 ms).
    /// Lower = earlier corrections, but more mid-sentence splits.
    pub end_window_ms: Option<u32>,
    /// `VT_VOLC_PLAN`: None = auto, Some(false) = never use the Ark plan
    /// gateway, Some(true) = only the plan gateway.
    pub plan: Option<bool>,
    /// `VT_VOLC_WS_BASE`: override the speech-console gateway base URL.
    pub ws_base: String,
}

impl VolcTuning {
    pub fn from_env() -> Self {
        let get = |k: &str| std::env::var(k).unwrap_or_default().trim().to_string();
        let endpoint = match get("VT_VOLC_MODE").to_lowercase().as_str() {
            "stream" | "bigmodel" | "legacy" => ENDPOINT_STREAM.to_string(),
            _ => ENDPOINT_ASYNC.to_string(),
        };
        let two_pass = !matches!(
            get("VT_VOLC_TWO_PASS").to_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        );
        let hotwords = get("VT_VOLC_HOTWORDS")
            .split([',', '，', '\n'])
            .map(str::trim)
            .filter(|w| !w.is_empty())
            .map(str::to_string)
            .collect();
        let end_window_ms = get("VT_VOLC_END_WINDOW_MS")
            .parse::<u32>()
            .ok()
            .map(|v| v.max(200)); // server minimum
        let plan = match get("VT_VOLC_PLAN").to_lowercase().as_str() {
            "" | "auto" => None,
            "0" | "false" | "no" | "off" => Some(false),
            _ => Some(true),
        };
        Self {
            resource_id: get("VT_VOLC_RESOURCE_ID"),
            endpoint,
            two_pass,
            hotwords,
            context: get("VT_VOLC_CONTEXT"),
            end_window_ms,
            plan,
            ws_base: get("VT_VOLC_WS_BASE"),
        }
    }
}

/// Key for the 方舟 plan gateway: `VT_ARK_PLAN_API_KEY` if set (a plan
/// key), else
/// `VT_ARK_API_KEY`.
fn ark_api_key_from_env() -> String {
    for var in ["VT_ARK_PLAN_API_KEY", "VT_ARK_API_KEY"] {
        let v = std::env::var(var).unwrap_or_default().trim().to_string();
        if !v.is_empty() {
            return v;
        }
    }
    String::new()
}

/// Loads the shared pi-voice-input config. Returns an error if the file
/// cannot be read/parsed (callers treat a missing/empty key as "unavailable").
/// Loads the VolcEngine credentials.
///
/// Priority:
///   1. `VT_VOLC_API_KEY` env var (optionally `VT_VOLC_BOOSTING_TABLE_ID`)
///      — no file needed at all
///   2. `VT_VOLC_CONFIG` env var — path to a JSON config file
///   3. `~/.pi/agent/voice-input.config.json` (shared with the pi
///      voice-input extension)
pub fn load_config() -> Result<VolcConfig> {
    if let Ok(key) = std::env::var("VT_VOLC_API_KEY") {
        let key = key.trim().to_string();
        if !key.is_empty() {
            return Ok(VolcConfig {
                api_key: key,
                ark_api_key: ark_api_key_from_env(),
                boosting_table_id: std::env::var("VT_VOLC_BOOSTING_TABLE_ID")
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
                config_path: PathBuf::from("<VT_VOLC_API_KEY>"),
                tuning: VolcTuning::from_env(),
            });
        }
    }

    let path = std::env::var("VT_VOLC_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_config_path());
    let ark_api_key = ark_api_key_from_env();
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        // No speech-console config at all is fine when the Ark plan key is
        // set: the plan gateway alone can serve model 2.0.
        Err(_) if !ark_api_key.is_empty() => {
            return Ok(VolcConfig {
                api_key: String::new(),
                ark_api_key,
                boosting_table_id: String::new(),
                config_path: path,
                tuning: VolcTuning::from_env(),
            })
        }
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", path.display()))
        }
    };
    let v: Value = serde_json::from_str(&raw).context("invalid voice input config JSON")?;
    Ok(VolcConfig {
        ark_api_key,
        api_key: v
            .get("volcApiKey")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string(),
        boosting_table_id: v
            .get("boostingTableId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string(),
        config_path: path,
        tuning: VolcTuning::from_env(),
    })
}

fn default_config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".pi")
        .join("agent")
        .join("voice-input.config.json")
}

/// True when the VolcEngine provider can be used (some route exists).
pub fn available() -> bool {
    load_config()
        .map(|c| !c.routes().is_empty())
        .unwrap_or(false)
}

pub enum VolcCmd {
    Audio { samples: Vec<f32>, sample_rate: u32 },
    Finish,
}

pub enum VolcEvent {
    /// WebSocket is open and the request config was accepted.
    Ready,
    /// Incremental recognized text (cumulative) — drives the live preview.
    Partial(String),
    /// Final transcript after the last audio packet was acknowledged.
    Final(String),
    Failed(String),
}

pub struct VolcSession {
    cmd_tx: mpsc::Sender<VolcCmd>,
    pub events: Receiver<VolcEvent>,
}

impl VolcSession {
    /// Spawns a worker thread that owns its own single-threaded tokio
    /// runtime. `connect_async` happens in the background; audio queued on
    /// the command channel is flushed once the socket is up.
    pub fn start() -> Result<Self> {
        let cfg = load_config()?;
        if cfg.routes().is_empty() {
            bail!(
                "VolcEngine API key missing: set VT_ARK_API_KEY (方舟 plan) or \
                 VT_VOLC_API_KEY, or configure volcApiKey in {} (e.g. via /voice key \
                 inside pi)",
                cfg.config_path.display()
            );
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<VolcCmd>(512);
        let (ev_tx, ev_rx) = bounded::<VolcEvent>(256);

        std::thread::Builder::new()
            .name("volc-asr".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ev_tx.try_send(VolcEvent::Failed(format!(
                            "failed to build tokio runtime: {e}"
                        )));
                        return;
                    }
                };
                let ev_report = ev_tx.clone();
                if let Err(e) = rt.block_on(run(&mut cmd_rx, ev_tx, cfg)) {
                    let _ = ev_report.try_send(VolcEvent::Failed(format!("{e:#}")));
                }
            })
            .context("failed to spawn volc-asr worker thread")?;

        Ok(Self {
            cmd_tx,
            events: ev_rx,
        })
    }

    /// Queues an audio chunk (any sample rate, mono f32); drops silently if
    /// the worker is backed up so the audio callback never blocks.
    pub fn send_audio(&self, samples: &[f32], sample_rate: u32) {
        let _ = self.cmd_tx.try_send(VolcCmd::Audio {
            samples: samples.to_vec(),
            sample_rate,
        });
    }

    /// Signals end-of-audio: sends the last packet and makes the worker
    /// wait for the final transcript.
    pub fn finish(&self) {
        let _ = self.cmd_tx.try_send(VolcCmd::Finish);
    }
}

macro_rules! vlog {
    ($($arg:tt)*) => {
        if std::env::var_os("VT_LOG").is_some_and(|v| !v.is_empty()) {
            eprintln!("[vt/volc] {}", format!($($arg)*));
        }
    };
}

/// Opens the websocket over one route. `Ok(None)` means the gateway
/// refused it in a way the next route may fix: 401/403 (key not granted
/// for that model) or 404 (endpoint not offered by that gateway).
async fn connect(
    cfg: &VolcConfig,
    route: &Route,
) -> Result<Option<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>
{
    use tokio_tungstenite::tungstenite::Error as WsError;

    let connect_id = uuid::Uuid::new_v4().to_string();
    let url = format!("{}{}", route.base, cfg.tuning.endpoint);
    let mut req = url
        .as_str()
        .into_client_request()
        .context("invalid ASR websocket URL")?;
    let headers = req.headers_mut();
    headers.insert("X-Api-Key", HeaderValue::from_str(&route.api_key)?);
    headers.insert("X-Api-Resource-Id", HeaderValue::from_str(&route.resource_id)?);
    headers.insert("X-Api-Connect-Id", HeaderValue::from_str(&connect_id)?);
    headers.insert("X-Api-Request-Id", HeaderValue::from_str(&connect_id)?);
    headers.insert("X-Api-Sequence", HeaderValue::from_static("-1"));

    let res = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(req))
        .await
        .map_err(|_| anyhow::anyhow!("timed out connecting to VolcEngine ASR"))?;
    match res {
        Ok((ws, resp)) => {
            let logid = resp
                .headers()
                .get("X-Tt-Logid")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-");
            vlog!("connected: url={} resource={} logid={}", url, route.resource_id, logid);
            announce_model(cfg, route);
            Ok(Some(ws))
        }
        Err(WsError::Http(resp)) if matches!(resp.status().as_u16(), 401 | 403 | 404) => {
            let body = resp
                .body()
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            vlog!(
                "handshake refused: gateway={} resource={} status={} body={}",
                route.gateway,
                route.resource_id,
                resp.status(),
                body.trim()
            );
            Ok(None)
        }
        Err(e) => Err(e).context("failed to connect to VolcEngine ASR"),
    }
}

/// One line on the first successful connection so the user can tell which
/// model actually serves them (the 2.0 → 1.0 fallback is otherwise silent).
fn announce_model(cfg: &VolcConfig, route: &Route) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static ANNOUNCED: AtomicBool = AtomicBool::new(false);
    if ANNOUNCED.swap(true, Ordering::Relaxed) {
        return;
    }
    let resource_id = route.resource_id.as_str();
    let generation = if resource_id.contains("seedasr") {
        "2.0 (seedasr)"
    } else if resource_id.contains("bigasr") {
        "1.0 (bigasr)"
    } else {
        "custom"
    };
    let t = &cfg.tuning;
    eprintln!(
        "[vt] VolcEngine ASR: model {generation} via {}, endpoint {}, two-pass {}, hotwords {}, context {}",
        route.gateway,
        t.endpoint,
        if t.two_pass && t.endpoint == ENDPOINT_ASYNC { "on" } else { "off" },
        t.hotwords.len(),
        if t.context.is_empty() { "no" } else { "yes" },
    );
}

async fn run(
    cmd_rx: &mut mpsc::Receiver<VolcCmd>,
    ev: Sender<VolcEvent>,
    cfg: VolcConfig,
) -> Result<()> {
    let routes = cfg.routes();
    let mut ws = None;
    for (i, route) in routes.iter().enumerate() {
        match connect(&cfg, route).await? {
            Some(s) => {
                ws = Some(s);
                break;
            }
            None if i + 1 < routes.len() => {
                let next = &routes[i + 1];
                eprintln!(
                    "[vt] VolcEngine {} gateway refused {}; falling back to {} / {}",
                    route.gateway, route.resource_id, next.gateway, next.resource_id
                );
            }
            None => bail!(
                "VolcEngine ASR refused every route (last: {} gateway, resource {}): \
                 check the API key and that the model is enabled for it \
                 (VT_LOG=1 shows the server's reason; override with VT_VOLC_RESOURCE_ID / VT_VOLC_PLAN)",
                route.gateway,
                route.resource_id
            ),
        }
    }
    let ws = ws.expect("at least one route");
    let _ = ev.try_send(VolcEvent::Ready);

    let (mut write, mut read) = ws.split();

    // 1) full request (session config) with sequence 1
    write
        .send(Message::Binary(
            full_request(1, &request_payload(&cfg))?.into(),
        ))
        .await
        .context("failed to send ASR full request")?;
    let mut seq: i32 = 2;
    vlog!("full request sent");

    // Pending 16 kHz mono s16le PCM bytes waiting to be segmented.
    let mut pcm: Vec<u8> = Vec::new();
    let seg_bytes = 16000 * 2 * SEGMENT_MS / 1000;
    let mut last_text = String::new();
    let mut finished = false;

    loop {
        let read_timeout = if finished {
            FINAL_TIMEOUT
        } else {
            IDLE_TIMEOUT
        };
        tokio::select! {
            cmd = cmd_rx.recv(), if !finished => {
                match cmd {
                    Some(VolcCmd::Audio { samples, sample_rate }) => {
                        vlog!("audio cmd: {} samples @ {}", samples.len(), sample_rate);
                        for s in resample_to_16k(&samples, sample_rate) {
                            let b = (s.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
                            pcm.extend_from_slice(&b.to_le_bytes());
                        }
                        while pcm.len() >= seg_bytes {
                            let seg: Vec<u8> = pcm.drain(..seg_bytes).collect();
                            vlog!("sending audio packet seq={}", seq);
                            write
                                .send(Message::Binary(audio_request(seq, &seg, false)?.into()))
                                .await
                                .context("failed to send ASR audio packet")?;
                            seq += 1;
                        }
                    }
                    Some(VolcCmd::Finish) | None => {
                        vlog!("finish cmd; sending last packet seq={}", seq);
                        let tail = std::mem::take(&mut pcm);
                        write
                            .send(Message::Binary(audio_request(seq, &tail, true)?.into()))
                            .await
                            .context("failed to send final ASR audio packet")?;
                        finished = true;
                    }
                }
            }

            msg = tokio::time::timeout(read_timeout, read.next()) => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => bail!("timed out waiting for ASR response"),
                };
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        let frame = parse_frame(&data)?;
                        let text = extract_text(&frame.payload);
                        vlog!("server frame: is_last={} text={:?}", frame.is_last, text);
                        if !text.is_empty() {
                            last_text = text.clone();
                        }
                        if frame.is_last {
                            let _ = ev.try_send(VolcEvent::Final(last_text.clone()));
                            return Ok(());
                        }
                        if !text.is_empty() {
                            let _ = ev.try_send(VolcEvent::Partial(text));
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        if finished {
                            let _ = ev.try_send(VolcEvent::Final(last_text.clone()));
                            return Ok(());
                        }
                        bail!("ASR socket closed before the final result");
                    }
                    Some(Ok(_)) => {} // ping/pong handled by tungstenite
                    Some(Err(e)) => bail!("ASR socket error: {e}"),
                }
            }
        }
    }
}

fn request_payload(cfg: &VolcConfig) -> Value {
    let t = &cfg.tuning;
    let mut request = json!({
        "model_name": "bigmodel",
        "enable_itn": true,
        "enable_punc": true,
        "enable_ddc": false,
        "result_type": "full",
    });
    // Two-pass is only implemented on the optimized bidirectional endpoint.
    if t.two_pass && t.endpoint == ENDPOINT_ASYNC {
        request["enable_nonstream"] = json!(true);
    }
    if let Some(ms) = t.end_window_ms {
        request["end_window_size"] = json!(ms);
    }

    let mut corpus = serde_json::Map::new();
    if !cfg.boosting_table_id.is_empty() {
        corpus.insert("boosting_table_id".into(), json!(cfg.boosting_table_id));
    }
    // Inline hotwords and free-text context share the `corpus.context`
    // field, which the API expects as a JSON *string*.
    let mut ctx = serde_json::Map::new();
    if !t.hotwords.is_empty() {
        let words: Vec<Value> = t.hotwords.iter().map(|w| json!({ "word": w })).collect();
        ctx.insert("hotwords".into(), Value::Array(words));
    }
    if !t.context.is_empty() {
        ctx.insert("context_type".into(), json!("dialog_ctx"));
        ctx.insert("context_data".into(), json!([{ "text": t.context }]));
    }
    if !ctx.is_empty() {
        corpus.insert("context".into(), json!(Value::Object(ctx).to_string()));
    }
    if !corpus.is_empty() {
        request["corpus"] = Value::Object(corpus);
    }

    json!({
        "user": { "uid": "voice-type" },
        "audio": {
            "format": "pcm",
            "codec": "raw",
            "rate": 16000,
            "bits": 16,
            "channel": 1,
        },
        "request": request,
    })
}

// ---------------------------------------------------------------------------
// VolcEngine v3 binary framing
// ---------------------------------------------------------------------------

fn frame_header(message_type: u8, flags: u8, serialization: u8, compression: u8) -> [u8; 4] {
    [
        0x11,
        (message_type << 4) | flags,
        (serialization << 4) | compression,
        0,
    ]
}

/// protocol version 1, header size 1 (4 bytes)
fn full_request(seq: i32, payload: &Value) -> Result<Vec<u8>> {
    let body = gzip(payload.to_string().as_bytes())?;
    let mut out = Vec::with_capacity(16 + body.len());
    out.extend_from_slice(&frame_header(
        MSG_CLIENT_FULL_REQUEST,
        FLAG_POS_SEQUENCE,
        SERIALIZATION_JSON,
        COMPRESSION_GZIP,
    ));
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Audio-only packet; the last packet uses a negative sequence + flag 0b0011.
fn audio_request(seq: i32, audio: &[u8], is_last: bool) -> Result<Vec<u8>> {
    let body = gzip(audio)?;
    let flags = if is_last {
        FLAG_NEG_WITH_SEQUENCE
    } else {
        FLAG_POS_SEQUENCE
    };
    let wire_seq = if is_last { -seq } else { seq };
    let mut out = Vec::with_capacity(16 + body.len());
    out.extend_from_slice(&frame_header(
        MSG_CLIENT_AUDIO_ONLY,
        flags,
        SERIALIZATION_NONE,
        COMPRESSION_GZIP,
    ));
    out.extend_from_slice(&wire_seq.to_be_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

#[derive(Debug)]
struct ServerFrame {
    is_last: bool,
    payload: Value,
}

fn parse_frame(msg: &[u8]) -> Result<ServerFrame> {
    if msg.len() < 4 {
        bail!("ASR frame too short");
    }
    let header_size = (msg[0] & 0x0f) as usize * 4;
    if msg.len() < header_size {
        bail!("ASR frame header truncated");
    }
    let message_type = msg[1] >> 4;
    let flags = msg[1] & 0x0f;
    let serialization = msg[2] >> 4;
    let compression = msg[2] & 0x0f;
    let mut off = header_size;

    if flags & FLAG_POS_SEQUENCE != 0 {
        if off + 4 > msg.len() {
            bail!("ASR frame sequence truncated");
        }
        let _seq = i32::from_be_bytes(msg[off..off + 4].try_into().unwrap());
        off += 4;
    }

    match message_type {
        MSG_SERVER_FULL_RESPONSE => {
            if off + 4 > msg.len() {
                bail!("ASR frame payload size truncated");
            }
            let size = u32::from_be_bytes(msg[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + size > msg.len() {
                bail!("ASR frame payload truncated");
            }
            let payload = maybe_gunzip(&msg[off..off + size], compression)?;
            let value = if serialization == SERIALIZATION_JSON && !payload.is_empty() {
                serde_json::from_slice(&payload).context("failed to parse ASR JSON payload")?
            } else {
                Value::Null
            };
            Ok(ServerFrame {
                is_last: flags & FLAG_SERVER_LAST != 0,
                payload: value,
            })
        }
        MSG_SERVER_ERROR => {
            if off + 4 > msg.len() {
                bail!("ASR error frame truncated");
            }
            let code = i32::from_be_bytes(msg[off..off + 4].try_into().unwrap());
            off += 4;
            let size = if off + 4 <= msg.len() {
                let s = u32::from_be_bytes(msg[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                s
            } else {
                0
            };
            let payload = if size > 0 && off + size <= msg.len() {
                maybe_gunzip(&msg[off..off + size], compression)
                    .unwrap_or_else(|_| msg[off..off + size].to_vec())
            } else {
                Vec::new()
            };
            bail!(
                "VolcEngine ASR error {}: {}",
                code,
                String::from_utf8_lossy(&payload)
            );
        }
        _ => Ok(ServerFrame {
            is_last: false,
            payload: Value::Null,
        }),
    }
}

fn extract_text(payload: &Value) -> String {
    if let Some(text) = payload.pointer("/result/text").and_then(Value::as_str) {
        let text = text.trim();
        if !text.is_empty() {
            return text.to_string();
        }
    }
    if let Some(utts) = payload
        .pointer("/result/utterances")
        .and_then(Value::as_array)
    {
        let joined: String = utts
            .iter()
            .filter_map(|u| u.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("");
        let joined = joined.trim();
        if !joined.is_empty() {
            return joined.to_string();
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// gzip helpers
// ---------------------------------------------------------------------------

fn gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(data).context("gzip write failed")?;
    enc.finish().context("gzip finish failed")
}

fn maybe_gunzip(data: &[u8], compression: u8) -> Result<Vec<u8>> {
    if compression != COMPRESSION_GZIP || data.is_empty() {
        return Ok(data.to_vec());
    }
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).context("gunzip failed")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn env_api_key_takes_precedence_over_files() {
        // Point the file fallback at a nonexistent path: if the env var is
        // honored, no file is ever read.
        std::env::set_var("VT_VOLC_CONFIG", "/nonexistent/voice-input.config.json");
        std::env::set_var("VT_VOLC_API_KEY", "test-key-from-env");
        std::env::set_var("VT_VOLC_BOOSTING_TABLE_ID", "tbl-123");
        let cfg = load_config().unwrap();
        assert_eq!(cfg.api_key, "test-key-from-env");
        assert_eq!(cfg.boosting_table_id, "tbl-123");
        assert_eq!(cfg.config_path, PathBuf::from("<VT_VOLC_API_KEY>"));
        std::env::remove_var("VT_VOLC_API_KEY");
        std::env::remove_var("VT_VOLC_BOOSTING_TABLE_ID");
        std::env::remove_var("VT_VOLC_CONFIG");
    }

    fn test_cfg(tuning: VolcTuning) -> VolcConfig {
        VolcConfig {
            api_key: "k".into(),
            ark_api_key: String::new(),
            boosting_table_id: String::new(),
            config_path: PathBuf::new(),
            tuning,
        }
    }

    fn default_tuning() -> VolcTuning {
        VolcTuning {
            resource_id: String::new(),
            endpoint: ENDPOINT_ASYNC.into(),
            two_pass: true,
            hotwords: vec![],
            context: String::new(),
            end_window_ms: None,
            plan: None,
            ws_base: String::new(),
        }
    }

    #[test]
    fn end_window_env_is_clamped_to_server_minimum() {
        std::env::set_var("VT_VOLC_END_WINDOW_MS", "100");
        assert_eq!(VolcTuning::from_env().end_window_ms, Some(200));
        std::env::set_var("VT_VOLC_END_WINDOW_MS", "600");
        let t = VolcTuning::from_env();
        assert_eq!(t.end_window_ms, Some(600));
        assert_eq!(request_payload(&test_cfg(t))["request"]["end_window_size"], json!(600));
        std::env::set_var("VT_VOLC_END_WINDOW_MS", "abc");
        assert_eq!(VolcTuning::from_env().end_window_ms, None);
        std::env::remove_var("VT_VOLC_END_WINDOW_MS");
    }

    #[test]
    fn routes_speech_key_only_tries_v2_then_v1() {
        let cfg = test_cfg(default_tuning());
        let r = cfg.routes();
        assert_eq!(r.len(), 2);
        assert_eq!((r[0].gateway, r[0].resource_id.as_str()), ("speech", RESOURCE_ID_V2));
        assert_eq!((r[1].gateway, r[1].resource_id.as_str()), ("speech", RESOURCE_ID_V1));
        assert!(r[0].base.starts_with(WS_BASE_SPEECH));
        assert_eq!(r[0].api_key, "k");
    }

    #[test]
    fn routes_ark_key_puts_plan_first_and_never_offers_v1_on_plan() {
        let mut cfg = test_cfg(default_tuning());
        cfg.ark_api_key = "ark-x".into();
        let r = cfg.routes();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].gateway, "ark-plan");
        assert_eq!(r[0].base, WS_BASE_PLAN);
        assert_eq!(r[0].api_key, "ark-x");
        assert_eq!(r[0].resource_id, RESOURCE_ID_V2);
        assert_eq!(r[1].gateway, "speech");

        // Legacy `bigmodel` endpoint does not exist on the plan gateway.
        cfg.tuning.endpoint = ENDPOINT_STREAM.into();
        assert!(cfg.routes().iter().all(|r| r.gateway == "speech"));
    }

    #[test]
    fn routes_plan_switch_and_pinned_resource() {
        let mut cfg = test_cfg(default_tuning());
        cfg.ark_api_key = "ark-x".into();
        cfg.tuning.plan = Some(false);
        assert!(cfg.routes().iter().all(|r| r.gateway == "speech"));

        cfg.tuning.plan = Some(true);
        let r = cfg.routes();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].gateway, "ark-plan");

        cfg.tuning.plan = None;
        cfg.tuning.resource_id = "volc.custom".into();
        let r = cfg.routes();
        assert_eq!(r.len(), 2);
        assert!(r.iter().all(|r| r.resource_id == "volc.custom"));

        // Ark key alone (no speech config) is still a usable provider.
        cfg.api_key.clear();
        cfg.tuning.resource_id.clear();
        assert_eq!(cfg.routes().len(), 1);
    }

    #[test]
    fn payload_two_pass_only_on_async_endpoint() {
        let p = request_payload(&test_cfg(default_tuning()));
        assert_eq!(p["request"]["enable_nonstream"], json!(true));
        assert!(p["request"].get("corpus").is_none());

        let legacy = VolcTuning {
            endpoint: ENDPOINT_STREAM.into(),
            ..default_tuning()
        };
        let p = request_payload(&test_cfg(legacy));
        assert!(p["request"].get("enable_nonstream").is_none());
    }

    #[test]
    fn payload_hotwords_and_context_are_a_json_string() {
        let t = VolcTuning {
            hotwords: vec!["Rust".into(), "Wayland".into()],
            context: "我是程序员，中英混说".into(),
            ..default_tuning()
        };
        let p = request_payload(&test_cfg(t));
        let ctx = p["request"]["corpus"]["context"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(ctx).unwrap();
        assert_eq!(parsed["hotwords"][1]["word"], "Wayland");
        assert_eq!(parsed["context_type"], "dialog_ctx");
        assert_eq!(parsed["context_data"][0]["text"], "我是程序员，中英混说");
    }

    #[test]
    fn hotwords_env_splits_on_ascii_and_fullwidth_commas() {
        std::env::set_var("VT_VOLC_HOTWORDS", " Rust, Wayland，sway ,,");
        std::env::set_var("VT_VOLC_MODE", "stream");
        std::env::set_var("VT_VOLC_TWO_PASS", "off");
        let t = VolcTuning::from_env();
        assert_eq!(t.hotwords, vec!["Rust", "Wayland", "sway"]);
        assert_eq!(t.endpoint, ENDPOINT_STREAM);
        assert!(!t.two_pass);
        std::env::remove_var("VT_VOLC_HOTWORDS");
        std::env::remove_var("VT_VOLC_MODE");
        std::env::remove_var("VT_VOLC_TWO_PASS");
    }

    fn server_full_frame(seq: i32, payload: &Value, is_last: bool) -> Vec<u8> {
        let body = gzip(payload.to_string().as_bytes()).unwrap();
        let flags = FLAG_POS_SEQUENCE | if is_last { FLAG_SERVER_LAST } else { 0 };
        let mut out = vec![
            0x11u8,
            (MSG_SERVER_FULL_RESPONSE << 4) | flags,
            (SERIALIZATION_JSON << 4) | COMPRESSION_GZIP,
            0,
        ];
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn parse_server_full_response_frame() {
        let payload = json!({"result": {"text": "你好 world"}});
        let frame = server_full_frame(7, &payload, false);
        let parsed = parse_frame(&frame).unwrap();
        assert!(!parsed.is_last);
        assert_eq!(extract_text(&parsed.payload), "你好 world");
    }

    #[test]
    fn parse_last_frame_with_utterances_fallback() {
        let payload =
            json!({"result": {"text": "", "utterances": [{"text": "a "}, {"text": "b"}]}});
        let frame = server_full_frame(9, &payload, true);
        let parsed = parse_frame(&frame).unwrap();
        assert!(parsed.is_last);
        assert_eq!(extract_text(&parsed.payload), "a b");
    }

    #[test]
    fn parse_error_frame() {
        let body = gzip(br#"{"message":"bad key"}"#).unwrap();
        let mut frame = vec![
            0x11u8,
            MSG_SERVER_ERROR << 4,
            (SERIALIZATION_JSON << 4) | COMPRESSION_GZIP,
            0,
        ];
        frame.extend_from_slice(&45000003i32.to_be_bytes());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        let err = parse_frame(&frame).unwrap_err();
        assert!(err.to_string().contains("45000003"));
    }

    /// End-to-end protocol check against the real streaming endpoint using
    /// the shared pi-voice-input API key. Run with:
    ///   cargo test -- --ignored live_streaming_session --nocapture
    #[test]
    #[ignore]
    fn live_streaming_session() {
        if !available() {
            eprintln!("skip: no VolcEngine API key configured");
            return;
        }
        let session = VolcSession::start().unwrap();

        // ~1.5 s of 440 Hz tone at 44.1 kHz (exercises the resample path too).
        let sr = 44_100u32;
        let n = sr as usize * 3 / 2;
        let mut samples = Vec::with_capacity(n);
        let mut t = 0.0f32;
        for _ in 0..n {
            samples.push((t * 2.0 * std::f32::consts::PI * 440.0).sin() * 0.3);
            t += 1.0 / sr as f32;
        }
        session.send_audio(&samples, sr);
        session.finish();

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut saw_partial = false;
        loop {
            match session.events.recv_timeout(Duration::from_millis(200)) {
                Ok(VolcEvent::Ready) => {}
                Ok(VolcEvent::Partial(_)) => saw_partial = true,
                Ok(VolcEvent::Final(text)) => {
                    eprintln!("final={text:?} partial_seen={saw_partial}");
                    break; // protocol round-trip succeeded (text may be empty)
                }
                Ok(VolcEvent::Failed(e)) => panic!("streaming failed: {e}"),
                Err(_) if Instant::now() > deadline => panic!("timeout waiting for final"),
                Err(_) => {}
            }
        }
    }
}
