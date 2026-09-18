//! 豆包流式语音识别 (VolcEngine streaming ASR, v3 binary protocol) over the
//! `bigmodel_async` bidirectional endpoint with two-pass recognition.

use anyhow::{bail, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// 火山方舟 Agent Plan gateway: Ark API key, model 2.0 only.
const URL_PLAN: &str = "wss://openspeech.bytedance.com/api/v3/plan/sauc/bigmodel_async";
/// 豆包语音 console gateway: speech API key, models 2.0 and 1.0.
const URL_SPEECH: &str = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async";
const RESOURCE_V2: &str = "volc.seedasr.sauc.duration";
const RESOURCE_V1: &str = "volc.bigasr.sauc.duration";
/// 200 ms of 16 kHz s16le per packet (the documented sweet spot).
const SEGMENT_BYTES: usize = 16_000 * 2 * 200 / 1000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const FINAL_TIMEOUT: Duration = Duration::from_secs(30);

// Frame header fields.
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

#[derive(Debug, Default, Clone)]
pub struct Config {
    pub ark_api_key: String,
    pub speech_api_key: String,
    pub hotwords: Vec<String>,
    pub context: String,
    pub audio_device: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Route {
    pub url: &'static str,
    pub api_key: String,
    pub resource_id: &'static str,
}

impl Config {
    /// Gateways to try in order: Ark plan (2.0), then speech console 2.0, then 1.0.
    pub fn routes(&self) -> Vec<Route> {
        let mut out = Vec::new();
        if !self.ark_api_key.is_empty() {
            out.push(Route {
                url: URL_PLAN,
                api_key: self.ark_api_key.clone(),
                resource_id: RESOURCE_V2,
            });
        }
        if !self.speech_api_key.is_empty() {
            for resource_id in [RESOURCE_V2, RESOURCE_V1] {
                out.push(Route {
                    url: URL_SPEECH,
                    api_key: self.speech_api_key.clone(),
                    resource_id,
                });
            }
        }
        out
    }
}

pub enum Event {
    /// Cumulative text recognized so far.
    Partial(String),
    Final(String),
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Opens the first route the gateways accept. A 401/403/404 handshake
/// refusal means "not provisioned here", so the next route is tried.
async fn connect(cfg: &Config) -> Result<Ws> {
    use tokio_tungstenite::tungstenite::Error as WsError;
    let routes = cfg.routes();
    if routes.is_empty() {
        bail!("no API key configured (set ArkApiKey or SpeechApiKey)");
    }
    let mut refused = String::new();
    for route in &routes {
        let id = uuid::Uuid::new_v4().to_string();
        let mut req = route.url.into_client_request()?;
        let h = req.headers_mut();
        h.insert("X-Api-Key", HeaderValue::from_str(&route.api_key)?);
        h.insert(
            "X-Api-Resource-Id",
            HeaderValue::from_static(route.resource_id),
        );
        h.insert("X-Api-Connect-Id", HeaderValue::from_str(&id)?);
        h.insert("X-Api-Request-Id", HeaderValue::from_str(&id)?);
        h.insert("X-Api-Sequence", HeaderValue::from_static("-1"));
        let res = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(req))
            .await
            .map_err(|_| anyhow::anyhow!("timed out connecting to VolcEngine ASR"))?;
        match res {
            Ok((ws, _)) => return Ok(ws),
            Err(WsError::Http(resp)) if matches!(resp.status().as_u16(), 401 | 403 | 404) => {
                refused = format!(
                    "{} ({}) -> HTTP {}",
                    route.url,
                    route.resource_id,
                    resp.status()
                );
            }
            Err(e) => return Err(e).context("failed to connect to VolcEngine ASR"),
        }
    }
    bail!("VolcEngine ASR refused every route, last: {refused}")
}

/// Streams `audio` (16 kHz mono s16le) until a `finish` signal arrives,
/// then waits for the final transcript. `stop_capture` runs on finish so
/// the microphone closes before the last packet goes out. A closed `finish`
/// channel cancels the session silently.
pub async fn run(
    cfg: &Config,
    mut audio: mpsc::UnboundedReceiver<Vec<u8>>,
    mut finish: mpsc::Receiver<()>,
    stop_capture: impl FnOnce(),
    mut on_event: impl FnMut(Event),
) -> Result<()> {
    let (mut write, mut read) = connect(cfg).await?.split();
    write
        .send(Message::Binary(full_request(&request_payload(cfg))?.into()))
        .await
        .context("failed to send ASR request")?;

    let mut seq: i32 = 2;
    let mut pcm: Vec<u8> = Vec::new();
    let mut last_text = String::new();
    let mut stop_capture = Some(stop_capture);
    let mut audio_open = true;
    let mut finished = false;

    loop {
        let read_timeout = if finished {
            FINAL_TIMEOUT
        } else {
            IDLE_TIMEOUT
        };
        tokio::select! {
            chunk = audio.recv(), if audio_open && !finished => match chunk {
                Some(chunk) => {
                    pcm.extend_from_slice(&chunk);
                    while pcm.len() >= SEGMENT_BYTES {
                        let seg: Vec<u8> = pcm.drain(..SEGMENT_BYTES).collect();
                        write.send(Message::Binary(audio_request(seq, &seg, false)?.into()))
                            .await.context("failed to send audio packet")?;
                        seq += 1;
                    }
                }
                None => audio_open = false,
            },
            sig = finish.recv() => {
                if sig.is_none() {
                    return Ok(()); // cancelled
                }
                if finished {
                    continue;
                }
                if let Some(stop) = stop_capture.take() {
                    stop();
                }
                while let Ok(chunk) = audio.try_recv() {
                    pcm.extend_from_slice(&chunk);
                }
                let tail = std::mem::take(&mut pcm);
                write.send(Message::Binary(audio_request(seq, &tail, true)?.into()))
                    .await.context("failed to send final audio packet")?;
                finished = true;
            }
            msg = tokio::time::timeout(read_timeout, read.next()) => {
                let Ok(msg) = msg else { bail!("timed out waiting for ASR response") };
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        let (is_last, text) = parse_frame(&data)?;
                        if !text.is_empty() {
                            last_text = text.clone();
                        }
                        if is_last {
                            on_event(Event::Final(last_text));
                            return Ok(());
                        }
                        if !text.is_empty() {
                            on_event(Event::Partial(text));
                        }
                    }
                    Some(Ok(Message::Close(_))) | None if finished => {
                        on_event(Event::Final(last_text));
                        return Ok(());
                    }
                    Some(Ok(Message::Close(_))) | None => bail!("ASR socket closed early"),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => bail!("ASR socket error: {e}"),
                }
            }
        }
    }
}

fn request_payload(cfg: &Config) -> Value {
    let mut request = json!({
        "model_name": "bigmodel",
        "enable_itn": true,
        "enable_punc": true,
        "enable_ddc": false,
        "enable_nonstream": true,
        "result_type": "full",
    });
    // Hotwords and free-text context share `corpus.context`, a JSON *string*.
    let mut ctx = serde_json::Map::new();
    if !cfg.hotwords.is_empty() {
        let words: Vec<Value> = cfg.hotwords.iter().map(|w| json!({ "word": w })).collect();
        ctx.insert("hotwords".into(), Value::Array(words));
    }
    if !cfg.context.is_empty() {
        ctx.insert("context_type".into(), json!("dialog_ctx"));
        ctx.insert("context_data".into(), json!([{ "text": cfg.context }]));
    }
    if !ctx.is_empty() {
        request["corpus"] = json!({ "context": Value::Object(ctx).to_string() });
    }
    json!({
        "user": { "uid": "fcitx5-voicetype" },
        "audio": { "format": "pcm", "codec": "raw", "rate": 16000, "bits": 16, "channel": 1 },
        "request": request,
    })
}

fn frame(message_type: u8, flags: u8, serialization: u8, seq: i32, body: &[u8]) -> Result<Vec<u8>> {
    let body = gzip(body)?;
    let mut out = Vec::with_capacity(12 + body.len());
    out.extend_from_slice(&[
        0x11, // protocol v1, header size 1 (4 bytes)
        (message_type << 4) | flags,
        (serialization << 4) | COMPRESSION_GZIP,
        0,
    ]);
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn full_request(payload: &Value) -> Result<Vec<u8>> {
    frame(
        MSG_CLIENT_FULL_REQUEST,
        FLAG_POS_SEQUENCE,
        SERIALIZATION_JSON,
        1,
        payload.to_string().as_bytes(),
    )
}

/// The last packet carries a negative sequence number and flag 0b0011.
fn audio_request(seq: i32, audio: &[u8], is_last: bool) -> Result<Vec<u8>> {
    let (flags, seq) = if is_last {
        (FLAG_NEG_WITH_SEQUENCE, -seq)
    } else {
        (FLAG_POS_SEQUENCE, seq)
    };
    frame(MSG_CLIENT_AUDIO_ONLY, flags, SERIALIZATION_NONE, seq, audio)
}

/// Returns `(is_last, text)`; server error frames become `Err`.
fn parse_frame(msg: &[u8]) -> Result<(bool, String)> {
    let header = msg.first().map_or(0, |b| (b & 0x0f) as usize * 4);
    if msg.len() < header.max(4) {
        bail!("ASR frame too short");
    }
    let (message_type, flags) = (msg[1] >> 4, msg[1] & 0x0f);
    let (serialization, compression) = (msg[2] >> 4, msg[2] & 0x0f);
    let mut off = header;
    let read_u32 = |off: &mut usize| -> Result<u32> {
        let b = msg.get(*off..*off + 4).context("ASR frame truncated")?;
        *off += 4;
        Ok(u32::from_be_bytes(b.try_into().unwrap()))
    };
    let body = |off: usize, size: usize| -> Result<Vec<u8>> {
        let raw = msg
            .get(off..off + size)
            .context("ASR frame payload truncated")?;
        if compression == COMPRESSION_GZIP && !raw.is_empty() {
            let mut out = Vec::new();
            GzDecoder::new(raw)
                .read_to_end(&mut out)
                .context("gunzip failed")?;
            Ok(out)
        } else {
            Ok(raw.to_vec())
        }
    };
    match message_type {
        MSG_SERVER_FULL_RESPONSE => {
            if flags & FLAG_POS_SEQUENCE != 0 {
                read_u32(&mut off)?; // sequence
            }
            let size = read_u32(&mut off)? as usize;
            let payload = body(off, size)?;
            let text = if serialization == SERIALIZATION_JSON && !payload.is_empty() {
                extract_text(&serde_json::from_slice(&payload).context("bad ASR JSON")?)
            } else {
                String::new()
            };
            Ok((flags & FLAG_SERVER_LAST != 0, text))
        }
        MSG_SERVER_ERROR => {
            let code = read_u32(&mut off)?;
            let size = read_u32(&mut off).unwrap_or(0) as usize;
            let payload = body(off, size).unwrap_or_default();
            bail!(
                "VolcEngine ASR error {code}: {}",
                String::from_utf8_lossy(&payload)
            )
        }
        _ => Ok((false, String::new())),
    }
}

fn extract_text(payload: &Value) -> String {
    let text = payload
        .pointer("/result/text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if !text.is_empty() {
        return text.to_string();
    }
    payload
        .pointer("/result/utterances")
        .and_then(Value::as_array)
        .map(|u| {
            u.iter()
                .filter_map(|u| u["text"].as_str())
                .collect::<String>()
        })
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_plan_first_then_speech_v2_v1() {
        let cfg = Config {
            ark_api_key: "ark".into(),
            speech_api_key: "sp".into(),
            ..Default::default()
        };
        let r = cfg.routes();
        assert_eq!(r.len(), 3);
        assert_eq!(
            (r[0].url, r[0].api_key.as_str(), r[0].resource_id),
            (URL_PLAN, "ark", RESOURCE_V2)
        );
        assert_eq!((r[1].url, r[1].resource_id), (URL_SPEECH, RESOURCE_V2));
        assert_eq!((r[2].url, r[2].resource_id), (URL_SPEECH, RESOURCE_V1));
        assert!(Config::default().routes().is_empty());
    }

    #[test]
    fn payload_two_pass_hotwords_and_context() {
        let p = request_payload(&Config::default());
        assert_eq!(p["request"]["enable_nonstream"], json!(true));
        assert!(p["request"].get("corpus").is_none());

        let cfg = Config {
            hotwords: vec!["Rust".into(), "Wayland".into()],
            context: "我是程序员，中英混说".into(),
            ..Default::default()
        };
        let p = request_payload(&cfg);
        let ctx: Value =
            serde_json::from_str(p["request"]["corpus"]["context"].as_str().unwrap()).unwrap();
        assert_eq!(ctx["hotwords"][1]["word"], "Wayland");
        assert_eq!(ctx["context_data"][0]["text"], "我是程序员，中英混说");
    }

    fn server_frame(payload: &Value, is_last: bool) -> Vec<u8> {
        let flags = FLAG_POS_SEQUENCE | if is_last { FLAG_SERVER_LAST } else { 0 };
        frame(
            MSG_SERVER_FULL_RESPONSE,
            flags,
            SERIALIZATION_JSON,
            7,
            payload.to_string().as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn parses_partial_last_and_error_frames() {
        let f = server_frame(&json!({"result": {"text": "你好 world"}}), false);
        assert_eq!(parse_frame(&f).unwrap(), (false, "你好 world".into()));

        let f = server_frame(
            &json!({"result": {"text": "", "utterances": [{"text": "a "}, {"text": "b"}]}}),
            true,
        );
        assert_eq!(parse_frame(&f).unwrap(), (true, "a b".into()));

        let mut f = vec![
            0x11,
            MSG_SERVER_ERROR << 4,
            (SERIALIZATION_JSON << 4) | COMPRESSION_GZIP,
            0,
        ];
        let body = gzip(br#"{"message":"bad key"}"#).unwrap();
        f.extend_from_slice(&45000003u32.to_be_bytes());
        f.extend_from_slice(&(body.len() as u32).to_be_bytes());
        f.extend_from_slice(&body);
        let err = parse_frame(&f).unwrap_err().to_string();
        assert!(err.contains("45000003") && err.contains("bad key"));

        assert!(parse_frame(&[0x11, 0x90]).is_err());
    }

    #[test]
    fn last_audio_packet_has_negative_sequence() {
        let f = audio_request(5, &[0, 0], true).unwrap();
        assert_eq!(f[1], (MSG_CLIENT_AUDIO_ONLY << 4) | FLAG_NEG_WITH_SEQUENCE);
        assert_eq!(i32::from_be_bytes(f[4..8].try_into().unwrap()), -5);
    }

    /// Real round trip through the gateway (no microphone). Run with
    /// `VT_ARK_API_KEY=... cargo test -- --ignored live`.
    #[test]
    #[ignore]
    fn live_session_round_trip() {
        let cfg = Config {
            ark_api_key: std::env::var("VT_ARK_API_KEY").unwrap_or_default(),
            speech_api_key: std::env::var("VT_SPEECH_API_KEY").unwrap_or_default(),
            ..Default::default()
        };
        let (audio_tx, audio_rx) = mpsc::unbounded_channel();
        let (finish_tx, finish_rx) = mpsc::channel(1);
        // 1.5 s of a 440 Hz tone at 48 kHz.
        let tone: Vec<f32> = (0..72_000)
            .map(|i| (i as f32 / 48_000.0 * 2.0 * std::f32::consts::PI * 440.0).sin() * 0.3)
            .collect();
        audio_tx
            .send(crate::audio::to_pcm16k(&tone, 48_000))
            .unwrap();
        finish_tx.try_send(()).unwrap();
        let mut events = 0;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run(
            &cfg,
            audio_rx,
            finish_rx,
            || {},
            |ev| {
                events += 1;
                if let Event::Final(t) = ev {
                    eprintln!("final={t:?}");
                }
            },
        ))
        .unwrap();
        assert!(events >= 1, "expected at least the final event");
    }
}
