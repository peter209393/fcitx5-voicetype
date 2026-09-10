// The objc crate's macros emit `cfg(cargo-clippy)` checks that newer rustc
// flags as unexpected (macOS only); harmless everywhere.
#![allow(unexpected_cfgs)]

mod asr;
mod audio;
mod hotkey;
mod tray;
mod typewriter;
mod volc;

use anyhow::{Context, Result};
use asr::StreamTranscriber;
use crossbeam_channel::{bounded, Select, SelectTimeoutError, Sender};
use hotkey::HotkeyEvent;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use volc::{VolcEvent, VolcSession};

const MIN_CHUNK_SAMPLES: usize = 8000;
const VOLC_FINAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Debug logging gated by `VT_LOG=1` (any non-empty value).
macro_rules! vlog {
    ($($arg:tt)*) => {
        if std::env::var_os("VT_LOG").is_some_and(|v| !v.is_empty()) {
            eprintln!("[vt] {}", format!($($arg)*));
        }
    };
}

#[derive(Clone, Debug)]
pub enum UiState {
    Idle,
    Recording { started_at: Instant },
    Transcribing { started_at: Instant },
    Done { text: String },
    Error { msg: String },
}

enum AsrProvider {
    Auto,
    Volc,
    Whisper,
}

fn asr_provider() -> AsrProvider {
    match std::env::var("VT_ASR_PROVIDER")
        .unwrap_or_default()
        .to_lowercase()
        .as_str()
    {
        "volc" | "volcengine" | "pi-voice-input" => AsrProvider::Volc,
        "whisper" | "local" => AsrProvider::Whisper,
        _ => AsrProvider::Auto,
    }
}

/// VolcEngine streaming ASR (the pi-voice-input backend) is the default
/// whenever its API key is configured.
fn use_volc() -> bool {
    match asr_provider() {
        AsrProvider::Volc => true,
        AsrProvider::Whisper => false,
        AsrProvider::Auto => volc::available(),
    }
}

/// What must be typed to move the on-screen text from `typed` to `target`.
#[derive(Debug, PartialEq, Eq)]
enum TypingPlan {
    /// Nothing to do (identical).
    Noop,
    /// The new text only appended: type this delta at the cursor.
    Append(String),
    /// The ASR rewrote earlier text: `backspaces` chars to delete, then this
    /// text to type (from the longest common prefix).
    Replace { backspaces: usize, retype: String },
}

/// Computes the minimal typing action between two streaming results.
/// Pure function (unit-testable; the caller performs the actual typing).
fn typing_plan(typed: &str, target: &str) -> TypingPlan {
    if typed == target {
        return TypingPlan::Noop;
    }
    if let Some(delta) = target.strip_prefix(typed) {
        return TypingPlan::Append(delta.to_string());
    }
    let common = typed
        .chars()
        .zip(target.chars())
        .take_while(|(a, b)| a == b)
        .count();
    TypingPlan::Replace {
        backspaces: typed.chars().count() - common,
        retype: target.chars().skip(common).collect(),
    }
}

/// Tracks what has already been typed at the cursor for the current
/// utterance so streaming updates only need to type the delta (and can
/// backspace over a divergence if the ASR rewrites earlier text).
#[derive(Default)]
struct TypedState {
    text: String,
}

impl TypedState {
    fn reset(&mut self) {
        self.text.clear();
    }

    /// Types the difference between what is already on screen and `target`,
    /// updating the tracked state. Returns the final on-screen text.
    fn type_towards(&mut self, target: &str) -> String {
        let target = target.trim();
        vlog!("type_towards: current={:?} target={:?}", self.text, target);
        if target.is_empty() {
            return self.text.clone();
        }

        match typing_plan(&self.text, target) {
            TypingPlan::Noop => {}
            TypingPlan::Append(delta) => {
                if let Err(e) = typewriter::type_text_auto(&delta) {
                    eprintln!("[vt] failed to type partial: {e:#}");
                    return self.text.clone();
                }
            }
            TypingPlan::Replace { backspaces, retype } => {
                if backspaces > 0 {
                    if let Err(e) = typewriter::backspace(backspaces) {
                        eprintln!("[vt] backspace failed: {e:#}");
                        return self.text.clone();
                    }
                }
                if let Err(e) = typewriter::type_text_auto(&retype) {
                    eprintln!("[vt] failed to retype partial: {e:#}");
                }
            }
        }
        self.text = target.to_string();
        self.text.clone()
    }
}

/// Resolve a finished streaming utterance: type the final text (or keep the
/// partials already typed), and on failure fall back to local whisper ASR
/// using the buffered recording.
#[allow(clippy::too_many_arguments)] // event-loop plumbing, flat args are clearest here
fn resolve_volc(
    res: Result<String, String>,
    volc_session: &mut Option<VolcSession>,
    volc_pending_since: &mut Option<Instant>,
    typed: &mut TypedState,
    buffer: &mut Vec<f32>,
    sample_rate: u32,
    transcriber: &Arc<StreamTranscriber>,
    cmd_tx: &Sender<tray::TrayCmd>,
) {
    *volc_session = None;
    *volc_pending_since = None;

    match res {
        Ok(text) if !text.trim().is_empty() => {
            let final_text = typed.type_towards(&text);
            let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Done {
                text: final_text.clone(),
            }));
        }
        Ok(_) => {
            // Empty final result: keep whatever partials were already typed
            // as the utterance result instead of discarding them.
            if typed.text.is_empty() {
                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Idle));
            } else {
                let text = typed.text.clone();
                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Done {
                    text: text.clone(),
                }));
            }
        }
        Err(msg) => {
            // Streaming failed: retry once with the local whisper server
            // using the buffered recording. Any partials already typed stay
            // on screen; whisper's result is typed after them.
            eprintln!("[vt] volc streaming failed: {msg}");
            if buffer.len() >= MIN_CHUNK_SAMPLES {
                eprintln!("[vt] falling back to local whisper ASR");
                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Transcribing {
                    started_at: Instant::now(),
                }));
                let samples = std::mem::take(buffer);
                let sr = sample_rate;
                let transcriber = Arc::clone(transcriber);
                let cmd_tx_clone = cmd_tx.clone();
                std::thread::spawn(move || match transcriber.transcribe_chunk(&samples, sr) {
                    Ok(text) if !text.trim().is_empty() => {
                        let trimmed = text.trim().to_string();
                        if let Err(e) = typewriter::type_text_auto(&trimmed) {
                            eprintln!("[vt] failed to type transcript: {e:#}");
                        }
                        let _ = cmd_tx_clone.send(tray::TrayCmd::UpdateState(UiState::Done {
                            text: trimmed.clone(),
                        }));
                    }
                    _ => {
                        let _ = cmd_tx_clone.send(tray::TrayCmd::UpdateState(UiState::Idle));
                    }
                });
            } else {
                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Error { msg }));
            }
        }
    }

    buffer.clear();
}

fn main() -> Result<()> {
    let dummy_path = PathBuf::from("/dev/null");
    let transcriber = Arc::new(StreamTranscriber::new("", &dummy_path));

    let (audio_tx, audio_rx) = bounded::<Vec<f32>>(256);
    audio::add_sender(audio_tx);

    let running = Arc::new(AtomicBool::new(true));

    let hotkey_rx = hotkey::start_hotkey_listener().context("Failed to start hotkey listener.")?;

    let (cmd_tx, cmd_rx) = bounded::<tray::TrayCmd>(16);

    let tray_running = running.clone();
    std::thread::spawn(move || {
        if let Err(e) = tray::run_tray(tray_running, cmd_rx) {
            eprintln!("Tray error: {:#}", e);
        }
    });

    let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Idle));

    let mut buffer: Vec<f32> = Vec::new();
    let mut sample_rate: u32 = 44_100;
    let mut audio_engine: Option<audio::AudioEngine> = None;
    let mut is_recording = false;

    // VolcEngine streaming session for the current utterance.
    let mut volc_session: Option<VolcSession> = None;
    let mut volc_pending_since: Option<Instant> = None;
    let mut typed = TypedState::default();

    loop {
        if !running.load(Ordering::SeqCst) {
            break;
        }

        // The volc events receiver is part of the event loop itself so
        // streaming partials are typed live while recording. (A plain
        // `select!` with a `default(50ms)` tick starves: audio chunks arrive
        // every ~20ms and the tick never fires during recording, so partials
        // would only be drained after release.) The receiver is cloned out
        // of the session because `Select` ops borrow the receiver for as
        // long as the `Select` lives.
        let volc_events = volc_session.as_ref().map(|s| s.events.clone());
        let mut sel = Select::new();
        let op_hotkey = sel.recv(&hotkey_rx);
        let op_audio = sel.recv(&audio_rx);
        let op_volc = volc_events.as_ref().map(|rx| sel.recv(rx));

        let selected = match sel.select_timeout(Duration::from_millis(50)) {
            Ok(op) => op,
            Err(SelectTimeoutError) => {
                vlog!(
                    "tick: recording={} volc={} buffer={}",
                    is_recording,
                    volc_session.is_some(),
                    buffer.len()
                );
                if !is_recording {
                    while audio_rx.try_recv().is_ok() {}
                }
                // Final-result timeout after the hotkey was released.
                if let (Some(_), Some(since)) = (volc_session.as_ref(), volc_pending_since) {
                    if since.elapsed() > VOLC_FINAL_TIMEOUT {
                        resolve_volc(
                            Err("volc streaming timed out waiting for the final transcript"
                                .to_string()),
                            &mut volc_session,
                            &mut volc_pending_since,
                            &mut typed,
                            &mut buffer,
                            sample_rate,
                            &transcriber,
                            &cmd_tx,
                        );
                    }
                }
                continue;
            }
        };

        let idx = selected.index();
        if idx == op_hotkey {
            match selected.recv(&hotkey_rx) {
                Ok(HotkeyEvent::Pressed) => {
                    vlog!("hotkey pressed (was_recording={})", is_recording);
                    if !is_recording {
                        buffer.clear();
                        typed.reset();
                        volc_pending_since = None;
                        let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Recording {
                            started_at: Instant::now(),
                        }));

                        // Default provider: VolcEngine streaming (pi-voice-input
                        // backend) when configured; falls back to local whisper.
                        volc_session = if use_volc() {
                            match VolcSession::start() {
                                Ok(session) => Some(session),
                                Err(e) => {
                                    eprintln!(
                                        "[vt] volc streaming unavailable, using local whisper: {e:#}"
                                    );
                                    None
                                }
                            }
                        } else {
                            None
                        };

                        match audio::AudioEngine::start_default_input(None) {
                            Ok(engine) => {
                                sample_rate = engine.sample_rate();
                                vlog!(
                                    "recording started: sr={} volc={}",
                                    sample_rate,
                                    volc_session.is_some()
                                );
                                audio_engine = Some(engine);
                                is_recording = true;
                            }
                            Err(e) => {
                                eprintln!("Failed to start recording: {:#}", e);
                                volc_session = None;
                                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Error {
                                    msg: e.to_string(),
                                }));
                            }
                        }
                    }
                }
                Ok(HotkeyEvent::Released) => {
                    vlog!("hotkey released (was_recording={})", is_recording);
                    if is_recording {
                        if let Some(engine) = audio_engine.take() {
                            engine.stop();
                        }

                        while let Ok(chunk) = audio_rx.try_recv() {
                            buffer.extend_from_slice(&chunk);
                            if let Some(session) = volc_session.as_ref() {
                                session.send_audio(&chunk, sample_rate);
                            }
                        }

                        is_recording = false;

                        if let Some(session) = volc_session.as_ref() {
                            vlog!(
                                "release: streaming path, buffer={} samples, waiting for final",
                                buffer.len()
                            );
                            // Streaming path: partials were already typed
                            // live; flush the last packet and wait for the
                            // final result (handled by the volc-event arm).
                            let _ =
                                cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Transcribing {
                                    started_at: Instant::now(),
                                }));
                            session.finish();
                            volc_pending_since = Some(Instant::now());
                        } else {
                            vlog!("release: whisper path, buffer={} samples", buffer.len());
                            // Local whisper path: transcribe the whole
                            // recording after release, then type it.
                            if buffer.len() >= MIN_CHUNK_SAMPLES {
                                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(
                                    UiState::Transcribing {
                                        started_at: Instant::now(),
                                    },
                                ));
                                let samples = buffer.clone();
                                let sr = sample_rate;
                                let transcriber = Arc::clone(&transcriber);
                                let cmd_tx_clone = cmd_tx.clone();
                                std::thread::spawn(move || {
                                    match transcriber.transcribe_chunk(&samples, sr) {
                                        Ok(text) if !text.trim().is_empty() => {
                                            let trimmed = text.trim().to_string();
                                            if let Err(e) = typewriter::type_text_auto(&trimmed) {
                                                eprintln!("[vt] failed to type transcript: {e:#}");
                                                let _ =
                                                    cmd_tx_clone.send(tray::TrayCmd::UpdateState(
                                                        UiState::Error { msg: e.to_string() },
                                                    ));
                                                return;
                                            }
                                            let _ = cmd_tx_clone.send(tray::TrayCmd::UpdateState(
                                                UiState::Done {
                                                    text: trimmed.clone(),
                                                },
                                            ));
                                        }
                                        Ok(_) => {
                                            let _ = cmd_tx_clone
                                                .send(tray::TrayCmd::UpdateState(UiState::Idle));
                                        }
                                        Err(e) => {
                                            eprintln!("Transcription error: {:#}", e);
                                            let _ = cmd_tx_clone.send(tray::TrayCmd::UpdateState(
                                                UiState::Error { msg: e.to_string() },
                                            ));
                                        }
                                    }
                                });
                            } else {
                                let _ = cmd_tx.send(tray::TrayCmd::UpdateState(UiState::Idle));
                            }

                            buffer.clear();
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        } else if idx == op_audio {
            if let Ok(chunk) = selected.recv(&audio_rx) {
                if is_recording {
                    static AUDIO_N: std::sync::atomic::AtomicUsize =
                        std::sync::atomic::AtomicUsize::new(0);
                    let n = AUDIO_N.fetch_add(1, Ordering::Relaxed);
                    if n.is_multiple_of(50) {
                        vlog!("audio chunk #{} len={}", n, chunk.len());
                    }
                    buffer.extend_from_slice(&chunk);
                    if let Some(session) = volc_session.as_ref() {
                        session.send_audio(&chunk, sample_rate);
                    }
                }
            }
        } else if op_volc == Some(idx) {
            // Receive the event first so the immutable borrow of the session
            // ends before the arms below take `&mut volc_session`.
            let ev = volc_events.as_ref().map(|rx| selected.recv(rx));
            if let Some(ev) = ev {
                match ev {
                    Ok(VolcEvent::Ready) => vlog!("volc: ready"),
                    Ok(VolcEvent::Partial(text)) => {
                        // Live preview: type the delta at the cursor while
                        // still recording. The hotkey is remapped to a
                        // non-modifier keycode at startup (see hotkey module),
                        // so the synthetic text is never polluted by a held
                        // modifier.
                        vlog!("volc: partial {:?}", text);
                        if !text.is_empty() {
                            typed.type_towards(&text);
                        }
                    }
                    Ok(VolcEvent::Final(text)) => {
                        vlog!("volc: final {:?}", text);
                        resolve_volc(
                            Ok(text),
                            &mut volc_session,
                            &mut volc_pending_since,
                            &mut typed,
                            &mut buffer,
                            sample_rate,
                            &transcriber,
                            &cmd_tx,
                        );
                    }
                    Ok(VolcEvent::Failed(msg)) => {
                        vlog!("volc: failed {:?}", msg);
                        if is_recording {
                            // Failed mid-recording: drop the session but keep
                            // recording; the release handler will transcribe
                            // the full buffer with local whisper.
                            volc_session = None;
                            volc_pending_since = None;
                        } else {
                            resolve_volc(
                                Err(msg),
                                &mut volc_session,
                                &mut volc_pending_since,
                                &mut typed,
                                &mut buffer,
                                sample_rate,
                                &transcriber,
                                &cmd_tx,
                            );
                        }
                    }
                    Err(_) => {
                        vlog!("volc: events channel disconnected (worker gone)");
                        // Events channel disconnected (worker gone): if we were
                        // waiting for a final result, resolve with what was typed.
                        if volc_pending_since.is_some() {
                            resolve_volc(
                                Ok(typed.text.clone()),
                                &mut volc_session,
                                &mut volc_pending_since,
                                &mut typed,
                                &mut buffer,
                                sample_rate,
                                &transcriber,
                                &cmd_tx,
                            );
                        }
                    }
                }
            }
        }
    }

    let _ = cmd_tx.send(tray::TrayCmd::Quit);
    running.store(false, Ordering::SeqCst);

    // Restore the remapped hotkey scancode so the physical key behaves
    // normally again once we exit.
    hotkey::shutdown();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_noop_and_append() {
        assert_eq!(typing_plan("你好", "你好"), TypingPlan::Noop);
        assert_eq!(
            typing_plan("你好", "你好世界 hello"),
            TypingPlan::Append("世界 hello".to_string())
        );
        // Empty start appends everything.
        assert_eq!(typing_plan("", "hi"), TypingPlan::Append("hi".to_string()));
    }

    #[test]
    fn plan_replace_on_divergence() {
        // ASR rewrote the last word (homophone): backspace 2 chars, retype.
        assert_eq!(
            typing_plan("你好时间", "你好世界"),
            TypingPlan::Replace {
                backspaces: 2,
                retype: "世界".to_string()
            }
        );
        // Complete rewrite keeps the common prefix only.
        assert_eq!(
            typing_plan("abc", "abd"),
            TypingPlan::Replace {
                backspaces: 1,
                retype: "d".to_string()
            }
        );
        // No common prefix at all.
        assert_eq!(
            typing_plan("xyz", "abc"),
            TypingPlan::Replace {
                backspaces: 3,
                retype: "abc".to_string()
            }
        );
    }

    #[test]
    fn plan_handles_multibyte_boundaries() {
        // Byte-prefix but char-divergent strings must not panic.
        let plan = typing_plan("é", "e");
        assert!(matches!(plan, TypingPlan::Replace { .. }));
    }
}
