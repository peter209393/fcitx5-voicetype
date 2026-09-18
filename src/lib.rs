//! C ABI used by the fcitx5 addon (`fcitx5/voicetype.cpp`).
//!
//! One session = one utterance: `vt_session_start` opens the microphone and
//! the streaming recognizer, `vt_session_finish` stops the microphone and
//! asks for the final transcript, `vt_session_free` cancels/cleans up.
//! Events are delivered through the callback from a worker thread; after
//! `vt_session_free` returns no callback is ever invoked again.

mod audio;
mod volc;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub const VT_EVENT_PARTIAL: c_int = 0;
pub const VT_EVENT_FINAL: c_int = 1;
pub const VT_EVENT_ERROR: c_int = 2;

/// All strings are NUL-terminated UTF-8; NULL means empty.
#[repr(C)]
pub struct VtConfig {
    pub ark_api_key: *const c_char,
    pub speech_api_key: *const c_char,
    /// Comma/newline separated.
    pub hotwords: *const c_char,
    pub context: *const c_char,
    pub audio_device: *const c_char,
}

pub type VtCallback = unsafe extern "C" fn(user: *mut c_void, kind: c_int, text: *const c_char);

struct Callback {
    cb: VtCallback,
    user: usize,
}

pub struct VtSession {
    finish: mpsc::Sender<()>,
    callback: Arc<Mutex<Option<Callback>>>,
}

unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().trim().to_string()
    }
}

/// # Safety
/// `cfg` must point to a valid `VtConfig`; `cb` must stay callable until
/// `vt_session_free` returns.
#[no_mangle]
pub unsafe extern "C" fn vt_session_start(
    cfg: *const VtConfig,
    cb: VtCallback,
    user: *mut c_void,
) -> *mut VtSession {
    let c = &*cfg;
    let cfg = volc::Config {
        ark_api_key: cstr(c.ark_api_key),
        speech_api_key: cstr(c.speech_api_key),
        hotwords: cstr(c.hotwords)
            .split([',', '，', '\n'])
            .map(str::trim)
            .filter(|w| !w.is_empty())
            .map(str::to_string)
            .collect(),
        context: cstr(c.context),
        audio_device: cstr(c.audio_device),
    };
    let (finish_tx, finish_rx) = mpsc::channel(1);
    let callback = Arc::new(Mutex::new(Some(Callback {
        cb,
        user: user as usize,
    })));
    let cb_worker = Arc::clone(&callback);
    std::thread::Builder::new()
        .name("voicetype".into())
        .spawn(move || {
            let emit = |kind: c_int, text: &str| {
                if let Some(c) = cb_worker.lock().unwrap().as_ref() {
                    let text = CString::new(text.replace('\0', " ")).unwrap();
                    unsafe { (c.cb)(c.user as *mut c_void, kind, text.as_ptr()) };
                }
            };
            let body = || -> anyhow::Result<()> {
                let (audio_tx, audio_rx) = mpsc::unbounded_channel();
                let stream = audio::start(&cfg.audio_device, audio_tx)?;
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(volc::run(
                    &cfg,
                    audio_rx,
                    finish_rx,
                    move || drop(stream),
                    |ev| match ev {
                        volc::Event::Partial(t) => emit(VT_EVENT_PARTIAL, &t),
                        volc::Event::Final(t) => emit(VT_EVENT_FINAL, &t),
                    },
                ))
            };
            // A panic must not leave the addon waiting forever: report it.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => emit(VT_EVENT_ERROR, &format!("{e:#}")),
                Err(p) => {
                    let msg = p
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| p.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".into());
                    emit(VT_EVENT_ERROR, &format!("internal error: {msg}"));
                }
            }
        })
        .expect("failed to spawn voicetype worker");
    Box::into_raw(Box::new(VtSession {
        finish: finish_tx,
        callback,
    }))
}

/// Stops recording and requests the final transcript (delivered via callback).
///
/// # Safety
/// `s` must come from `vt_session_start` and not yet be freed.
#[no_mangle]
pub unsafe extern "C" fn vt_session_finish(s: *mut VtSession) {
    if let Some(s) = s.as_ref() {
        let _ = s.finish.try_send(());
    }
}

/// Cancels the session if still running and releases it. Blocks until any
/// in-flight callback has returned; no callback fires afterwards.
///
/// # Safety
/// `s` must come from `vt_session_start` and not yet be freed.
#[no_mangle]
pub unsafe extern "C" fn vt_session_free(s: *mut VtSession) {
    if s.is_null() {
        return;
    }
    let s = Box::from_raw(s);
    s.callback.lock().unwrap().take();
    // Dropping `finish` closes the channel, which cancels the worker.
}
