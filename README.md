# Voice Type

> 🎙️ **Hold-to-talk voice typing for Linux & macOS** — hold a key, speak, and the text appears **live at your cursor** as you talk. Real-time streaming speech-to-text in a tiny Rust tray daemon.

No dictation windows, no clicking — Voice Type turns any focused input box (browser, chat, editor, terminal) into a voice input box. Works on **Wayland** (sway, Hyprland, GNOME, KDE), **X11**, and **macOS**.

[![CI](https://github.com/peter209393/voice-type/actions/workflows/ci.yml/badge.svg)](https://github.com/peter209393/voice-type/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](#license)
![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-informational)
![Rust](https://img.shields.io/badge/rust-%F0%9F%A6%80-orange)
![ASR](https://img.shields.io/badge/ASR-VolcEngine%20streaming%20%7C%20faster--whisper%20local-green)

## Features

- 🗣️ **Real-time voice typing** — streaming ASR partials are typed live while you speak; no waiting for release
- ⌨️ **Push-to-talk hotkey** — hold **Alt** / **Option** to record, release to finish
- 🔁 **Self-correcting** — when the recognizer rewrites earlier words, the divergence is backspaced and retyped
- ☁️ **Cloud ASR by default** — 豆包流式语音识别 2.0 (Seed-ASR) via VolcEngine, two-pass recognition, punctuation & ITN; accurate on Chinese/English mixed speech without any LLM post-processing
- 🏠 **Offline fallback** — local [faster-whisper](https://github.com/SYSTRAN/faster-whisper) server when the cloud is unavailable
- 🎨 **State-aware tray icon** — procedurally rendered, independent of the icon theme
- 🖥️ **Cross-platform, single binary** — Linux (Wayland/X11) & macOS, no Electron

## Install

### One-click via AI agent

Paste [`agent-install.md`](agent-install.md) into your coding agent (pi, Claude Code, Cursor, …) on the target machine — it handles prerequisites, build, API key, autostart and verification:

```text
Install and set up Voice Type (https://github.com/peter209393/voice-type) …
→ full prompt: agent-install.md in the repo root
```

### Manual

```bash
sudo usermod -a -G input $USER && re-login   # Linux: hotkey access
sudo pacman -S wtype                         # or: sudo apt install wtype

git clone https://github.com/peter209393/voice-type.git
cd voice-type && cargo build --release
export VT_VOLC_API_KEY="your-key"            # or see Configuration
./target/release/voice-type
```

Get a VolcEngine key at <https://console.volcengine.com/speech/new/setting/apikeys?projectName=default>, or go fully offline with the [faster-whisper server](#local-offline-fallback-faster-whisper).

> **Get 豆包流式语音识别模型 2.0** — it is markedly better at English and Chinese/English code-switching. Two ways: (a) enable it for the key's project in the speech console, or (b) subscribe to a 火山方舟 Agent/Coding Plan, turn on 超额后付费 for the speech models, and set the plan's API key as `VT_ARK_API_KEY` (or `VT_ARK_PLAN_API_KEY`) — no speech-console key needed at all. Voice Type tries the plan gateway first, then the speech console with 2.0, then 1.0; the startup line `[vt] VolcEngine ASR: model 2.0 (seedasr) via ark-plan …` confirms which route is active.

## Usage

Focus any input box, **hold Alt and speak** — text appears live at the cursor (Chinese/English mixed input works well). Release to finalize. The tray icon shows: idle → recording → transcribing → done/error.

## How It Works

Audio is streamed to the ASR engine in 100 ms packets while you hold the hotkey. Each partial transcript is diffed against what is already on screen; only the delta is typed (with backspaces on rewrites). On release, the final transcript completes the utterance.

<details>
<summary><b>Hotkey remapping (Linux)</b> — why Alt becomes F13</summary>

The hotkey is a **modifier** (Alt). Wayland compositors merge modifier state across all keyboards on the seat, so while Alt is held, synthetic keys from `wtype` reach apps as `Alt+<key>` — treated as shortcuts and dropped, which broke live typing. On startup Voice Type remaps the hotkey's scancode to `F13` via `EVIOCSKEYCODE_V2` (no root; `input` group suffices): F13 is a non-modifier with an inert keysym that apps and terminals ignore entirely. The original mapping is restored on exit, including `SIGINT`/`SIGTERM`.

While running: the hotkey key temporarily loses its Alt/AltGr function (restored on exit). After a `kill -9`, restore by terminating the app gracefully on next run, replugging the keyboard, or rebooting. Devices whose driver refuses the remap fall back to the original keycode.
</details>

<details>
<summary><b>Local offline fallback (faster-whisper)</b></summary>

Run an isolated Python 3.12 server via [`uv`](https://docs.astral.sh/uv/), then set `VT_ASR_PROVIDER=whisper`:

```bash
uv run --python 3.12 \
    --with fastapi --with "uvicorn[standard]" \
    --with "faster-whisper" --with python-multipart \
    uvicorn server.asr_server:app --host 127.0.0.1 --port 8000
```

First run downloads the model (~1.5 GB for `medium`, cached in `~/.cache/huggingface`). Use a systemd user unit to keep it running (see Autostart).
</details>

| Platform | Hotkey | Typing | Tray |
|----------|--------|--------|------|
| Linux (Wayland) | Alt | wtype, ydotool | ksni (waybar etc.) |
| Linux (X11) | Alt | xdotool, ydotool | ksni / GTK (`gtk-tray` feature) |
| macOS | Option | enigo | NSStatusItem |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VT_ASR_PROVIDER` | `auto` | `auto` (VolcEngine when keyed, else whisper), `volc`, `whisper` |
| `VT_VOLC_API_KEY` | unset | VolcEngine key via env var — takes precedence over any config file |
| `VT_VOLC_PLAN` | auto | Route ASR through the 火山方舟 Agent/Coding Plan gateway (`…/api/v3/plan/sauc/`), billed to the plan (超额后付费). Auto = try it first whenever an Ark key is set; `0` never, `1` only |
| `VT_ARK_PLAN_API_KEY` / `VT_ARK_API_KEY` | unset | 火山方舟 (Ark) plan API key for the plan gateway; the first one set wins |
| `VT_VOLC_WS_BASE` | `wss://openspeech.bytedance.com/api/v3/sauc/` | Override the speech-console gateway base URL |
| `VT_VOLC_RESOURCE_ID` | auto | `X-Api-Resource-Id`. Auto tries 豆包流式语音识别模型 **2.0** (`volc.seedasr.sauc.duration`, best for Chinese/English mixing) and falls back to 1.0 (`volc.bigasr.sauc.duration`) if the key is not enabled for it |
| `VT_VOLC_MODE` | `async` | `async` = `bigmodel_async` (optimized, supports two-pass) · `stream` = legacy `bigmodel` |
| `VT_VOLC_TWO_PASS` | `1` | Two-pass recognition: live partials from the streaming model, each finished sentence re-recognized by the non-streaming model (more accurate final text). `0` to disable |
| `VT_VOLC_END_WINDOW_MS` | server default (800) | Silence that ends a sentence and triggers its two-pass re-recognition; min 200. Lower = earlier fixes, more splits |
| `VT_VOLC_HOTWORDS` | unset | Comma-separated hotwords sent inline (names, products, jargon; ≈100 tokens max), e.g. `Rust,Wayland,sway,tokio` |
| `VT_VOLC_CONTEXT` | unset | Free-text context for the recognizer, e.g. `我是程序员，中英混说，常用术语是 Rust、Linux、Wayland` |
| `VT_VOLC_BOOSTING_TABLE_ID` | unset | VolcEngine hotwords table id (自学习平台) |
| `VT_VOLC_CONFIG` | `~/.pi/agent/voice-input.config.json` | Shared pi-voice-input config file |
| `VT_ASR_URL` | `http://127.0.0.1:8000` | faster-whisper server URL |
| `VT_ASR_MODEL` | `medium` | faster-whisper model (`tiny`…`large-v3`, `large-v3-turbo` recommended for zh/en mixing) |
| `VT_ASR_DEVICE` / `VT_ASR_COMPUTE_TYPE` | `cpu` / `int8` | CTranslate2 device / compute type |
| `VT_ASR_BEAM` / `VT_ASR_LANGUAGE` / `VT_ASR_PROMPT` | `5` / auto / bilingual sample | faster-whisper beam size, forced language (`zh`/`en`), initial prompt that biases code-switching |
| `VT_LOG` | unset | Any non-empty value = verbose debug logging |

## Autostart

**sway** — `~/.config/sway/config`:

```
exec VT_VOLC_API_KEY=your-key ~/.local/bin/voice-type &
```

**systemd** — `~/.config/systemd/user/voice-type.service`:

```ini
[Unit]
Description=Voice Type — hold-to-talk voice typing
After=graphical-session.target
[Service]
Environment=VT_VOLC_API_KEY=your-key
ExecStart=%h/.local/bin/voice-type
Restart=on-failure
[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now voice-type
```

**macOS** — add to Login Items (System Settings → General).

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "No keyboard device found" | Add yourself to the `input` group and re-login |
| "No typing tool found" | Install `wtype`, `ydotool`, or `xdotool` |
| ASR errors / connection refused | Start the faster-whisper server; check `VT_ASR_URL` |
| Live typing garbles with an IME | Switch the IME to direct/English mode while dictating |
| Right Alt doesn't work as Alt/AltGr | Expected while running (remapped to F13); restored on exit |
| Key still remapped after a crash | Graceful-kill the app on next run, replug keyboard, or reboot |
| Live partials missing | Check the VolcEngine key; debug with `VT_LOG=1` |
| English / mixed speech inaccurate | Get ASR model 2.0 (plan or console, see Install; look for `model 1.0` in the startup log), add `VT_VOLC_HOTWORDS` / `VT_VOLC_CONTEXT`, keep `VT_VOLC_TWO_PASS=1` |
| `45000010 … call ark get status code:401` | The plan gateway accepted the connection but 方舟 rejected the key: use the **plan** API key (`VT_ARK_PLAN_API_KEY`) and make sure 超额后付费 is on for the speech models |
| No tray icon | Bar must support StatusNotifierItem (waybar etc.) |
| macOS: typing doesn't work | Grant Accessibility permissions |

## License

MIT
