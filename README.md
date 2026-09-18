# fcitx5-voicetype

> 🎙️ **Hold a key, speak, and the text appears at your cursor** — an [fcitx5](https://fcitx-im.org/) addon that streams your voice to 豆包流式语音识别 2.0 (VolcEngine Seed-ASR) and types the result live.

No daemon, no tray icon, no fake keyboard: it is a regular fcitx5 module. Partial results show up as **preedit** while you talk; the final transcript is **committed** when you release the key. Works wherever fcitx5 works (Wayland, X11, any toolkit) and alongside your normal input method (pinyin, keyboard, …).

[![CI](https://github.com/YouNeedWork/fcitx5-voicetype/actions/workflows/ci.yml/badge.svg)](https://github.com/YouNeedWork/fcitx5-voicetype/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Platform](https://img.shields.io/badge/platform-Linux%20%2F%20fcitx5%20%E2%89%A5%205.1-informational)

## Features

- **Push-to-talk**: hold a key (default right Alt), speak, release. Nothing to click, no extra process.
- **Live**: partial results are shown as preedit while you speak and corrected in place; the final two-pass result is committed on release.
- **Accurate on 中英混说**: 豆包流式语音识别 2.0 with punctuation, ITN, hotwords and a free-text context hint.
- **Plays nice with fcitx5**: it is a module, not an engine — pinyin or any other input method stays active; configured from `fcitx5-configtool`.
- **Small**: ~300 lines of C++ and ~700 lines of Rust, one shared object, no runtime dependencies beyond fcitx5 and ALSA.

## How it works

```
hold Alt_R ──► fcitx5 module (C++) ──► Rust core: microphone ──► 豆包 ASR (websocket)
                     ▲                                                   │
                     └──── partial text → preedit, final text → commit ◄─┘
```

- **C++ module** (`fcitx5/voicetype.cpp`, ~300 lines with the header): watches the push-to-talk key, shows partials as preedit, commits the final text, exposes the settings to `fcitx5-configtool`.
- **Rust core** (`src/`, ~700 lines, static library): captures the microphone with `cpal`, resamples to 16 kHz, and speaks the VolcEngine v3 streaming protocol (two-pass recognition, punctuation, ITN, hotwords, context).

## Install

Requirements: `fcitx5` (≥ 5.1) with development headers, `extra-cmake-modules`, `cmake`, `ninja`, `alsa-lib`, a Rust toolchain.

```sh
# Arch
sudo pacman -S --needed fcitx5 extra-cmake-modules cmake ninja alsa-lib rust
# Debian/Ubuntu
sudo apt install libfcitx5core-dev extra-cmake-modules cmake ninja-build libasound2-dev cargo

cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr
cmake --build build
sudo cmake --install build --strip
fcitx5 -r   # restart fcitx5 to load the addon
```

To remove it: `sudo rm /usr/lib/fcitx5/libvoicetype.so /usr/share/fcitx5/addon/voicetype.conf` and restart fcitx5.

## Configure

You need one API key: either a 火山方舟 **Agent Plan** key (billed against the plan, model 2.0) or a 豆包语音 console key from [console.volcengine.com/speech](https://console.volcengine.com/speech/app) with 流式语音识别 enabled for your app.

Open **fcitx5-configtool → Addons → Voice Type**, or edit `~/.config/fcitx5/conf/voicetype.conf`:

```ini
# 火山方舟 Agent Plan key: serves 豆包 ASR 2.0 through the plan gateway.
ArkApiKey=ark-...
# Or a 豆包语音 console key (2.0 if enabled for the app, else 1.0).
SpeechApiKey=
# Proper nouns / jargon, comma separated. Free-text context for the recognizer.
Hotwords=Rust, Wayland, fcitx5
Context=我是程序员，中英混说
# Substring of the input device name; empty = "pipewire" if present, else default.
AudioDevice=

# Push-to-talk keys (hold to speak). Default: right Alt. Key lists are sections.
[Hotkey]
0=Alt_R
```

Either key works; when both are set the plan gateway is tried first, then the console with model 2.0, then 1.0. Config changes apply on the next utterance (no restart needed when saved from `fcitx5-configtool`).

## Use

1. Focus any text field.
2. **Hold** the hotkey and speak. A 🎙 shows next to the cursor; recognized text appears underlined as you talk.
3. **Release** the key. The final (two-pass corrected) transcript replaces the preedit and is committed.

Errors (no key, no microphone, network) are shown for a few seconds next to the cursor and logged to fcitx5's stderr.

## Development

```sh
cargo test                                # protocol + resampling unit tests
cargo clippy --all-targets -- -D warnings
VT_ARK_API_KEY=ark-... cargo test -- --ignored live   # real gateway round trip, no microphone
cmake --build build                       # rebuilds the Rust core when src/ changes
python3 scripts/ictest.py 4               # end-to-end through fcitx5's DBus frontend (speak!)
```

Layout: `src/` Rust core (`lib.rs` C ABI, `audio.rs` capture, `volc.rs` protocol), `fcitx5/` the addon, `CMakeLists.txt` builds both.

## Troubleshooting

- **Nothing happens when holding the key.** Run `python3 scripts/ictest.py 4` while speaking: it drives the addon through fcitx5's DBus frontend and prints what fcitx5 sends back (`UpdateFormattedPreedit` partials, `CommitString` final). If it prints `press handled = False`, the hotkey did not match: the `[Hotkey]` section must be a list (`0=Alt_R`), not `Hotkey=Alt_R`.
- **Check the log.** Start fcitx5 in a terminal (`fcitx5 -r`) and look for `voicetype: hotkeys …` at startup (config loaded) and `voicetype: session started` / `final …` per utterance. Errors from the recognizer or microphone are logged with `voicetype:` too.
- **After upgrading fcitx5** rebuild and reinstall the addon, then restart fcitx5 — a running fcitx5 whose libraries were replaced underneath tends to crash on the next restart.

## License

MIT
