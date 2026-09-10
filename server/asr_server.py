"""Local ASR HTTP server backed by faster-whisper (CTranslate2).

Exposes an OpenAI-compatible endpoint so the Rust client can POST a WAV
multipart `file` and get back `{"text": "..."}`.

Run with uv (isolated Python 3.12, avoids system Python 3.14 wheel gaps):

    uv run --python 3.12 \
        --with fastapi --with "uvicorn[standard]" \
        --with "faster-whisper" --with python-multipart \
        uvicorn asr_server:app --host 127.0.0.1 --port 8000

Tuned for CPU-only machines: device="cpu", compute_type="int8".
"""

from __future__ import annotations

import io
import os
from typing import Optional

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse

from faster_whisper import WhisperModel

ASR_MODEL = os.environ.get("VT_ASR_MODEL", "medium")
ASR_DEVICE = os.environ.get("VT_ASR_DEVICE", "cpu")
ASR_COMPUTE_TYPE = os.environ.get("VT_ASR_COMPUTE_TYPE", "int8")
# Decoding knobs. Whisper picks ONE language per 30 s window, so mixed
# Chinese/English speech often loses the English half; a bilingual
# initial prompt and a wider beam noticeably help code-switching.
ASR_BEAM = int(os.environ.get("VT_ASR_BEAM", "5"))
ASR_LANGUAGE = os.environ.get("VT_ASR_LANGUAGE") or None  # e.g. "zh", "en"
ASR_PROMPT = os.environ.get(
    "VT_ASR_PROMPT",
    "以下是普通话和 English 混合的语音输入，技术术语保留英文，例如：我用 Rust 写了一个 "
    "Wayland 上的 voice typing 工具，然后 push 到 GitHub。",
)

app = FastAPI(title="voice-type asr")

_model: Optional[WhisperModel] = None


def get_model() -> WhisperModel:
    global _model
    if _model is None:
        print(
            f"[asr] loading model='{ASR_MODEL}' "
            f"device='{ASR_DEVICE}' compute_type='{ASR_COMPUTE_TYPE}' ..."
        )
        _model = WhisperModel(
            ASR_MODEL, device=ASR_DEVICE, compute_type=ASR_COMPUTE_TYPE
        )
        print("[asr] model ready")
    return _model


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "model": ASR_MODEL, "beam": ASR_BEAM, "language": ASR_LANGUAGE}


@app.post("/v1/audio/transcriptions")
async def transcriptions(file: UploadFile = File(...)) -> JSONResponse:
    data = await file.read()
    if not data:
        return JSONResponse({"text": ""})

    segments, _info = get_model().transcribe(
        io.BytesIO(data),
        vad_filter=True,
        beam_size=ASR_BEAM,
        language=ASR_LANGUAGE,
        initial_prompt=ASR_PROMPT or None,
        condition_on_previous_text=False,
        task="transcribe",
    )
    # segments is a generator; materialize it.
    text = "".join(seg.text for seg in segments).strip()
    return JSONResponse({"text": text})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "asr_server:app",
        host=os.environ.get("VT_ASR_HOST", "127.0.0.1"),
        port=int(os.environ.get("VT_ASR_PORT", "8000")),
    )
