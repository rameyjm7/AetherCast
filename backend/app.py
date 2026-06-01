import os
import queue
import threading
from dataclasses import dataclass

import numpy as np
import requests
import websocket
from websocket import WebSocketConnectionClosedException
from flask import Flask, Response, jsonify, request, send_from_directory


def _gateway_base() -> str:
    return os.getenv("SDR_GATEWAY_BASE_URL", "http://127.0.0.1:8080").rstrip("/")


def _gateway_token() -> str:
    token = (os.getenv("SDR_GATEWAY_API_TOKEN", "") or "").strip()
    if token:
        return token
    return "Vaed36MgaPWugC0Ie5KLYGsiR9wRWKDN/yMNImjGyyENH9lsmZMHUfcRiKShAr4Y"


def _gateway_headers() -> dict:
    token = _gateway_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _ws_url_for_stream(stream_id: str) -> str:
    base = _gateway_base()
    if base.startswith("https://"):
        ws_base = "wss://" + base[len("https://") :]
    else:
        ws_base = "ws://" + base[len("http://") :]
    return f"{ws_base}/ws/iq/{stream_id}"


@dataclass
class RadioState:
    running: bool = False
    stream_id: str | None = None
    device_id: str | None = None
    freq_mhz: float = 92.1
    sample_rate_sps: int = 2000000
    worker_alive: bool = False
    produced_chunks: int = 0
    served_chunks: int = 0
    last_audio_rms: float = 0.0
    worker_error: str = ""
    gateway_start_response: dict | None = None


class FmDemod:
    """Very simple mono narrow pipeline for broadcast FM listening."""

    def __init__(self, in_rate: int, out_rate: int = 48000):
        self.in_rate = int(in_rate)
        self.out_rate = int(out_rate)
        # Keep discriminator stream reasonably wide, then resample to audio.
        self.decim = max(1, int(round(self.in_rate / 240000.0)))
        self.demod_rate = self.in_rate / float(self.decim)
        self.prev = np.complex64(1.0 + 0j)
        self.resample_pos = 0.0
        self._leftover = b""

    def process_iq_i8(self, raw: bytes) -> bytes:
        if not raw:
            return b""
        if self._leftover:
            raw = self._leftover + raw
            self._leftover = b""
        if len(raw) % 2 != 0:
            self._leftover = raw[-1:]
            raw = raw[:-1]
        if not raw:
            return b""
        iq = np.frombuffer(raw, dtype=np.int8).astype(np.float32)
        if iq.size < 4:
            return b""
        i = iq[0::2] / 128.0
        q = iq[1::2] / 128.0

        # Fast boxcar decimation to reduce CPU load and smooth RF noise.
        if self.decim > 1:
            n = (i.size // self.decim) * self.decim
            if n < self.decim:
                return b""
            i = i[:n].reshape(-1, self.decim).mean(axis=1)
            q = q[:n].reshape(-1, self.decim).mean(axis=1)
        z = (i + 1j * q).astype(np.complex64)

        z_prev = np.empty_like(z)
        z_prev[0] = self.prev
        z_prev[1:] = z[:-1]
        self.prev = z[-1]

        # Phase discriminator FM demod.
        demod = np.angle(z * np.conj(z_prev)).astype(np.float32)

        if demod.size < 4:
            return b""

        # Light audio smoothing (moving average) for hiss control.
        kernel = np.array([0.2, 0.2, 0.2, 0.2, 0.2], dtype=np.float32)
        y = np.convolve(demod, kernel, mode="same")

        # Resample to out_rate using linear interpolation with continuity.
        step = self.demod_rate / float(self.out_rate)
        positions = np.arange(self.resample_pos, y.size - 1, step, dtype=np.float32)
        if positions.size == 0:
            self.resample_pos = float(self.resample_pos + y.size)
            return b""
        idx = np.floor(positions).astype(np.int32)
        frac = positions - idx
        audio_f = y[idx] * (1.0 - frac) + y[idx + 1] * frac
        self.resample_pos = float(positions[-1] + step - (y.size - 1))

        # Normalize and convert to PCM16 mono.
        peak = float(np.max(np.abs(audio_f))) if audio_f.size else 1.0
        scale = 0.85 / max(peak, 0.2)
        audio = np.clip(audio_f * scale, -1.0, 1.0)
        pcm = (audio * 32767.0).astype(np.int16)
        return pcm.tobytes()


app = Flask(__name__, static_folder="../frontend", static_url_path="")
state = RadioState()
audio_q: queue.Queue[bytes] = queue.Queue(maxsize=80)
worker_stop = threading.Event()
worker_thread: threading.Thread | None = None


def _drain_audio_queue() -> None:
    while not audio_q.empty():
        try:
            audio_q.get_nowait()
        except queue.Empty:
            break


def _worker_loop(stream_id: str, sample_rate_sps: int) -> None:
    demod = FmDemod(sample_rate_sps)
    ws = websocket.WebSocket()
    pcm_accum = bytearray()
    target_chunk_bytes = 32768
    state.worker_alive = True
    state.worker_error = "Worker starting"
    try:
        headers = []
        token = _gateway_token()
        if token:
            headers.append(f"Authorization: Bearer {token}")
        ws.connect(_ws_url_for_stream(stream_id), timeout=8, header=headers)
        ws.settimeout(1.0)
        state.worker_error = ""
        while not worker_stop.is_set():
            try:
                chunk = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except WebSocketConnectionClosedException:
                state.worker_error = "Gateway websocket closed"
                break
            except Exception as exc:
                state.worker_error = f"Worker recv error: {exc}"
                break
            if not isinstance(chunk, (bytes, bytearray)):
                continue
            pcm = demod.process_iq_i8(bytes(chunk))
            if not pcm:
                continue
            pcm_accum.extend(pcm)
            if len(pcm_accum) < target_chunk_bytes:
                continue
            out = bytes(pcm_accum)
            pcm_accum.clear()
            audio_i16 = np.frombuffer(out, dtype=np.int16)
            if audio_i16.size:
                state.last_audio_rms = float(np.sqrt(np.mean((audio_i16.astype(np.float32) / 32768.0) ** 2)))
            state.produced_chunks += 1
            try:
                audio_q.put(out, timeout=0.1)
            except queue.Full:
                try:
                    audio_q.get_nowait()
                except queue.Empty:
                    pass
                try:
                    audio_q.put_nowait(out)
                except queue.Full:
                    pass
    finally:
        state.worker_alive = False
        try:
            ws.close()
        except Exception:
            pass
        # If worker exits unexpectedly, reflect stopped state so frontend can react.
        if not worker_stop.is_set():
            if not state.worker_error:
                state.worker_error = "Worker exited unexpectedly"
            state.running = False
            state.stream_id = None


def _stop_stream() -> None:
    global worker_thread
    worker_stop.set()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=2.0)
    worker_thread = None

    stream_id = state.stream_id
    if stream_id:
        try:
            requests.post(
                f"{_gateway_base()}/streams/{stream_id}/stop",
                headers=_gateway_headers(),
                timeout=5,
            )
        except Exception:
            pass

    state.running = False
    state.stream_id = None
    state.produced_chunks = 0
    state.served_chunks = 0
    state.last_audio_rms = 0.0
    state.worker_alive = False
    state.worker_error = ""
    state.gateway_start_response = None
    _drain_audio_queue()


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/devices")
def devices():
    resp = requests.get(f"{_gateway_base()}/devices", headers=_gateway_headers(), timeout=10)
    return jsonify(resp.json())


@app.post("/api/radio/start")
def start_radio():
    global worker_thread
    payload = request.get_json(force=True) or {}
    device_id = str(payload.get("device_id", "")).strip()
    freq_mhz = float(payload.get("freq_mhz", 92.1))
    sample_rate_sps = int(payload.get("sample_rate_sps", 2000000))

    if not device_id:
        return jsonify({"error": "device_id is required"}), 400

    if state.running:
        _stop_stream()

    resp = requests.post(
        f"{_gateway_base()}/streams/start",
        headers=_gateway_headers(),
        json={
            "device_id": device_id,
            "center_freq_hz": int(freq_mhz * 1e6),
            "sample_rate_sps": sample_rate_sps,
            "lna_gain_db": 32,
            "vga_gain_db": 40,
            "amp_enable": False,
            "baseband_filter_hz": sample_rate_sps,
            "duration_seconds": None,
            "num_samples": None,
        },
        timeout=12,
    )
    if resp.status_code >= 400:
        return jsonify(resp.json()), resp.status_code

    body = resp.json()
    stream_id = body["stream_id"]
    actual_rate = int(body.get("config", {}).get("sample_rate_sps", sample_rate_sps))
    worker_stop.clear()
    _drain_audio_queue()

    worker_thread = threading.Thread(target=_worker_loop, args=(stream_id, actual_rate), daemon=True)
    worker_thread.start()

    state.running = True
    state.stream_id = stream_id
    state.device_id = device_id
    state.freq_mhz = freq_mhz
    state.sample_rate_sps = actual_rate
    state.gateway_start_response = body

    return jsonify({"ok": True, "stream_id": stream_id, "sample_rate_sps": actual_rate})


@app.post("/api/radio/stop")
def stop_radio():
    _stop_stream()
    return jsonify({"ok": True})


@app.get("/api/radio/status")
def status():
    return jsonify(
        {
            "running": state.running,
            "stream_id": state.stream_id,
            "device_id": state.device_id,
            "freq_mhz": state.freq_mhz,
            "sample_rate_sps": state.sample_rate_sps,
            "queued_chunks": audio_q.qsize(),
            "worker_alive": state.worker_alive,
            "produced_chunks": state.produced_chunks,
            "served_chunks": state.served_chunks,
            "last_audio_rms": state.last_audio_rms,
            "worker_error": state.worker_error,
            "gateway_start_response": state.gateway_start_response,
        }
    )


@app.get("/api/audio/chunk")
def audio_chunk():
    if not state.running:
        return Response(b"", mimetype="application/octet-stream", status=204)

    timeout = float(request.args.get("timeout", 0.7))
    timeout = max(0.05, min(timeout, 2.0))

    try:
        pcm = audio_q.get(timeout=timeout)
    except queue.Empty:
        return Response(b"", mimetype="application/octet-stream", status=204)

    state.served_chunks += 1
    return Response(pcm, mimetype="application/octet-stream")


@app.get("/api/audio/batch")
def audio_batch():
    if not state.running:
        return Response(b"", mimetype="application/octet-stream", status=204)

    count = int(request.args.get("count", 6))
    count = max(1, min(count, 16))
    timeout = float(request.args.get("timeout", 0.4))
    timeout = max(0.05, min(timeout, 2.0))

    chunks: list[bytes] = []
    for idx in range(count):
        try:
            pcm = audio_q.get(timeout=timeout if idx == 0 else 0.02)
        except queue.Empty:
            break
        chunks.append(pcm)
        state.served_chunks += 1

    if not chunks:
        return Response(b"", mimetype="application/octet-stream", status=204)
    return Response(b"".join(chunks), mimetype="application/octet-stream")


if __name__ == "__main__":
    host = os.getenv("FM_RADIO_HOST", "0.0.0.0")
    port = int(os.getenv("FM_RADIO_PORT", "5050"))
    app.run(host=host, port=port, threaded=True)
