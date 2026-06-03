import os
import queue
import json
import shutil
import subprocess
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
    return f"{ws_base}/ws/iq/{stream_id}?keep=1"


@dataclass
class RadioState:
    running: bool = False
    stream_id: str | None = None
    device_id: str | None = None
    freq_mhz: float = 92.1
    sample_rate_sps: int = 2000000
    lna_gain_db: int = 32
    vga_gain_db: int = 40
    worker_alive: bool = False
    produced_chunks: int = 0
    served_chunks: int = 0
    last_audio_rms: float = 0.0
    worker_error: str = ""
    gateway_start_response: dict | None = None
    rds_available: bool = False
    rds_enabled: bool = False
    rds_ps: str = ""
    rds_rt: str = ""
    rds_pi: str = ""
    rds_pty: str = ""
    rds_feed_bytes: int = 0
    rds_lines: int = 0
    rds_json_lines: int = 0
    rds_last_line: str = ""
    rds_error: str = ""
    rds_stderr: str = ""
    rds_pilot_db: float = -120.0
    rds_subcarrier_db: float = -120.0
    empty_audio_polls: int = 0
    restarting: bool = False


class FmDemod:
    """Very simple mono narrow pipeline for broadcast FM listening."""

    def __init__(self, in_rate: int, out_rate: int = 48000):
        self.in_rate = int(in_rate)
        self.out_rate = int(out_rate)
        # Keep discriminator stream reasonably wide, then resample to audio.
        self.decim = max(1, int(round(self.in_rate / 240000.0)))
        self.demod_rate = self.in_rate / float(self.decim)
        self.prev = np.complex64(1.0 + 0j)
        self.channel_filter = self._design_lowpass(num_taps=129, cutoff_hz=115000.0)
        self._filter_tail = np.zeros(max(0, self.channel_filter.size - 1), dtype=np.complex64)
        self.resample_pos = 0.0
        self.resample_pos_rds = 0.0
        self._leftover = b""
        self.rds_rate = 171000
        self.rds_pilot_db = -120.0
        self.rds_subcarrier_db = -120.0
        self._rds_metric_buf = np.empty(0, dtype=np.float32)
        self._rds_scale = 1.0

    def process_iq_i8(self, raw: bytes) -> tuple[bytes, bytes]:
        if not raw:
            return b"", b""
        if self._leftover:
            raw = self._leftover + raw
            self._leftover = b""
        if len(raw) % 2 != 0:
            self._leftover = raw[-1:]
            raw = raw[:-1]
        if not raw:
            return b"", b""
        iq = np.frombuffer(raw, dtype=np.int8).astype(np.float32)
        if iq.size < 4:
            return b"", b""
        i = iq[0::2] / 128.0
        q = iq[1::2] / 128.0

        z = (i + 1j * q).astype(np.complex64)
        if z.size < 4:
            return b"", b""
        z = self._channel_filter_and_decimate(z)
        if z.size < 4:
            return b"", b""

        z_prev = np.empty_like(z)
        z_prev[0] = self.prev
        z_prev[1:] = z[:-1]
        self.prev = z[-1]

        # Phase discriminator FM demod.
        demod = np.angle(z * np.conj(z_prev)).astype(np.float32)
        if demod.size < 4:
            return b"", b""
        self._update_rds_signal_metrics(demod)

        # RDS path: feed multiplex-like discriminator to redsea at 171 kHz.
        step_rds = self.demod_rate / float(self.rds_rate)
        pos_rds = np.arange(self.resample_pos_rds, demod.size - 1, step_rds, dtype=np.float64)
        if pos_rds.size:
            next_pos_rds = float(pos_rds[-1] + step_rds - (demod.size - 1))
            idx_r = np.floor(pos_rds).astype(np.int32)
            valid_r = idx_r + 1 < demod.size
            idx_r = idx_r[valid_r]
            pos_rds = pos_rds[valid_r]
            if not pos_rds.size:
                self.resample_pos_rds = max(0.0, next_pos_rds)
                rds_pcm = b""
            else:
                frac_r = pos_rds - idx_r
                rds_f = demod[idx_r] * (1.0 - frac_r) + demod[idx_r + 1] * frac_r
                self.resample_pos_rds = max(0.0, next_pos_rds)
                if rds_f.size:
                    block_peak = float(np.percentile(np.abs(rds_f), 99.0))
                    target_scale = 0.85 / max(block_peak, 0.2)
                    self._rds_scale = (self._rds_scale * 0.96) + (target_scale * 0.04)
                rds_pcm = (np.clip(rds_f * self._rds_scale, -1.0, 1.0) * 32767.0).astype(np.int16).tobytes()
        else:
            self.resample_pos_rds = float(self.resample_pos_rds + demod.size)
            rds_pcm = b""

        # Light audio smoothing (moving average) for hiss control.
        kernel = np.array([0.2, 0.2, 0.2, 0.2, 0.2], dtype=np.float32)
        y = np.convolve(demod, kernel, mode="same")

        # Resample to out_rate using linear interpolation with continuity.
        step = self.demod_rate / float(self.out_rate)
        positions = np.arange(self.resample_pos, y.size - 1, step, dtype=np.float64)
        if positions.size == 0:
            self.resample_pos = float(self.resample_pos + y.size)
            return b"", rds_pcm
        next_pos = float(positions[-1] + step - (y.size - 1))
        idx = np.floor(positions).astype(np.int32)
        valid = idx + 1 < y.size
        idx = idx[valid]
        positions = positions[valid]
        if positions.size == 0:
            self.resample_pos = max(0.0, next_pos)
            return b"", rds_pcm
        frac = positions - idx
        audio_f = y[idx] * (1.0 - frac) + y[idx + 1] * frac
        self.resample_pos = max(0.0, next_pos)

        # Normalize and convert to PCM16 mono.
        peak = float(np.max(np.abs(audio_f))) if audio_f.size else 1.0
        scale = 0.85 / max(peak, 0.2)
        audio = np.clip(audio_f * scale, -1.0, 1.0)
        pcm = (audio * 32767.0).astype(np.int16)
        return pcm.tobytes(), rds_pcm

    def _design_lowpass(self, num_taps: int, cutoff_hz: float) -> np.ndarray:
        cutoff = min(cutoff_hz, (self.in_rate / 2.0) * 0.92)
        n = np.arange(num_taps, dtype=np.float32) - ((num_taps - 1) / 2.0)
        taps = 2.0 * cutoff / self.in_rate * np.sinc(2.0 * cutoff / self.in_rate * n)
        taps *= np.hamming(num_taps).astype(np.float32)
        taps /= np.sum(taps)
        return taps.astype(np.float32)

    def _channel_filter_and_decimate(self, z: np.ndarray) -> np.ndarray:
        if self.channel_filter.size <= 1:
            return z
        z_ext = np.concatenate((self._filter_tail, z))
        filtered = np.convolve(z_ext, self.channel_filter, mode="valid").astype(np.complex64)
        self._filter_tail = z_ext[-self._filter_tail.size :].astype(np.complex64) if self._filter_tail.size else self._filter_tail
        return filtered[:: self.decim] if self.decim > 1 else filtered

    def _update_rds_signal_metrics(self, demod: np.ndarray) -> None:
        if demod.size:
            self._rds_metric_buf = np.concatenate((self._rds_metric_buf, demod.astype(np.float32, copy=False)))
            max_metric_samples = int(max(8192, self.demod_rate * 0.35))
            if self._rds_metric_buf.size > max_metric_samples:
                self._rds_metric_buf = self._rds_metric_buf[-max_metric_samples:]

        if self._rds_metric_buf.size < 4096:
            return
        n = min(32768, self._rds_metric_buf.size)
        samples = self._rds_metric_buf[-n:]
        samples = samples - float(np.mean(samples))
        windowed = samples * np.hanning(n).astype(np.float32)
        spectrum = np.abs(np.fft.rfft(windowed)) ** 2
        freqs = np.fft.rfftfreq(n, d=1.0 / self.demod_rate)
        noise = float(np.median(spectrum)) + 1e-12

        def band_db(center_hz: float, width_hz: float) -> float:
            band = (freqs >= center_hz - width_hz) & (freqs <= center_hz + width_hz)
            if not np.any(band):
                return -120.0
            power = float(np.mean(spectrum[band]))
            return float(10.0 * np.log10((power + 1e-12) / noise))

        self.rds_pilot_db = band_db(19000.0, 800.0)
        self.rds_subcarrier_db = band_db(57000.0, 3500.0)


class RdsDecoder:
    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.alive = False
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._feed_buf = bytearray()

    def start(self) -> bool:
        if shutil.which("redsea") is None:
            state.rds_error = "redsea not found"
            return False
        try:
            self.proc = subprocess.Popen(
                ["redsea", "--input", "mpx", "--samplerate", "171000", "--output", "json", "--rbds", "--show-partial"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                bufsize=0,
            )
        except Exception as exc:
            state.rds_error = f"redsea start failed: {exc}"
            return False
        self.alive = True
        state.rds_error = ""
        state.rds_stderr = ""
        self._stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._stdout_thread.start()
        self._stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self._stderr_thread.start()
        return True

    def _read_stdout(self) -> None:
        if self.proc is None or self.proc.stdout is None:
            return
        while self.alive:
            line = self.proc.stdout.readline()
            if not line:
                break
            state.rds_lines += 1
            state.rds_last_line = line.decode("utf-8", errors="ignore").strip()[-300:]
            try:
                data = json.loads(state.rds_last_line)
            except Exception as exc:
                state.rds_error = f"redsea non-json output: {exc}"
                continue
            state.rds_json_lines += 1
            state.rds_error = ""
            state.rds_pi = str(data.get("pi", state.rds_pi) or state.rds_pi)
            state.rds_ps = str(data.get("ps", data.get("partial_ps", state.rds_ps)) or state.rds_ps)
            state.rds_rt = str(
                data.get("radiotext", data.get("partial_radiotext", state.rds_rt)) or state.rds_rt
            )
            state.rds_pty = str(data.get("prog_type", data.get("partial_prog_type", state.rds_pty)) or state.rds_pty)

    def _read_stderr(self) -> None:
        if self.proc is None or self.proc.stderr is None:
            return
        while self.alive:
            line = self.proc.stderr.readline()
            if not line:
                break
            state.rds_stderr = line.decode("utf-8", errors="ignore").strip()[-300:]

    def feed(self, pcm171k: bytes) -> None:
        if not self.alive or self.proc is None or self.proc.stdin is None or not pcm171k:
            return
        try:
            self._feed_buf.extend(pcm171k)
            if len(self._feed_buf) < 8192:
                return
            out = bytes(self._feed_buf)
            self._feed_buf.clear()
            self.proc.stdin.write(out)
            self.proc.stdin.flush()
            state.rds_feed_bytes += len(out)
        except Exception:
            self.alive = False
            state.rds_error = "redsea stdin closed"

    def stop(self) -> None:
        self.alive = False
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
            except Exception:
                pass
        self.proc = None


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


def _gateway_streams() -> list[dict]:
    try:
        resp = requests.get(f"{_gateway_base()}/streams", headers=_gateway_headers(), timeout=2)
        if resp.status_code >= 400:
            return []
        streams = resp.json()
        return streams if isinstance(streams, list) else []
    except requests.RequestException:
        return []


def _stop_gateway_stream(stream_id: str) -> None:
    if not stream_id:
        return
    try:
        requests.post(f"{_gateway_base()}/streams/{stream_id}/stop", headers=_gateway_headers(), timeout=3)
    except requests.RequestException:
        pass


def _stop_duplicate_gateway_streams(device_id: str | None, keep_stream_id: str | None = None) -> None:
    if not device_id:
        return
    for stream in _gateway_streams():
        stream_id = str(stream.get("stream_id", "")).strip()
        cfg = stream.get("config", {}) or {}
        if stream_id and stream_id != keep_stream_id and str(cfg.get("device_id", "")).strip() == device_id:
            _stop_gateway_stream(stream_id)


def _worker_loop(stream_id: str, sample_rate_sps: int) -> None:
    demod = FmDemod(sample_rate_sps)
    rds = RdsDecoder()
    state.rds_available = shutil.which("redsea") is not None
    state.rds_enabled = rds.start() if state.rds_available else False
    state.rds_feed_bytes = 0
    state.rds_lines = 0
    state.rds_json_lines = 0
    state.rds_last_line = ""
    state.rds_stderr = ""
    state.rds_pilot_db = -120.0
    state.rds_subcarrier_db = -120.0
    if not state.rds_available:
        state.rds_ps = ""
        state.rds_rt = ""
        state.rds_pi = ""
        state.rds_pty = ""
    pcm_accum = bytearray()
    target_chunk_bytes = 32768
    state.worker_alive = True
    state.worker_error = "Worker starting"
    try:
        headers = []
        token = _gateway_token()
        if token:
            headers.append(f"Authorization: Bearer {token}")
        while not worker_stop.is_set():
            ws = websocket.WebSocket()
            try:
                ws.connect(_ws_url_for_stream(stream_id), timeout=8, header=headers)
                ws.settimeout(1.0)
                state.worker_error = ""
                while not worker_stop.is_set() and state.stream_id == stream_id:
                    try:
                        chunk = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    except WebSocketConnectionClosedException:
                        state.worker_error = "Gateway websocket closed; reconnecting"
                        break
                    except Exception as exc:
                        state.worker_error = f"Worker recv error: {exc}; reconnecting"
                        break
                    if not isinstance(chunk, (bytes, bytearray)):
                        continue
                    try:
                        pcm, rds_pcm = demod.process_iq_i8(bytes(chunk))
                    except Exception as exc:
                        state.worker_error = f"Demod error: {exc}"
                        break
                    state.rds_pilot_db = demod.rds_pilot_db
                    state.rds_subcarrier_db = demod.rds_subcarrier_db
                    if state.rds_enabled:
                        rds.feed(rds_pcm)
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
                try:
                    ws.close()
                except Exception:
                    pass
            if not worker_stop.is_set() and state.stream_id == stream_id:
                worker_stop.wait(0.75)
    finally:
        rds.stop()
        if state.stream_id == stream_id:
            state.worker_alive = False
        # Only the active stream worker is allowed to change the visible radio state.
        if state.stream_id == stream_id and not worker_stop.is_set():
            if not state.worker_error:
                state.worker_error = "Worker exited unexpectedly"
            state.running = False
            state.stream_id = None


def _attach_existing_stream(
    stream_id: str,
    device_id: str,
    center_freq_hz: int,
    sample_rate_sps: int,
    lna_gain_db: int,
    vga_gain_db: int,
) -> None:
    global worker_thread
    if worker_thread and worker_thread.is_alive() and state.stream_id == stream_id:
        _stop_duplicate_gateway_streams(device_id, stream_id)
        return
    if worker_thread and worker_thread.is_alive():
        worker_stop.set()
        worker_thread.join(timeout=1.5)
        worker_thread = None
    worker_stop.clear()
    _drain_audio_queue()
    _stop_duplicate_gateway_streams(device_id, stream_id)
    worker_thread = threading.Thread(target=_worker_loop, args=(stream_id, sample_rate_sps), daemon=True)
    worker_thread.start()
    state.running = True
    state.stream_id = stream_id
    state.device_id = device_id
    state.freq_mhz = float(center_freq_hz) / 1e6
    state.sample_rate_sps = int(sample_rate_sps)
    state.lna_gain_db = int(lna_gain_db)
    state.vga_gain_db = int(vga_gain_db)
    state.worker_error = ""
    state.empty_audio_polls = 0


def _try_attach_from_gateway() -> None:
    try:
        streams = _gateway_streams()
        if not streams:
            return
        if state.running and state.stream_id and any(str(s.get("stream_id", "")).strip() == state.stream_id for s in streams):
            _stop_duplicate_gateway_streams(state.device_id, state.stream_id)
            return
        first = streams[-1]
        cfg = first.get("config", {}) or {}
        stream_id = str(first.get("stream_id", "")).strip()
        device_id = str(cfg.get("device_id", "")).strip()
        center_freq_hz = int(cfg.get("center_freq_hz", 92_100_000))
        sample_rate_sps = int(cfg.get("sample_rate_sps", 2_000_000))
        lna_gain_db = int(cfg.get("lna_gain_db", 32))
        vga_gain_db = int(cfg.get("vga_gain_db", 40))
        if not stream_id or not device_id:
            return
        _attach_existing_stream(stream_id, device_id, center_freq_hz, sample_rate_sps, lna_gain_db, vga_gain_db)
    except Exception:
        return


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
    state.rds_enabled = False
    _drain_audio_queue()


def _restart_current_stream(reason: str) -> bool:
    global worker_thread
    if state.restarting or not state.device_id:
        return False
    state.restarting = True
    state.worker_error = f"Restarting stream: {reason}"
    device_id = state.device_id
    freq_mhz = state.freq_mhz
    sample_rate_sps = state.sample_rate_sps
    lna_gain_db = state.lna_gain_db
    vga_gain_db = state.vga_gain_db
    try:
        worker_stop.set()
        if worker_thread and worker_thread.is_alive():
            worker_thread.join(timeout=1.5)
        worker_thread = None
        if state.stream_id:
            _stop_gateway_stream(state.stream_id)
        _stop_duplicate_gateway_streams(device_id)
        _drain_audio_queue()

        resp = requests.post(
            f"{_gateway_base()}/streams/start",
            headers=_gateway_headers(),
            json={
                "device_id": device_id,
                "center_freq_hz": int(freq_mhz * 1e6),
                "sample_rate_sps": sample_rate_sps,
                "lna_gain_db": lna_gain_db,
                "vga_gain_db": vga_gain_db,
                "amp_enable": False,
                "baseband_filter_hz": sample_rate_sps,
                "duration_seconds": None,
                "num_samples": None,
            },
            timeout=12,
        )
        if resp.status_code >= 400:
            state.worker_error = f"Restart failed: HTTP {resp.status_code}"
            return False
        body = resp.json()
        stream_id = body["stream_id"]
        accepted_config = body.get("config", {}) or {}
        actual_rate = int(accepted_config.get("sample_rate_sps", sample_rate_sps))
        state.stream_id = stream_id
        state.sample_rate_sps = actual_rate
        state.lna_gain_db = int(accepted_config.get("lna_gain_db", lna_gain_db))
        state.vga_gain_db = int(accepted_config.get("vga_gain_db", vga_gain_db))
        state.gateway_start_response = body
        state.produced_chunks = 0
        state.served_chunks = 0
        state.last_audio_rms = 0.0
        state.empty_audio_polls = 0
        worker_stop.clear()
        worker_thread = threading.Thread(target=_worker_loop, args=(stream_id, actual_rate), daemon=True)
        worker_thread.start()
        state.running = True
        state.worker_error = ""
        return True
    except Exception as exc:
        state.worker_error = f"Restart failed: {exc}"
        return False
    finally:
        state.restarting = False


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/devices")
def devices():
    try:
        resp = requests.get(f"{_gateway_base()}/devices", headers=_gateway_headers(), timeout=3)
        if resp.status_code >= 400:
            return jsonify(resp.json()), resp.status_code
        return jsonify(resp.json())
    except requests.RequestException as exc:
        return jsonify(
            {
                "error": "sdr-gateway is unavailable",
                "detail": str(exc),
                "gateway_base": _gateway_base(),
            }
        ), 503


@app.post("/api/radio/start")
def start_radio():
    global worker_thread
    payload = request.get_json(force=True) or {}
    device_id = str(payload.get("device_id", "")).strip()
    freq_mhz = float(payload.get("freq_mhz", 92.1))
    sample_rate_sps = int(payload.get("sample_rate_sps", 2000000))
    lna_gain_db = int(payload.get("lna_gain_db", 32))
    vga_gain_db = int(payload.get("vga_gain_db", 40))

    if not device_id:
        return jsonify({"error": "device_id is required"}), 400
    if not 87.5 <= freq_mhz <= 108.0:
        return jsonify({"error": "freq_mhz must be between 87.5 and 108.0"}), 400

    if state.running:
        _stop_stream()

    try:
        resp = requests.post(
            f"{_gateway_base()}/streams/start",
            headers=_gateway_headers(),
            json={
                "device_id": device_id,
                "center_freq_hz": int(freq_mhz * 1e6),
                "sample_rate_sps": sample_rate_sps,
                "lna_gain_db": lna_gain_db,
                "vga_gain_db": vga_gain_db,
                "amp_enable": False,
                "baseband_filter_hz": sample_rate_sps,
                "duration_seconds": None,
                "num_samples": None,
            },
            timeout=12,
        )
    except requests.RequestException as exc:
        return jsonify(
            {
                "error": "sdr-gateway is unavailable",
                "detail": str(exc),
                "gateway_base": _gateway_base(),
            }
        ), 503
    if resp.status_code >= 400:
        return jsonify(resp.json()), resp.status_code

    body = resp.json()
    stream_id = body["stream_id"]
    accepted_config = body.get("config", {}) or {}
    actual_rate = int(accepted_config.get("sample_rate_sps", sample_rate_sps))
    actual_lna = int(accepted_config.get("lna_gain_db", lna_gain_db))
    actual_vga = int(accepted_config.get("vga_gain_db", vga_gain_db))
    worker_stop.clear()
    _drain_audio_queue()

    worker_thread = threading.Thread(target=_worker_loop, args=(stream_id, actual_rate), daemon=True)
    worker_thread.start()

    state.running = True
    state.stream_id = stream_id
    state.device_id = device_id
    state.freq_mhz = freq_mhz
    state.sample_rate_sps = actual_rate
    state.lna_gain_db = actual_lna
    state.vga_gain_db = actual_vga
    state.worker_error = ""
    state.gateway_start_response = body

    return jsonify(
        {
            "ok": True,
            "stream_id": stream_id,
            "sample_rate_sps": actual_rate,
            "lna_gain_db": actual_lna,
            "vga_gain_db": actual_vga,
        }
    )


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
            "lna_gain_db": state.lna_gain_db,
            "vga_gain_db": state.vga_gain_db,
            "queued_chunks": audio_q.qsize(),
            "worker_alive": state.worker_alive,
            "produced_chunks": state.produced_chunks,
            "served_chunks": state.served_chunks,
            "last_audio_rms": state.last_audio_rms,
            "worker_error": state.worker_error,
            "gateway_start_response": state.gateway_start_response,
            "rds_available": state.rds_available,
            "rds_enabled": state.rds_enabled,
            "rds_pi": state.rds_pi,
            "rds_ps": state.rds_ps,
            "rds_rt": state.rds_rt,
            "rds_pty": state.rds_pty,
            "rds_feed_bytes": state.rds_feed_bytes,
            "rds_lines": state.rds_lines,
            "rds_json_lines": state.rds_json_lines,
            "rds_last_line": state.rds_last_line,
            "rds_error": state.rds_error,
            "rds_stderr": state.rds_stderr,
            "rds_pilot_db": state.rds_pilot_db,
            "rds_subcarrier_db": state.rds_subcarrier_db,
        }
    )


@app.post("/api/radio/attach-existing")
def attach_existing():
    _try_attach_from_gateway()
    return jsonify({"ok": True, "running": state.running, "stream_id": state.stream_id})


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
        state.empty_audio_polls += 1
        if (
            state.empty_audio_polls >= 4
            and state.running
            and "websocket closed" in (state.worker_error or "").lower()
        ):
            _restart_current_stream("audio stalled after websocket close")
        return Response(b"", mimetype="application/octet-stream", status=204)
    state.empty_audio_polls = 0
    return Response(b"".join(chunks), mimetype="application/octet-stream")


if __name__ == "__main__":
    host = os.getenv("FM_RADIO_HOST", "0.0.0.0")
    port = int(os.getenv("FM_RADIO_PORT", "5050"))
    app.run(host=host, port=port, threaded=True)
