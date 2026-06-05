# AetherCast

AetherCast turns local broadcast FM from an SDR into a browser-playable radio stream.
<img width="920" height="606" alt="image" src="https://github.com/user-attachments/assets/7ec138e5-15c3-4351-be91-34bee981b784" />




It is built with the same overall approach as SDR-Shark:

- SDR IQ is sourced from `sdr-gateway`
- Backend handles SDR control + FM demodulation
- Frontend is a simple tuner + audio player UI

## Features

- Lists SDR devices from `sdr-gateway`
- Starts/stops a gateway IQ stream
- Demodulates FM (phase discriminator + de-emphasis)
- Streams PCM audio chunks to browser for playback
- Shows basic RDS diagnostics when `redsea` is installed

## Project Layout

- `backend/app.py`: Flask API + FM demod worker
- `frontend/index.html`: simple web UI

## Requirements

- Running `sdr-gateway` instance (`http://127.0.0.1:8080` default)
- Python 3.10+
- One SDR device visible in `sdr-gateway /devices`

## Setup

```bash
cd /home/jake/workspace/SDR/AetherCast
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If `sdr-gateway` auth is enabled:

```bash
export SDR_GATEWAY_API_TOKEN="<your-token>"
```

Optional base URL override:

```bash
export SDR_GATEWAY_BASE_URL="http://127.0.0.1:8080"
```

## Run

```bash
cd /home/jake/workspace/SDR/AetherCast
source .venv/bin/activate
python3 backend/app.py
```

Open:

- `http://127.0.0.1:5050`

Tune frequency (MHz), select device, press play.

## Notes

- Default receive sample rate is `2000000` sps for better FM demodulation stability.
- Audio is mono FM and intentionally minimal for a first usable baseline.
- If playback is choppy, reduce system load or switch to a lower-latency SDR backend/device.
