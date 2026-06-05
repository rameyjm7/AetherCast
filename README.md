# AetherCast

AetherCast turns local broadcast FM into a polished, browser-playable radio experience using an SDR and `sdr-gateway`.

<img width="920" height="606" alt="AetherCast screenshot" src="https://github.com/user-attachments/assets/7ec138e5-15c3-4351-be91-34bee981b784" />

It follows the same overall model as SDR-Shark:

- `sdr-gateway` provides SDR control and IQ streaming
- the backend handles FM demodulation and audio delivery
- the frontend provides a lightweight tuner, playback controls, and live metadata

## Features

- Browser-playable FM radio sourced from a local SDR
- Device discovery through `sdr-gateway`
- Start, stop, and retune without leaving the page
- Live frequency display with recent-station history
- Optional stereo decode with a simplified default UI
- RDS station and song metadata when `redsea` is installed
- Real-time spectrum view

## Architecture

- `backend/app.py`
  Flask API, gateway control, FM demodulation worker, audio chunk streaming, and RDS integration
- `frontend/index.html`
  Single-page UI for tuning, playback, spectrum display, and metadata presentation

## Requirements

- Python 3.10+
- A running `sdr-gateway` instance
- At least one SDR device visible from `sdr-gateway /devices`

Default gateway URL:

```text
http://127.0.0.1:8080
```

## Setup

```bash
cd /home/jake/workspace/SDR/AetherCast
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If gateway auth is enabled:

```bash
export SDR_GATEWAY_API_TOKEN="<your-token>"
```

If your gateway is not running on the default address:

```bash
export SDR_GATEWAY_BASE_URL="http://127.0.0.1:8080"
```

## Run

```bash
cd /home/jake/workspace/SDR/AetherCast
source .venv/bin/activate
python3 backend/app.py
```

Then open:

```text
http://127.0.0.1:5050
```

Select a device, tune a station, and press play.

## Optional RDS Support

For richer station and song metadata, install `redsea` and make sure it is available on your `PATH`.

When RDS is available, AetherCast can display:

- station name
- song title
- artist
- lock state in the top status bar

## Notes

- AetherCast currently uses a `2000000` sps receive rate by default for stable FM demodulation.
- The UI is intentionally simple by default, with extra controls hidden behind `Advanced`.
- If playback becomes choppy, check system load, SDR bandwidth stability, and gateway health first.
