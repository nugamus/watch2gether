# watch2gether (movie-night fork)

Watch videos from your own server together with a friend over the internet, in strict sync, with proper subtitles and a password-free link for your guest.

This is a fork of [stasnocap/watch2gether](https://github.com/stasnocap/watch2gether) by stasnocap, which provides the single-binary design and the strict-sync engine. This fork adds HLS preparation for MKV/HEVC files, subtitles, per-viewer audio tracks, host/guest roles with share links, chat, and a Docker image for home servers. MIT licensed, like upstream.

It is still one small Rust binary ([axum](https://github.com/tokio-rs/axum) + [tokio](https://tokio.rs)) with the web page compiled in, ffmpeg for media work, and a JSON file for state. There is no database server.

## Features

- **Strict sync.** Play, pause and seek apply to everyone. If one viewer stalls or is still loading, the room waits for them ("Waiting for Lin to buffer…"), then resumes for all from the same position. On a slow link the buffer required before resuming grows, so the room pauses less often but for longer.
- **Plays MKV, HEVC and multi-audio files everywhere.** Files browsers can already play (H.264 MP4 with one AAC track, VP9/AV1 WebM) are streamed as-is. Everything else is prepared once, in the background, into an HLS folder in the cache, with a progress indicator for everyone:
  - 8-bit H.264 video is copied, so quality is untouched and preparing a 2 GB movie takes about a minute.
  - HEVC and 10-bit video are transcoded to H.264 (at most 1080p). This uses Intel Quick Sync (VAAPI) when `/dev/dri` is available, otherwise libx264.
  - Every audio track is kept (as AAC stereo), and text subtitles are extracted.
- **Subtitles.**
  - Sources: embedded tracks; files next to the video (`Movie.srt`, `Movie.zh.srt`, `Movie.en.vtt`, `Movie.ass`…); files the host uploads or links to during a session. Added files are stored with the video and are there again next time.
  - Chinese files in GBK/GB18030 or Big5 (and UTF-16) are detected and converted, so they never show as garbage. SRT/ASS/SSA are converted to WebVTT.
  - Each viewer picks their own track and can show a **second track on top** (e.g. Chinese at the bottom, English on top). Size and outline/box style are per viewer.
  - The host sets the **subtitle delay** for everyone (±0.1 s / ±1 s, or `z`/`x`).
- **Audio tracks per viewer**, in Firefox, Chrome and Safari (iPad/iPhone included).
- **Host and guests.**
  - The host logs in with the password and is the only one who sees the library, picks videos, adds subtitles, and manages share links.
  - Guests join through a link with no password. They can play, pause, seek, chat and pick their own tracks, and see nothing else (see below).
- **Chat and reactions.** A small side panel, and six text reactions that float over the video for a moment.
- **mpv-style player.** The video is the whole page, with a thin bar that fades out and the usual mpv keys.

## Sharing with a guest

1. Log in as host and open the library (the list icon). Under **Share links**, enter a name ("Lin") and optionally a number of days until it expires, then press **Create**.
2. Copy the link (`https://your.host/r/<long-random-token>`) and send it. It is shown **only once**, because only a hash of it is stored. If it gets lost, press **New link**.
3. Your guest opens it. The token is swapped for a session cookie and the address bar changes to the plain site address. No password is typed. The same link keeps working for future movie nights, and so does the cookie (30 days). Both survive restarts.
4. Pick a video to start a session; guests who are waiting join automatically. **End session** sends everyone back to "Waiting for the host", and the link stays valid.
5. **New link** replaces the token (the old link and its sessions stop working). **Revoke** deletes the link and disconnects its guests within seconds.

What guests can't do is enforced by the server, not just hidden in the page:

- `/api/*` (library, links, subtitle uploads) answers 403 to guests.
- Video files are only served under `/m/<nonce>/…`, where the nonce changes with every video, and only resolve to the current video's files. Old URLs, other files and `../` give 404.
- Guests only ever see the video's title, never its path. Host-only WebSocket commands from a guest are ignored.
- Tokens are 256-bit random values compared in constant time. Cookies are `HttpOnly`, `SameSite=Lax`, and `Secure` behind HTTPS.

## Keyboard

| Key | Action |
|---|---|
| `space` | play / pause |
| `←` / `→` | seek 5 s |
| `↑` / `↓` | volume |
| `f` | fullscreen |
| `m` | mute |
| `j` | cycle subtitle track |
| `v` | show / hide subtitles |
| `#` | cycle audio track |
| `z` / `x` | subtitle delay −0.1 s / +0.1 s (host) |

## Running on ZimaOS / CasaOS (Docker)

A prebuilt image is published to GitHub Container Registry on every push to `master`: `ghcr.io/nugamus/watch2gether:latest`, plus a `sha-<commit>` tag.

In ZimaOS, add a custom app from this compose file (App Store → Custom Install → Import):

```yaml
name: watch2gether
services:
  watch2gether:
    image: ghcr.io/nugamus/watch2gether:latest
    restart: unless-stopped
    ports:
      - "8096:8080"
    environment:
      W2G_PASSWORD: change-me
      W2G_PRECACHE: lite      # optional: off (default), original or lite
    volumes:
      - /DATA/Media/Movies:/videos:ro
      - /DATA/AppData/watch2gether/cache:/cache
      - /DATA/AppData/watch2gether/data:/data
    devices:
      - /dev/dri:/dev/dri
x-casaos:
  hostname: stream.nugamus.quest
  scheme: https
  port_map: "443"
```

- `/videos` is your library (read-only is fine). `/cache` holds prepared HLS copies, about the size of the original for copied video and smaller for transcodes. It is never cleaned automatically; delete folders in it to free space. `/data` holds `state.json` (links, sessions, subtitle delays) and added subtitles. Back it up if you care about your links.
- `devices: /dev/dri` is optional. With it, HEVC transcodes run on the Intel GPU; the log then says `video encoder for transcodes: Vaapi`. Without it, or if the GPU test fails, transcodes use libx264 and the log says `X264`. Remove the two `devices` lines if your machine has no `/dev/dri`.
- The container runs as root so it can write to folders ZimaOS creates as root and open `/dev/dri`. To run as a normal user instead, add `user: "1000:1000"` and `group_add: ["<gid of /dev/dri/renderD128>"]` (`stat -c %g /dev/dri/renderD128`), and make the cache and data folders writable by that user.

**Reverse proxy (Nginx Proxy Manager):** create a proxy host for `stream.nugamus.quest` pointing to `http://<server-ip>:8096`, enable **Websockets Support**, and request an SSL certificate with Force SSL. NPM sends `X-Forwarded-Proto: https`, which makes cookies `Secure`.

**Making the image public:** GHCR packages start out private, so ZimaOS can't pull them without credentials. After the first workflow run, go to your GitHub profile → **Packages** → **watch2gether** → **Package settings** → **Change visibility** → **Public**. Alternatively, keep it private and log in on the server with `docker login ghcr.io` using a personal access token with `read:packages`.

**Updating:** pull the image again in ZimaOS (or `docker compose pull && docker compose up -d`).

## Running without Docker

Requirements: [Rust](https://rustup.rs) (edition 2024), and [ffmpeg](https://ffmpeg.org) with `ffprobe` on your `PATH`.

```sh
W2G_PASSWORD='choose-a-password' cargo run --release -- ~/Videos --port 8080
```

```
watch2gether [OPTIONS] [DIR]
```

| Option | Default | Description |
|---|---|---|
| `DIR` | `.` | Folder of videos to share (searched recursively) |
| `--port <PORT>` | `8080` | Port to listen on |
| `--bind <ADDR>` | `0.0.0.0` | Address to listen on |
| `--password <PW>` / `W2G_PASSWORD` | | Host password. Prefer the environment variable so it stays out of the process list |
| `--cache-dir <DIR>` | `~/.cache/watch2gether` | Prepared HLS copies and converted subtitles |
| `--data-dir <DIR>` | `~/.local/share/watch2gether` | Share links, sessions, subtitle delays and added subtitles |
| `--no-auth` | | Everyone who can reach the port is a host |

Supported files: `.mp4`, `.m4v`, `.mov`, `.webm`, `.mkv`. Subtitle files: `.srt`, `.ass`, `.ssa`, `.vtt`.

A systemd user service is in `deploy/` (see the comments in it).

## Bandwidth

Copied video streams at its original bitrate (file size ÷ duration: a 2 GB, 2.5-hour movie is about 1.8 Mbit/s). Transcoded video gets about 1.5× the source bitrate, capped at 5 Mbit/s. Each viewer's connection needs to keep up with that. If it can't, strict sync makes the room wait rather than letting the slow viewer fall behind. The player keeps up to a minute of video buffered ahead. hls.js is compiled into the binary, so nothing loads from a CDN (some are blocked in China).

### Viewers on slow or distant connections

The host's library panel has three switches under **Slow connections**. All are off by default.

- **Offer a smaller 720p copy**: the server makes a 720p, ~1.3 Mbit/s copy once (about 1 GB for a 2-hour film) and offers it next to the original. Each viewer has a **Quality** button: *Auto* (the default) lets the player pick by that viewer's speed, so fast viewers keep the original and slow ones get 720p without choosing anything.
- **Download the movie before starting**: every viewer downloads the whole video into the browser first (it needs that much free disk space), with their progress shown to the room. Nobody can stall mid-film after that. On *Auto*, a viewer whose connection is too slow for the original gets the 720p copy instead.
- **Extra buffering for distant viewers**: wait for 10 s of video (not 3 s) before counting someone as ready, and keep up to 10 minutes buffered.

`W2G_PRECACHE` prepares the library ahead of time so choosing a video starts at once: `original` makes HLS copies of everything browsers can't play as-is, `lite` also makes the 720p copies. New files are picked up within two minutes, once their size has stopped changing (so a file that is still being copied in is left alone). One file is prepared at a time, and only while nobody is waiting for a conversion.

## How sync works

The server holds the single source of truth for the room: which video, playing or paused, and the position. Browsers talk to it over a WebSocket.

- Every action (play, pause, seek, picking a video) goes to the server, which broadcasts the new state to everyone.
- While playing, the server re-broadcasts its position every 2 seconds, and any viewer more than 0.6 s off is corrected.
- Every viewer tells the server whether they're **ready**, meaning they have enough video buffered at the current position. The room plays only while everyone is ready:
  - when someone stalls for more than 0.7 s, everyone pauses;
  - after any seek or new video, everyone must confirm they've loaded the new spot before playback resumes;
  - a viewer who joins or reconnects counts as not ready until they've loaded.
- The room resumes from the slowest viewer's position, so a viewer on a weak connection never has to jump forward into video they haven't downloaded.
- **Resume anyway** lets anyone stop waiting for a stalled viewer.
- If a viewer's browser cannot play the video at all, everyone sees why instead of waiting forever.

## Limitations

- **HDR** HEVC is transcoded without tone-mapping, so it looks washed out. Use SDR releases where you can.
- **Image subtitles** (PGS, VobSub) can't be shown; the subtitle menu lists them as skipped. ASS styling and positioning are reduced to plain text with line breaks and italics.
- **The first play** of a file that needs preparing waits for the whole preparation. Copying is quick; a software transcode of a long HEVC film can take a long time, so start it ahead of time (pick the video, then end the session).
- **One room per server.** Everyone with access shares the same room.
- **iPhone** has no element fullscreen; the fullscreen button hides the side panels instead.
- Subtitle links must point to the subtitle file itself (no zip/rar archives).

## Project layout

```
src/main.rs        server: CLI, roles and share links, WebSocket sync, media routes, library, tests
src/convert.rs     probing, direct-play rule, HLS preparation with ffmpeg
src/subs.rs        sidecar discovery, encoding detection, WebVTT conversion, safe URL download
src/store.rs       state.json: links, sessions, added subtitles, delays
static/index.html  the player page (compiled into the binary)
static/login.html  the host login page
static/hls.min.js  hls.js 1.7.3 (Apache-2.0, see static/LICENSE-hls.js.txt)
Dockerfile         build + Debian runtime with ffmpeg and Intel's VAAPI driver
.github/workflows  tests, then image build and push to GHCR
deploy/            systemd user service
```

## License

[MIT](LICENSE). Original work © stasnocap; hls.js is Apache-2.0.
