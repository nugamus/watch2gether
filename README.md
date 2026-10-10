# watch2gether

Watch videos from your own computer together with friends over the internet, in perfect sync.

watch2gether is a single Rust binary. Point it at a folder of videos, open the link in a browser, and everyone in the room sees the same frame at the same time. Play, pause and seek apply to everyone, and if one viewer's connection can't keep up, the whole room waits for them.

No uploads, no third-party service, nothing to install for viewers: they just open a web page.

## Features

- **Strict sync.** Play, pause, seek and ±10s skips apply to everyone. If one viewer stalls or is still loading, the room freezes until they're ready, then resumes for all from the same position.
- **Your files, streamed directly.** Videos are served straight from disk with HTTP range requests, so seeking is instant and nothing is re-encoded.
- **Plays `.mkv` on iPhone.** Safari can't play Matroska, so `.mkv` files are converted to MP4 once on first play (video copied, audio turned into AAC) and cached. Later plays start instantly.
- **Folder library.** Browse your video folder as a collapsible tree in the page, and pick what to watch together.
- **Password protected.** Viewers log in with a shared password. Sessions last 30 days, and guessing is rate-limited.
- **Works on phones.** Responsive layout with a collapsible library, inline playback on iPhone, and a tap-to-join prompt where the browser blocks autoplay.
- **Small and fast.** One binary with the web page compiled in, built on [axum](https://github.com/tokio-rs/axum) and [tokio](https://tokio.rs).

## Requirements

- [Rust](https://rustup.rs) (edition 2024, so a recent toolchain)
- [ffmpeg](https://ffmpeg.org) with `ffprobe` on your `PATH`, only needed to play `.mkv` files
- A way for your friends to reach your computer: port forwarding, a tunnel such as Cloudflare Tunnel or Tailscale Funnel, or a reverse proxy. Use HTTPS in front of it, because the password is sent in plain text otherwise.

## Quick start

```sh
git clone https://github.com/stasnocap/watch2gether.git
cd watch2gether
W2G_PASSWORD='choose-a-password' cargo run --release -- ~/Videos --port 8080
```

Open `http://localhost:8080`, enter the password, pick a video, and send the link (and password) to your friends.

## Usage

```
watch2gether [OPTIONS] [DIR]
```

| Option | Default | Description |
|---|---|---|
| `DIR` | `.` | Folder of videos to share (searched recursively) |
| `--port <PORT>` | `8080` | Port to listen on |
| `--bind <ADDR>` | `0.0.0.0` | Address to listen on |
| `--password <PW>` / `W2G_PASSWORD` | | Password viewers must enter. Prefer the environment variable, so the password doesn't show up in the process list |
| `--cache-dir <DIR>` | `~/.cache/watch2gether` | Where converted `.mkv` → `.mp4` copies are stored |
| `--no-auth` | | Turn off the password check. Anyone who can reach the port can join |

The server refuses to start without a password unless `--no-auth` is given.

Supported files: `.mp4`, `.m4v`, `.mov`, `.webm` (played as-is) and `.mkv` (converted on first play).

## Running as a systemd service

Install the binary and set up a user service, which runs as you so it can read your videos:

```sh
cargo install --path .

# Password file, readable only by you
echo 'W2G_PASSWORD=choose-a-password' > ~/.config/watch2gether.env
chmod 600 ~/.config/watch2gether.env

mkdir -p ~/.config/systemd/user
cp deploy/watch2gether.service ~/.config/systemd/user/
# Edit the folder and port in ExecStart if needed, then:
systemctl --user daemon-reload
systemctl --user enable --now watch2gether

# Keep it running when you're logged out
loginctl enable-linger "$USER"
```

Logs: `journalctl --user -u watch2gether -f`

## How sync works

The server holds the single source of truth for the room: which video, playing or paused, and the position. Browsers talk to it over a WebSocket.

- Every action (play, pause, seek, picking a video) goes to the server, which broadcasts the new state to everyone.
- While playing, the server re-broadcasts its position every 2 seconds, and any viewer more than 0.6s off is corrected.
- Every viewer tells the server whether they're **ready**, meaning they have enough video buffered at the current position. The room plays only while everyone is ready:
  - when someone stalls for more than 0.7s, everyone pauses;
  - after any seek or new video, everyone must confirm they've loaded the new spot before playback resumes;
  - a viewer who joins or reconnects counts as not ready until they've loaded.
- The room resumes from the slowest viewer's position, so a viewer on a weak connection never has to jump forward into video they haven't downloaded.
- On a viewer who keeps stalling, the buffer required before resuming doubles (3s, 6s, ... up to 30s), so the room pauses less often but for longer.
- A **Resume anyway** button lets anyone stop waiting for a stalled viewer.

## Limitations

- **Bandwidth.** Video is streamed at its original quality. A viewer needs a connection faster than the file's bitrate (roughly file size ÷ duration), otherwise the room will spend a lot of time waiting for them. 4K remuxes can be 15+ Mbit/s.
- **`.mkv` conversion** keeps only the first audio track and drops subtitles. The first play of a large file takes a while (it's a one-time copy). The cache is never cleaned automatically, and each converted file takes about as much space as the original.
- **Codecs.** Conversion changes the container, not the video codec. Very heavy files, such as 4K 10-bit HDR HEVC, may still be too much for some phones.
- **iPhone fullscreen** uses Safari's native player, which can't show anything the page draws on top.
- **One room per server.** Everyone connected shares the same room.

## Project layout

```
src/main.rs        server: CLI, auth, WebSocket sync, room state, file library
src/convert.rs     on-demand mkv → mp4 conversion with ffmpeg
static/index.html  the player page (compiled into the binary)
static/login.html  the login page
deploy/            systemd user service
```

## License

[MIT](LICENSE)
