use std::{
    collections::{HashMap, HashSet},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Extension, Form, Json, Router,
    body::Body,
    extract::{
        DefaultBodyLimit, Multipart, Path as UrlPath, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::broadcast;
use tower::ServiceExt;
use tower_http::{compression::CompressionLayer, services::ServeFile};

mod convert;
mod store;
mod subs;

use store::Store;

#[derive(Parser)]
#[command(about = "Watch a local video together, in sync")]
struct Args {
    /// Directory containing videos to share
    #[arg(default_value = ".")]
    dir: PathBuf,
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// Host password (prefer the W2G_PASSWORD env var: it stays out of `ps`)
    #[arg(long, env = "W2G_PASSWORD", hide_env_values = true)]
    password: Option<String>,
    /// Where prepared (HLS) copies are kept [default: ~/.cache/watch2gether]
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// Where share links, sessions and added subtitles are kept [default: ~/.local/share/watch2gether]
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Make everyone who can reach the port a host
    #[arg(long, conflicts_with = "password")]
    no_auth: bool,
    /// Prepare videos ahead of time, and new ones once they finish copying in
    #[arg(long, env = "W2G_PRECACHE", value_enum, default_value = "off")]
    precache: Precache,
}

struct Auth {
    password_digest: [u8; 32],
    /// Host cookie value, derived from the password so restarts keep sessions valid.
    token: String,
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

impl Auth {
    fn new(password: &str) -> Self {
        let token = convert::hex(&Sha256::digest(format!("w2g-session:{password}")));
        Auth {
            password_digest: digest(password),
            token,
        }
    }
}

/// Who is making a request. Guests can watch, control playback and chat; only the host sees the
/// library, picks videos, adds subtitles and manages links.
#[derive(Clone, Debug, PartialEq)]
enum Role {
    Host,
    Guest { sid: String },
}

/// What is being played. Guests only ever learn the `nonce` URL, never the library path.
enum Media {
    /// A file browsers can play as-is.
    Direct(PathBuf),
    /// A prepared HLS folder in the cache.
    Hls(PathBuf),
}

struct SubTrack {
    id: String,
    name: String,
    lang: Option<String>,
    path: PathBuf,
}

struct Current {
    /// Library path, so a change of quality can prepare the same video again.
    rel: String,
    /// Stable id of the video (also keys its subtitles and offset in the store).
    vid: String,
    title: String,
    /// Random per session/video: media URLs are `/m/<nonce>/...`, so old ones die on every switch.
    nonce: String,
    media: Media,
    /// Prepared 720p copy of an HLS video, offered as a second quality.
    lite: Option<PathBuf>,
    subs: Vec<SubTrack>,
    skipped: Vec<String>,
    offset: f64,
}

impl Current {
    /// The file behind `/m/<nonce>/<rest>`, if it belongs to the current video.
    fn resolve(&self, nonce: &str, rest: &str) -> Option<PathBuf> {
        if !bool::from(self.nonce.as_bytes().ct_eq(nonce.as_bytes())) {
            return None;
        }
        if let Some(id) = rest.strip_prefix("sub/") {
            return self
                .subs
                .iter()
                .find(|s| s.id == id)
                .map(|s| s.path.clone());
        }
        match &self.media {
            Media::Direct(p) => (rest == "video").then(|| p.clone()),
            Media::Hls(dir) => {
                let (dir, rest) = match (rest.strip_prefix("lite/"), &self.lite) {
                    (Some(r), Some(l)) => (l, r),
                    _ => (dir, rest),
                };
                let rel = Path::new(rest);
                let safe =
                    !rest.is_empty() && rel.components().all(|c| matches!(c, Component::Normal(_)));
                safe.then(|| dir.join(rel))
            }
        }
    }

    fn out(&self) -> MediaOut {
        let base = format!("/m/{}/", self.nonce);
        MediaOut {
            vid: self.vid.clone(),
            title: self.title.clone(),
            lite: self.lite.is_some(),
            src: match self.media {
                Media::Direct(_) => format!("{base}video"),
                Media::Hls(_) => format!("{base}master.m3u8"),
            },
            subs: self
                .subs
                .iter()
                .map(|s| SubOut {
                    id: s.id.clone(),
                    // Same name twice (an embedded "English" and an English .srt): say where each is from.
                    name: if self.subs.iter().filter(|o| o.name == s.name).count() > 1 {
                        let from = match s.id.as_bytes()[0] {
                            b'e' => "embedded",
                            b'x' => "file",
                            _ => "added",
                        };
                        format!("{} ({from})", s.name)
                    } else {
                        s.name.clone()
                    },
                    lang: s.lang.clone(),
                    src: format!("{base}sub/{}", s.id),
                })
                .collect(),
            skipped: self.skipped.clone(),
        }
    }
}

/// Authoritative playback state; position is extrapolated while playing.
struct Room {
    current: Option<Current>,
    /// Library path being prepared; playback is paused until it is ready.
    preparing: Option<String>,
    playing: bool,
    pos: f64,
    at: Instant,
    /// Everyone connected.
    clients: HashSet<u64>,
    /// Display names viewers chose.
    names: HashMap<u64, String>,
    /// Last 50 chat messages (not persisted).
    chat: std::collections::VecDeque<ChatMsg>,
    /// When each viewer last reacted, to limit reactions to one a second.
    reacted: HashMap<u64, Instant>,
    /// Viewers who have not confirmed they are ready (stalled, still loading, or just seeked).
    buffering: HashSet<u64>,
    /// Playback was paused because of `buffering` and should resume once it clears.
    resume: bool,
    /// Bumped on every deliberate position change (seek, new video) so clients can tell those
    /// apart from the room merely pausing to wait for someone.
    seq: u64,
    /// How the host wants videos delivered (all off = the original behaviour).
    mode: Mode,
}

/// Room settings for viewers on slow or distant connections.
#[derive(Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
struct Mode {
    /// Also offer a 720p ~1.3 Mbit/s copy; each viewer picks (or lets the player pick).
    #[serde(default)]
    lite: bool,
    /// Bigger buffers and stricter "ready" checks for high-latency viewers.
    #[serde(default)]
    slow: bool,
}

impl Room {
    fn new() -> Room {
        Room {
            current: None,
            preparing: None,
            playing: false,
            pos: 0.0,
            at: Instant::now(),
            clients: HashSet::new(),
            names: HashMap::new(),
            chat: Default::default(),
            reacted: HashMap::new(),
            buffering: HashSet::new(),
            resume: false,
            seq: 0,
            mode: Mode::default(),
        }
    }

    fn buffering(&mut self, id: u64) {
        self.buffering.insert(id);
        if self.playing {
            let p = self.position();
            self.set(false, p);
            self.resume = true;
        }
    }

    /// Play was requested: start now if everyone is ready, otherwise start as soon as they are.
    fn play(&mut self, t: f64) {
        if self.buffering.is_empty() {
            self.resume = false;
            self.set(true, t);
        } else {
            self.resume = true;
            self.set(false, t);
        }
    }

    /// `t` is the stalled viewer's own position. The room resumes from the earliest one so the
    /// slow viewer never has to jump forward over a bad connection.
    fn ready(&mut self, id: u64, t: f64, seq: u64) {
        if seq != self.seq {
            return; // confirmation for a position that has since been replaced
        }
        if self.buffering.remove(&id) && self.resume && t < self.pos {
            self.pos = t.max(0.0);
        }
        self.try_resume();
    }

    /// Resume if we paused for buffering and nobody is stalled any more.
    fn try_resume(&mut self) -> bool {
        if self.resume && self.buffering.is_empty() {
            self.resume = false;
            let p = self.pos;
            self.set(true, p);
            return true;
        }
        false
    }

    fn clear_hold(&mut self) {
        self.buffering.clear();
        self.resume = false;
    }

    fn position(&self) -> f64 {
        if self.playing {
            self.pos + self.at.elapsed().as_secs_f64()
        } else {
            self.pos
        }
    }

    fn set(&mut self, playing: bool, pos: f64) {
        self.playing = playing;
        self.pos = pos.max(0.0);
        self.at = Instant::now();
    }

    /// Start over at 0:00 with `current` (or nothing), waiting for everyone to load it.
    fn reset(&mut self, current: Option<Current>) {
        self.current = current;
        self.preparing = None;
        self.seq += 1;
        self.clear_hold();
        if self.current.is_some() {
            self.buffering = self.clients.clone();
        }
        self.set(false, 0.0);
    }

    fn snapshot(&self, origin: u64) -> String {
        let mut waiting: Vec<String> = self
            .buffering
            .iter()
            .map(|id| {
                self.names
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| "someone".into())
            })
            .collect();
        waiting.sort();
        serde_json::to_string(&Out::State {
            playing: self.playing,
            t: self.position(),
            media: self.current.as_ref().map(Current::out),
            preparing: self.preparing.as_deref().map(title_of),
            offset: self.current.as_ref().map_or(0.0, |c| c.offset),
            waiting,
            seq: self.seq,
            origin,
            mode: self.mode,
        })
        .unwrap()
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum In {
    Play {
        t: f64,
    },
    Pause {
        t: f64,
    },
    Seek {
        t: f64,
    },
    Buffering {
        /// Why the viewer thinks it is not ready, for the log.
        #[serde(default)]
        why: String,
    },
    Ready {
        t: f64,
        seq: u64,
    },
    /// Escape hatch: stop waiting for whoever is stalled.
    Force,
    /// This viewer's browser cannot play the video; tell everyone why the room is stuck.
    Failed {
        msg: String,
    },
    Chat {
        text: String,
    },
    React {
        r: String,
    },
    // Host only:
    Mode {
        #[serde(flatten)]
        mode: Mode,
    },
    Select {
        path: String,
    },
    End,
    /// Subtitle delay in seconds, for everyone.
    Offset {
        v: f64,
    },
    /// Remove a subtitle the host added.
    Subremove {
        id: String,
    },
}

#[derive(Serialize)]
struct MediaOut {
    vid: String,
    /// A 720p quality is offered next to the original.
    lite: bool,
    title: String,
    src: String,
    subs: Vec<SubOut>,
    skipped: Vec<String>,
}

#[derive(Serialize)]
struct SubOut {
    id: String,
    name: String,
    lang: Option<String>,
    src: String,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Out {
    State {
        playing: bool,
        t: f64,
        media: Option<MediaOut>,
        /// Title of the video being prepared.
        preparing: Option<String>,
        /// Subtitle delay in seconds, set by the host for everyone.
        offset: f64,
        /// Names of the viewers the room is waiting for.
        waiting: Vec<String>,
        seq: u64,
        origin: u64,
        mode: Mode,
    },
    Progress {
        pct: u8,
    },
    Error {
        msg: String,
    },
    Viewers {
        n: usize,
    },
    Welcome {
        host: bool,
        build: &'static str,
        /// Recent chat, so a (re)joining viewer sees the conversation.
        chat: Vec<ChatMsg>,
    },
    Chat(ChatMsg),
    React {
        name: String,
        r: String,
    },
}

#[derive(Serialize, Clone)]
struct ChatMsg {
    name: String,
    text: String,
}

/// The reactions viewers can send.
const REACTIONS: [&str; 6] = ["♥", "ha", "wow", "!", "?", "zzz"];

struct App {
    auth: Option<Auth>,
    /// Serializes login attempts so guessing is limited to ~1/sec.
    login_lock: tokio::sync::Mutex<()>,
    root: PathBuf,
    cache: PathBuf,
    store: Store,
    encoder: convert::Encoder,
    /// Cache keys with a conversion in flight.
    jobs: Mutex<HashSet<String>>,
    room: Mutex<Room>,
    tx: broadcast::Sender<String>,
    next_id: Mutex<u64>,
}

type Shared = Arc<App>;

/// Which build this is (set by CI: run number, commit, date), so it is clear what is deployed.
const BUILD: &str = match option_env!("W2G_BUILD") {
    Some(b) => b,
    None => "dev",
};

fn default_dir(env: &str, home_sub: &str) -> PathBuf {
    std::env::var_os(env)
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(home_sub)))
        .unwrap_or_else(std::env::temp_dir)
        .join("watch2gether")
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("watch2gether build {BUILD}");
    let root = match args.dir.canonicalize() {
        Ok(p) if p.is_dir() => p,
        _ => {
            eprintln!("not a directory: {}", args.dir.display());
            std::process::exit(1);
        }
    };
    let auth = match (&args.password, args.no_auth) {
        (Some(p), _) if !p.is_empty() => Some(Auth::new(p)),
        (_, true) => None,
        _ => {
            eprintln!(
                "set a password (W2G_PASSWORD or --password), or pass --no-auth to disable the check"
            );
            std::process::exit(1);
        }
    };
    let cache = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_dir("XDG_CACHE_HOME", ".cache"));
    let data = args
        .data_dir
        .clone()
        .unwrap_or_else(|| default_dir("XDG_DATA_HOME", ".local/share"));
    if let Err(e) = std::fs::create_dir_all(&cache) {
        eprintln!("cannot create cache dir {}: {e}", cache.display());
        std::process::exit(1);
    }
    let store = Store::open(&data).unwrap_or_else(|e| {
        eprintln!("cannot use data dir {}: {e}", data.display());
        std::process::exit(1);
    });
    convert::clean_partials(&cache);
    let encoder = convert::detect_encoder().await;
    println!("video encoder for transcodes: {encoder:?}");
    let app = new_app(root.clone(), cache.clone(), store, auth, encoder);

    // Periodic state broadcast so clients can correct drift.
    let tick = app.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(2));
        loop {
            iv.tick().await;
            let (send, msg) = {
                let r = tick.room.lock().unwrap();
                (r.playing, r.snapshot(0))
            };
            if send {
                let _ = tick.tx.send(msg);
            }
        }
    });

    if args.precache != Precache::Off {
        tokio::spawn(precache(app.clone(), args.precache));
    }

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!(
        "Sharing {} on http://{addr} (cache: {}, data: {})",
        root.display(),
        cache.display(),
        data.display()
    );
    axum::serve(listener, router(app)).await.unwrap();
}

fn new_app(
    root: PathBuf,
    cache: PathBuf,
    store: Store,
    auth: Option<Auth>,
    encoder: convert::Encoder,
) -> Shared {
    let (tx, _) = broadcast::channel(64);
    Arc::new(App {
        auth,
        login_lock: tokio::sync::Mutex::new(()),
        root,
        cache,
        store,
        encoder,
        jobs: Mutex::new(HashSet::new()),
        room: Mutex::new(Room::new()),
        tx,
        next_id: Mutex::new(1),
    })
}

fn router(app: Shared) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../static/index.html")) }),
        )
        .route("/ws", get(ws_handler))
        .route(
            "/hls.js",
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "text/javascript"),
                        (header::CACHE_CONTROL, "max-age=86400"),
                    ],
                    include_str!("../static/hls.min.js"),
                )
            }),
        )
        .route("/m/{nonce}/{*rest}", get(media))
        .route("/api/files", get(list_files))
        .route("/api/links", get(list_links).post(create_link))
        .route("/api/links/{id}", axum::routing::delete(revoke_link))
        .route("/api/links/{id}/regenerate", post(regenerate_link))
        .route(
            "/api/subs",
            post(add_sub).layer(DefaultBodyLimit::max(subs::MAX_BYTES + 64 * 1024)),
        )
        .route("/login", get(login_page).post(login_post))
        .route("/r/{token}", get(redeem))
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        // Gzip the page, hls.js (620 KB -> ~190 KB) and subtitles for slow links. Video is already
        // compressed, and partial (range) responses must not be.
        .layer(CompressionLayer::new().compress_when(
            |status: StatusCode,
             _: axum::http::Version,
             h: &HeaderMap,
             _: &axum::http::Extensions| {
                let ct = h
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                status == StatusCode::OK && (ct.starts_with("text/") || ct.contains("javascript"))
            },
        ))
        .with_state(app)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(app): State<Shared>,
    Extension(role): Extension<Role>,
) -> impl IntoResponse {
    ws.on_upgrade(move |s| client(s, app, role))
}

fn broadcast_viewers(app: &App) {
    let n = app.room.lock().unwrap().clients.len();
    app.send(&Out::Viewers { n });
}

async fn client(socket: WebSocket, app: Shared, role: Role) {
    let id = {
        let mut n = app.next_id.lock().unwrap();
        *n += 1;
        *n
    };
    let mut rx = app.tx.subscribe();
    let (mut sink, mut stream) = socket.split();

    let name = viewer_name(&app, &role);
    // A newcomer counts as not ready until they confirm, so a playing room pauses for them.
    let (first, chat) = {
        let mut r = app.room.lock().unwrap();
        r.clients.insert(id);
        r.names.insert(id, name);
        if r.current.is_some() {
            r.buffering(id);
        }
        (r.snapshot(0), r.chat.iter().cloned().collect())
    };
    let welcome = serde_json::to_string(&Out::Welcome {
        host: role == Role::Host,
        build: BUILD,
        chat,
    })
    .unwrap();
    let _ = sink.send(Message::text(welcome)).await;
    let _ = sink.send(Message::text(first.clone())).await;
    let _ = app.tx.send(first);
    broadcast_viewers(&app);

    // Pings keep a quiet (paused) connection alive: reverse proxies such as nginx drop a
    // WebSocket after 60 s without traffic.
    let mut send_task = tokio::spawn(async move {
        let mut ping = tokio::time::interval(Duration::from_secs(25));
        loop {
            let out = tokio::select! {
                msg = rx.recv() => match msg {
                    Ok(msg) => Message::text(msg),
                    Err(_) => break,
                },
                _ = ping.tick() => Message::Ping(Default::default()),
            };
            if sink.send(out).await.is_err() {
                break;
            }
        }
    });

    let (app2, role2) = (app.clone(), role.clone());
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Text(text) = msg else { continue };
            if let Ok(cmd) = serde_json::from_str::<In>(&text) {
                handle(&app2, id, &role2, cmd).await;
            }
        }
    });

    // A guest whose link was revoked or expired is disconnected within a few seconds.
    let (app3, role3) = (app.clone(), role.clone());
    let mut auth_task = tokio::spawn(async move {
        let Role::Guest { sid } = role3 else {
            return std::future::pending().await;
        };
        let mut iv = tokio::time::interval(Duration::from_secs(3));
        while app3.store.guest(&sid).is_some() {
            iv.tick().await;
        }
    });

    tokio::select! {
        _ = &mut send_task => {}
        _ = &mut recv_task => {}
        _ = &mut auth_task => {}
    }
    send_task.abort();
    recv_task.abort();
    auth_task.abort();
    // A viewer who left can no longer hold the room up.
    let snap = {
        let mut r = app.room.lock().unwrap();
        r.clients.remove(&id);
        r.names.remove(&id);
        r.reacted.remove(&id);
        r.buffering.remove(&id).then(|| {
            r.try_resume();
            r.snapshot(0)
        })
    };
    if let Some(snap) = snap {
        let _ = app.tx.send(snap);
    }
    broadcast_viewers(&app);
}

/// What everyone sees a viewer as: the name the host gave their share link, or "Host".
/// Nobody has to type a name.
fn viewer_name(app: &App, role: &Role) -> String {
    let Role::Guest { sid } = role else {
        return "Host".into();
    };
    let link = app.store.guest(sid);
    let name = app.store.read(|d| {
        d.links
            .iter()
            .find(|l| Some(&l.id) == link.as_ref())
            .map(|l| l.name.clone())
    });
    name.filter(|n| !n.trim().is_empty())
        .map(|n| n.trim().chars().take(24).collect())
        .unwrap_or_else(|| "Guest".into())
}

/// Apply one WebSocket command from viewer `id`. Host-only commands from guests are ignored.
async fn handle(app: &Shared, id: u64, role: &Role, cmd: In) {
    let host = *role == Role::Host;
    let out = match cmd {
        In::Select { path } if host => {
            if is_video(&app.root, &path) {
                select(app, path, id).await;
            }
            return;
        }
        In::End if host => {
            let mut r = app.room.lock().unwrap();
            r.reset(None);
            r.snapshot(id)
        }
        In::Offset { v } if host => {
            let v = (v.clamp(-600.0, 600.0) * 10.0).round() / 10.0;
            let mut r = app.room.lock().unwrap();
            let Some(c) = r.current.as_mut() else { return };
            c.offset = v;
            let vid = c.vid.clone();
            app.store.update(|d| d.offsets.insert(vid, v));
            r.snapshot(0)
        }
        In::Subremove { id } if host => {
            let Some(vid) = app
                .room
                .lock()
                .unwrap()
                .current
                .as_ref()
                .map(|c| c.vid.clone())
            else {
                return;
            };
            let removed = app.store.update(|d| {
                let list = d.subs.entry(vid.clone()).or_default();
                let before = list.len();
                list.retain(|u| format!("u{}", u.id) != id);
                before != list.len()
            });
            if removed {
                let _ = std::fs::remove_file(app.upload_path(&vid, &id[1..]));
            }
            app.refresh_uploads()
        }
        In::Mode { mode } if host => {
            let (rel, again) = {
                let mut r = app.room.lock().unwrap();
                let old = std::mem::replace(&mut r.mode, mode);
                let cur = r.current.as_ref();
                // Offering (or no longer offering) the lite copy needs the video prepared again.
                let again = cur.is_some() && old.lite != mode.lite;
                (cur.map(|c| c.rel.clone()), again)
            };
            if let (Some(rel), true) = (rel, again) {
                select(app, rel, id).await;
                return;
            }
            app.room.lock().unwrap().snapshot(id)
        }
        In::Select { .. }
        | In::End
        | In::Offset { .. }
        | In::Subremove { .. }
        | In::Mode { .. } => return,
        In::Failed { msg } => {
            let name = app
                .room
                .lock()
                .unwrap()
                .names
                .get(&id)
                .cloned()
                .unwrap_or_else(|| "A viewer".into());
            let msg: String = msg.chars().take(200).collect();
            app.send(&Out::Error {
                msg: format!("{name} cannot play this video: {msg}"),
            });
            return;
        }
        In::Chat { text } => {
            let text: String = text
                .trim()
                .chars()
                .filter(|c| !c.is_control())
                .take(500)
                .collect();
            let mut r = app.room.lock().unwrap();
            let Some(name) = r.names.get(&id).cloned() else {
                return;
            };
            if text.is_empty() {
                return;
            }
            let msg = ChatMsg { name, text };
            if r.chat.len() == 50 {
                r.chat.pop_front();
            }
            r.chat.push_back(msg.clone());
            serde_json::to_string(&Out::Chat(msg)).unwrap()
        }
        In::React { r: reaction } => {
            let mut r = app.room.lock().unwrap();
            let Some(name) = r.names.get(&id).cloned() else {
                return;
            };
            let recent = r
                .reacted
                .get(&id)
                .is_some_and(|t| t.elapsed() < Duration::from_secs(1));
            if recent || !REACTIONS.contains(&reaction.as_str()) {
                return;
            }
            r.reacted.insert(id, Instant::now());
            serde_json::to_string(&Out::React { name, r: reaction }).unwrap()
        }
        cmd => {
            let mut r = app.room.lock().unwrap();
            if r.current.is_none() {
                return;
            }
            // One line per sync event: when the room keeps waiting, the log says who and why.
            let who = r
                .names
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("viewer {id}"));
            match &cmd {
                In::Buffering { why } => {
                    let why: String = why.chars().filter(|c| !c.is_control()).take(120).collect();
                    println!("{who}: not ready ({why})")
                }
                In::Ready { t, .. } => println!("{who}: ready at {t:.1}"),
                In::Play { t } => println!("{who}: play at {t:.1}"),
                In::Pause { t } => println!("{who}: pause at {t:.1}"),
                In::Seek { t } => println!("{who}: seek to {t:.1}"),
                In::Force => println!("{who}: resume anyway"),
                _ => {}
            }
            match cmd {
                In::Buffering { .. } => r.buffering(id),
                In::Ready { t, seq } => r.ready(id, t, seq),
                In::Force => {
                    r.buffering.clear();
                    r.try_resume();
                }
                In::Play { t } => r.play(t),
                In::Pause { t } => {
                    r.resume = false;
                    r.set(false, t)
                }
                In::Seek { t } => {
                    // Freeze the room until every viewer confirms the new position is loaded.
                    r.seq += 1;
                    if r.playing {
                        r.resume = true;
                    }
                    r.buffering = r.clients.clone();
                    r.set(false, t)
                }
                _ => unreachable!(),
            }
            r.snapshot(id)
        }
    };
    let _ = app.tx.send(out);
}

impl App {
    fn send(&self, out: &Out) {
        let _ = self.tx.send(serde_json::to_string(out).unwrap());
    }

    fn upload_path(&self, vid: &str, id: &str) -> PathBuf {
        self.store
            .dir()
            .join("subs")
            .join(vid)
            .join(format!("{id}.vtt"))
    }

    fn upload_tracks(&self, vid: &str) -> Vec<SubTrack> {
        let list = self
            .store
            .read(|d| d.subs.get(vid).cloned().unwrap_or_default());
        list.into_iter()
            .map(|u| SubTrack {
                path: self.upload_path(vid, &u.id),
                id: format!("u{}", u.id),
                name: u.name,
                lang: u.lang,
            })
            .collect()
    }

    /// Re-read the host's added subtitles for the current video; returns the new state.
    fn refresh_uploads(&self) -> String {
        let mut r = self.room.lock().unwrap();
        if let Some(c) = r.current.as_mut() {
            c.subs.retain(|s| !s.id.starts_with('u'));
            c.subs.extend(self.upload_tracks(&c.vid));
        }
        r.snapshot(0)
    }

    /// A sidecar subtitle converted to WebVTT, cached until the file changes.
    async fn sidecar_vtt(&self, path: &Path) -> Result<PathBuf, String> {
        let key = convert::cache_key(path, &path.to_string_lossy()).ok_or("unreadable")?;
        let out = self.cache.join("subs").join(format!("{key}.vtt"));
        if !out.exists() {
            if std::fs::metadata(path).map_err(|e| e.to_string())?.len() > subs::MAX_BYTES as u64 {
                return Err("larger than 5 MB".into());
            }
            let vtt = subs::to_vtt(&std::fs::read(path).map_err(|e| e.to_string())?).await?;
            std::fs::create_dir_all(out.parent().unwrap()).map_err(|e| e.to_string())?;
            std::fs::write(&out, vtt).map_err(|e| e.to_string())?;
        }
        Ok(out)
    }

    /// Make `rel` the room's video, paused at 0:00, and tell everyone.
    async fn activate(&self, rel: String, media: Media, meta: convert::Meta, origin: u64) {
        let vid = video_id(&rel);
        let mut subs = Vec::new();
        let sidecars = subs::sidecars(&self.root.join(&rel));
        if let Media::Hls(dir) = &media {
            for (i, s) in meta.subs.iter().enumerate() {
                subs.push(SubTrack {
                    id: format!("e{i}"),
                    name: s.name.clone(),
                    lang: s.lang.clone(),
                    path: dir.join(&s.file),
                });
            }
        }
        for (i, sc) in sidecars.into_iter().enumerate() {
            match self.sidecar_vtt(&sc.path).await {
                Ok(path) => subs.push(SubTrack {
                    id: format!("x{i}"),
                    name: sc.name,
                    lang: sc.lang,
                    path,
                }),
                Err(e) => eprintln!("skipping subtitle {}: {e}", sc.path.display()),
            }
        }
        subs.extend(self.upload_tracks(&vid));
        let offset = self
            .store
            .read(|d| d.offsets.get(&vid).copied().unwrap_or(0.0));
        let want_lite = self.room.lock().unwrap().mode.lite;
        let lite = match &media {
            Media::Hls(dir) if want_lite => {
                Some(lite_dir(dir)).filter(|l| l.join("master.m3u8").exists())
            }
            _ => None,
        };
        let cur = Current {
            lite,
            title: title_of(&rel),
            rel: rel.clone(),
            vid,
            nonce: store::random_token(16),
            media,
            subs,
            skipped: meta.skipped,
            offset,
        };
        let msg = {
            let mut r = self.room.lock().unwrap();
            r.reset(Some(cur));
            r.snapshot(origin)
        };
        let _ = self.tx.send(msg);
    }
}

/// File name without folders or extension: all guests get to see of the library.
fn title_of(rel: &str) -> String {
    Path::new(rel)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn video_id(rel: &str) -> String {
    convert::hex(&Sha256::digest(format!("vid:{rel}"))[..8])
}

async fn select(app: &Shared, path: String, id: u64) {
    let src = app.root.join(&path);
    let info = match convert::probe(&src).await {
        Ok(i) => i,
        Err(e) => {
            return app.send(&Out::Error {
                msg: format!("Cannot play {}: {e}", title_of(&path)),
            });
        }
    };
    let mode = app.room.lock().unwrap().mode;
    if !mode.lite && convert::direct_play(&path, &info) {
        app.activate(path, Media::Direct(src), convert::Meta::default(), id)
            .await;
        return;
    }
    let Some(key) = convert::cache_key(&src, &path) else {
        return;
    };
    let dir = app.cache.join(&key);
    let need_lite = mode.lite && !lite_dir(&dir).join("master.m3u8").exists();
    if let Some(meta) = convert::load_meta(&dir).filter(|_| !need_lite) {
        app.activate(path, Media::Hls(dir), meta, id).await;
        return;
    }

    // Pause everyone and show progress while ffmpeg works in the background.
    let msg = {
        let mut r = app.room.lock().unwrap();
        r.preparing = Some(path.clone());
        let p = r.position();
        r.set(false, p);
        r.snapshot(id)
    };
    let _ = app.tx.send(msg);
    // If it is already being prepared (e.g. by precache), that job switches the room when done.
    tokio::spawn(prepare(app.clone(), path, src, dir, need_lite));
}

/// Prepare the HLS folder `dir` (and its lite copy) for library file `path`. Switches the room to
/// it if the room is waiting for it. Returns false if ffmpeg failed.
async fn prepare(app: Shared, path: String, src: PathBuf, dir: PathBuf, need_lite: bool) -> bool {
    let key = dir
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    if !app.jobs.lock().unwrap().insert(key.clone()) {
        return true;
    }
    let wanted = |app: &App| app.room.lock().unwrap().preparing.as_deref() == Some(path.as_str());
    let a2 = app.clone();
    let progress = |pct| {
        if wanted(&a2) {
            a2.send(&Out::Progress { pct })
        }
    };
    let res = async {
        let meta = match convert::load_meta(&dir) {
            Some(m) => m,
            None => convert::run(&src, &dir, app.encoder, false, &progress).await?,
        };
        if need_lite {
            convert::run(&src, &lite_dir(&dir), app.encoder, true, &progress).await?;
        }
        Ok::<_, String>(meta)
    }
    .await;
    app.jobs.lock().unwrap().remove(&key);
    let still_wanted = wanted(&app);
    match res {
        Ok(meta) if still_wanted => {
            let lite_missing =
                app.room.lock().unwrap().mode.lite && !lite_dir(&dir).join("master.m3u8").exists();
            if lite_missing {
                // The room asked for a lite copy while this job (precache) only made the original.
                let lite = convert::run(&src, &lite_dir(&dir), app.encoder, true, &progress).await;
                if let Err(e) = lite {
                    eprintln!("lite copy of {path} failed: {e}");
                }
            }
            app.activate(path, Media::Hls(dir), meta, 0).await;
            true
        }
        Ok(_) => true,
        Err(msg) => {
            eprintln!("conversion of {path} failed: {msg}");
            if still_wanted {
                let snap = {
                    let mut r = app.room.lock().unwrap();
                    r.preparing = None;
                    r.snapshot(0)
                };
                let _ = app.tx.send(snap);
                app.send(&Out::Error {
                    msg: format!("Could not prepare {}: {msg}", title_of(&path)),
                });
            }
            false
        }
    }
}

/// What the server prepares ahead of time, so picking a video starts it at once.
#[derive(Clone, Copy, PartialEq, Debug, clap::ValueEnum)]
enum Precache {
    Off,
    /// HLS copies of every video browsers can't play as-is.
    Original,
    /// HLS copies of every video plus their 720p lite copies.
    Lite,
}

/// Library files whose size and mtime were the same last round: anything still being copied in
/// keeps changing, so it is only prepared once it has been left alone for a whole round.
type Seen = HashMap<String, (u64, std::time::SystemTime)>;

/// Prepare every video in the library, one at a time, and keep doing it as files are added.
async fn precache(app: Shared, what: Precache) {
    let (mut seen, mut failed) = (Seen::new(), HashSet::new());
    loop {
        precache_round(&app, what, &mut seen, &mut failed).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

async fn precache_round(
    app: &Shared,
    what: Precache,
    seen: &mut Seen,
    failed: &mut HashSet<String>,
) {
    let root = app.root.clone();
    let files = tokio::task::spawn_blocking(move || {
        let mut v = Vec::new();
        scan(&root, &root, &mut v);
        v
    })
    .await
    .unwrap_or_default();
    seen.retain(|rel, _| files.contains(rel));
    for rel in files {
        let src = app.root.join(&rel);
        let Some(sig) = std::fs::metadata(&src)
            .ok()
            .and_then(|m| Some((m.len(), m.modified().ok()?)))
        else {
            continue;
        };
        if seen.insert(rel.clone(), sig) != Some(sig) {
            continue; // new or still changing
        }
        let Some(key) = convert::cache_key(&src, &rel) else {
            continue;
        };
        let dir = app.cache.join(&key);
        let need_lite = what == Precache::Lite && !lite_dir(&dir).join("master.m3u8").exists();
        if failed.contains(&key) || (convert::load_meta(&dir).is_some() && !need_lite) {
            continue;
        }
        if what == Precache::Original
            && convert::probe(&src)
                .await
                .is_ok_and(|p| convert::direct_play(&rel, &p))
        {
            continue;
        }
        // Stay out of the way of a conversion the room is waiting for.
        while !app.jobs.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        println!("precaching {rel}");
        if !prepare(app.clone(), rel, src, dir, need_lite).await {
            failed.insert(key);
        }
    }
}

/// Where the 720p copy of the HLS folder `dir` lives.
fn lite_dir(dir: &Path) -> PathBuf {
    let mut name = dir.file_name().unwrap_or_default().to_os_string();
    name.push("-lite");
    dir.with_file_name(name)
}

/// Serves the current video's files, and nothing else, to anyone in the room.
async fn media(
    State(app): State<Shared>,
    UrlPath((nonce, rest)): UrlPath<(String, String)>,
    req: Request,
) -> Response {
    let (target, lite) = {
        let r = app.room.lock().unwrap();
        let c = r.current.as_ref();
        let target = c.and_then(|c| c.resolve(&nonce, &rest));
        (
            target,
            c.and_then(|c| c.lite.clone())
                .filter(|_| rest == "master.m3u8"),
        )
    };
    if let (Some(p), Some(l)) = (&target, lite) {
        let read = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
        let body = convert::combined_master(&read(p), &read(&l.join("master.m3u8")));
        return (
            [
                (header::CONTENT_TYPE, "application/vnd.apple.mpegurl"),
                (header::CACHE_CONTROL, "private, max-age=86400, immutable"),
            ],
            body,
        )
            .into_response();
    }
    match target {
        Some(p) => match ServeFile::new(p).oneshot(req).await {
            Ok(mut res) => {
                // A nonce URL always names the same bytes, so browsers can keep what they have
                // instead of downloading it again after a seek. `private`: never in a shared cache.
                if res.status().is_success() {
                    res.headers_mut().insert(
                        header::CACHE_CONTROL,
                        header::HeaderValue::from_static("private, max-age=86400, immutable"),
                    );
                }
                res.map(Body::new)
            }
            Err(e) => match e {},
        },
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

const EXTS: [&str; 5] = ["mp4", "webm", "mkv", "m4v", "mov"];

fn has_video_ext(p: &Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| EXTS.contains(&e.to_ascii_lowercase().as_str()))
}

/// True if `rel` resolves to a video file inside `root` (blocks `..` and symlink escapes).
fn is_video(root: &Path, rel: &str) -> bool {
    root.join(rel)
        .canonicalize()
        .is_ok_and(|p| p.starts_with(root) && p.is_file() && has_video_ext(&p))
}

fn scan(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_dir() {
            scan(root, &p, out);
        } else if ft.is_file()
            && has_video_ext(&p)
            && let Ok(rel) = p.strip_prefix(root)
        {
            out.push(rel.to_string_lossy().into_owned());
        }
    }
}

#[derive(Serialize)]
struct FileOut {
    path: String,
    vid: String,
}

async fn list_files(State(app): State<Shared>) -> Json<Vec<FileOut>> {
    let root = app.root.clone();
    let mut files = tokio::task::spawn_blocking(move || {
        let mut v = Vec::new();
        scan(&root, &root, &mut v);
        v
    })
    .await
    .unwrap_or_default();
    files.sort();
    Json(
        files
            .into_iter()
            .map(|path| FileOut {
                vid: video_id(&path),
                path,
            })
            .collect(),
    )
}

/// Host adds a subtitle to the current video, from an uploaded file or a URL. It is converted to
/// WebVTT and kept in the data folder, so it is there again next time the video is played.
async fn add_sub(State(app): State<Shared>, mut form: Multipart) -> Response {
    let bad = |m: String| (StatusCode::BAD_REQUEST, m).into_response();
    let (mut file, mut url) = (None, None);
    while let Ok(Some(field)) = form.next_field().await {
        match field.name() {
            Some("file") => {
                let name = field.file_name().unwrap_or("").to_string();
                match field.bytes().await {
                    Ok(b) if !b.is_empty() => file = Some((name, b.to_vec())),
                    Ok(_) => {}
                    Err(_) => return bad("The file is larger than 5 MB".into()),
                }
            }
            Some("url") => url = field.text().await.ok().filter(|u| !u.trim().is_empty()),
            _ => {}
        }
    }
    let (source, bytes) = match (file, url) {
        (Some(f), _) => f,
        (None, Some(u)) => match subs::fetch(&u).await {
            Ok(b) => (u.rsplit('/').next().unwrap_or("").to_string(), b),
            Err(e) => return bad(format!("Could not download it: {e}")),
        },
        (None, None) => return bad("Choose a file or paste a link".into()),
    };
    let vtt = match subs::to_vtt(&bytes).await {
        Ok(v) => v,
        Err(e) => return bad(format!("Could not use it: {e}")),
    };
    let Some(vid) = app
        .room
        .lock()
        .unwrap()
        .current
        .as_ref()
        .map(|c| c.vid.clone())
    else {
        return (StatusCode::CONFLICT, "Start a video first").into_response();
    };
    let (lang, name) = subs::upload_label(&source, &vtt);
    let id = store::random_token(6);
    let path = app.upload_path(&vid, &id);
    if let Err(e) =
        std::fs::create_dir_all(path.parent().unwrap()).and_then(|_| std::fs::write(&path, vtt))
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not save it: {e}"),
        )
            .into_response();
    }
    app.store.update(|d| {
        d.subs
            .entry(vid)
            .or_default()
            .push(store::Upload { id, name, lang })
    });
    let _ = app.tx.send(app.refresh_uploads());
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Serialize)]
struct LinkOut {
    id: String,
    name: String,
    created: u64,
    expires: Option<u64>,
    /// Only present right after creating or regenerating: the token is not stored.
    path: Option<String>,
}

fn link_out(l: &store::Link, token: Option<String>) -> LinkOut {
    LinkOut {
        id: l.id.clone(),
        name: l.name.clone(),
        created: l.created,
        expires: l.expires,
        path: token.map(|t| format!("/r/{t}")),
    }
}

async fn list_links(State(app): State<Shared>) -> Json<Vec<LinkOut>> {
    Json(
        app.store
            .read(|d| d.links.iter().map(|l| link_out(l, None)).collect()),
    )
}

#[derive(Deserialize)]
struct NewLink {
    name: String,
    /// Days until the link stops working; none = never.
    days: Option<u32>,
}

async fn create_link(State(app): State<Shared>, Json(n): Json<NewLink>) -> Json<LinkOut> {
    let name: String = n.name.trim().chars().take(40).collect();
    let expires = n
        .days
        .filter(|d| *d > 0)
        .map(|d| store::now() + d as u64 * 86400);
    let (link, token) = app
        .store
        .create_link(if name.is_empty() { "Link".into() } else { name }, expires);
    Json(link_out(&link, Some(token)))
}

async fn regenerate_link(State(app): State<Shared>, UrlPath(id): UrlPath<String>) -> Response {
    let Some(token) = app.store.regenerate(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let link = app
        .store
        .read(|d| d.links.iter().find(|l| l.id == id).cloned());
    match link {
        Some(l) => Json(link_out(&l, Some(token))).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn revoke_link(State(app): State<Shared>, UrlPath(id): UrlPath<String>) -> StatusCode {
    if app.store.revoke(&id) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

fn cookie<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .find_map(|c| c.trim().strip_prefix(name)?.strip_prefix('='))
}

fn role_of(app: &App, req: &Request) -> Option<Role> {
    let Some(auth) = &app.auth else {
        return Some(Role::Host);
    };
    if cookie(req, "w2g").is_some_and(|c| bool::from(c.as_bytes().ct_eq(auth.token.as_bytes()))) {
        return Some(Role::Host);
    }
    let sid = cookie(req, "w2g_g")?;
    app.store.guest(sid).map(|_| Role::Guest {
        sid: sid.to_string(),
    })
}

/// Works out who is asking and keeps guests away from everything host-only. Enforced here for
/// every route, so hiding things in the page is only cosmetic.
async fn guard(State(app): State<Shared>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if path == "/login" || path.starts_with("/r/") {
        return next.run(req).await;
    }
    match role_of(&app, &req) {
        Some(Role::Guest { .. }) if path.starts_with("/api/") => {
            StatusCode::FORBIDDEN.into_response()
        }
        Some(role) => {
            req.extensions_mut().insert(role);
            next.run(req).await
        }
        None if path == "/" => {
            let stale_link = cookie(&req, "w2g_g").is_some();
            Redirect::to(if stale_link { "/login?e=2" } else { "/login" }).into_response()
        }
        None => StatusCode::UNAUTHORIZED.into_response(),
    }
}

/// `; Secure` when the browser reached us over HTTPS (through the reverse proxy).
fn secure(headers: &HeaderMap) -> &'static str {
    let https = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("https"));
    if https { "; Secure" } else { "" }
}

/// A share link: swap the token for a session cookie and move to the clean URL, so the token
/// doesn't stay in the address bar or history. Lax (not Strict) because the guest arrives from a
/// chat app, and a Strict cookie would be withheld on that cross-site redirect chain.
async fn redeem(
    State(app): State<Shared>,
    UrlPath(token): UrlPath<String>,
    headers: HeaderMap,
) -> Response {
    match app.store.redeem(&token) {
        Some(sid) => {
            let cookie = format!(
                "w2g_g={sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{}",
                store::SESSION_SECS,
                secure(&headers)
            );
            ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
        }
        None => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            (
                StatusCode::NOT_FOUND,
                "This link doesn't work any more. Ask the host for a new one.",
            )
                .into_response()
        }
    }
}

async fn login_page() -> Html<&'static str> {
    Html(include_str!("../static/login.html"))
}

#[derive(Deserialize)]
struct LoginForm {
    password: String,
}

async fn login_post(
    State(app): State<Shared>,
    headers: HeaderMap,
    Form(f): Form<LoginForm>,
) -> Response {
    let Some(auth) = &app.auth else {
        return Redirect::to("/").into_response();
    };
    let _guard = app.login_lock.lock().await;
    if bool::from(digest(&f.password).ct_eq(&auth.password_digest)) {
        let cookie = format!(
            "w2g={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000{}",
            auth.token,
            secure(&headers)
        );
        ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
    } else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Redirect::to("/login?e=1").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;

    struct T {
        app: Shared,
        _dirs: [tempfile::TempDir; 3],
    }

    fn setup() -> T {
        let dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let root = dirs[0].path().canonicalize().unwrap();
        std::fs::create_dir(root.join("shows")).unwrap();
        std::fs::write(root.join("a.mp4"), b"movie a").unwrap();
        std::fs::write(root.join("shows/b.mp4"), b"movie b").unwrap();
        let store = Store::open(dirs[2].path()).unwrap();
        let app = new_app(
            root,
            dirs[1].path().to_path_buf(),
            store,
            Some(Auth::new("pw")),
            convert::Encoder::X264,
        );
        T { app, _dirs: dirs }
    }

    async fn get(app: &Shared, path: &str, cookie: Option<&str>) -> Response {
        let mut req = HttpRequest::get(path);
        if let Some(c) = cookie {
            req = req.header(header::COOKIE, c);
        }
        router(app.clone())
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    fn set_cookie(res: &Response) -> String {
        let c = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        c.split(';').next().unwrap().to_string()
    }

    async fn body(res: Response) -> String {
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
    }

    fn host_cookie(app: &Shared) -> String {
        format!("w2g={}", app.auth.as_ref().unwrap().token)
    }

    async fn guest_cookie(app: &Shared) -> (String, String, String) {
        let (link, token) = app.store.create_link("Lin".into(), None);
        let res = get(app, &format!("/r/{token}"), None).await;
        assert_eq!(res.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            res.headers()[header::LOCATION],
            "/",
            "token leaves the address bar"
        );
        (set_cookie(&res), link.id, token)
    }

    #[tokio::test]
    async fn strangers_are_kept_out() {
        let t = setup();
        let res = get(&t.app, "/", None).await;
        assert_eq!(res.headers()[header::LOCATION], "/login");
        assert_eq!(
            get(&t.app, "/api/files", None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&t.app, "/ws", None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&t.app, "/r/not-a-token", None).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(&t.app, "/api/files", Some("w2g=forged")).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn host_sees_library() {
        let t = setup();
        let res = get(&t.app, "/api/files", Some(&host_cookie(&t.app))).await;
        assert_eq!(res.status(), StatusCode::OK);
        let b = body(res).await;
        assert!(b.contains("shows/b.mp4") && b.contains("a.mp4"), "{b}");
    }

    #[tokio::test]
    async fn guest_link_gives_password_free_access_but_no_library() {
        let t = setup();
        let (guest, _, _) = guest_cookie(&t.app).await;
        assert!(guest.starts_with("w2g_g="));
        assert_eq!(
            get(&t.app, "/", Some(&guest)).await.status(),
            StatusCode::OK
        );
        for p in ["/api/files", "/api/links"] {
            assert_eq!(
                get(&t.app, p, Some(&guest)).await.status(),
                StatusCode::FORBIDDEN,
                "{p}"
            );
        }
        let req = HttpRequest::post("/api/links")
            .header(header::COOKIE, &guest)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"name":"evil"}"#))
            .unwrap();
        let res = router(t.app.clone()).oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        // Raw library paths are not served at all any more.
        assert_eq!(
            get(&t.app, "/video/a.mp4", Some(&guest)).await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn guest_cannot_pick_videos_or_end_sessions() {
        let t = setup();
        let guest = Role::Guest { sid: "x".into() };
        handle(
            &t.app,
            5,
            &guest,
            In::Select {
                path: "a.mp4".into(),
            },
        )
        .await;
        assert!(t.app.room.lock().unwrap().current.is_none());

        let root = t.app.root.clone();
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        handle(&t.app, 5, &guest, In::End).await;
        assert!(t.app.room.lock().unwrap().current.is_some());
        handle(&t.app, 1, &Role::Host, In::End).await;
        assert!(t.app.room.lock().unwrap().current.is_none());
    }

    #[tokio::test]
    async fn media_urls_only_reach_the_current_video() {
        let t = setup();
        let (guest, _, _) = guest_cookie(&t.app).await;
        let g = Some(guest.as_str());
        let root = t.app.root.clone();
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        let nonce = t
            .app
            .room
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .nonce
            .clone();
        assert!(nonce.len() >= 20);

        let res = get(&t.app, &format!("/m/{nonce}/video"), g).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body(res).await, "movie a");
        for bad in [
            "/m/wrong/video",
            "/m/{n}/shows/b.mp4",
            "/m/{n}/../shows/b.mp4",
            "/m/{n}/sub/x",
        ] {
            let url = bad.replace("{n}", &nonce);
            assert_eq!(
                get(&t.app, &url, g).await.status(),
                StatusCode::NOT_FOUND,
                "{url}"
            );
        }

        // HLS folders: files inside are served, nothing outside.
        let hls = t.app.cache.join("k");
        std::fs::create_dir_all(hls.join("v")).unwrap();
        std::fs::write(hls.join("v/seg00001.m4s"), b"seg").unwrap();
        std::fs::write(t.app.cache.join("secret"), b"no").unwrap();
        t.app
            .activate("shows/b.mp4".into(), Media::Hls(hls), Default::default(), 0)
            .await;
        let n2 = t
            .app
            .room
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .nonce
            .clone();
        assert_eq!(
            get(&t.app, &format!("/m/{n2}/v/seg00001.m4s"), g)
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            get(&t.app, &format!("/m/{n2}/../secret"), g).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(&t.app, &format!("/m/{n2}/v/..%2f..%2fsecret"), g)
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        // The old video's URL died with the switch.
        assert_eq!(
            get(&t.app, &format!("/m/{nonce}/video"), g).await.status(),
            StatusCode::NOT_FOUND
        );

        // State sent to guests carries the nonce URL and the title, never the library path.
        let snap = t.app.room.lock().unwrap().snapshot(0);
        assert!(
            snap.contains(&format!("/m/{n2}/master.m3u8")) && !snap.contains("shows/"),
            "{snap}"
        );
    }

    #[tokio::test]
    async fn revoked_links_and_their_sessions_stop_working() {
        let t = setup();
        let (guest, id, token) = guest_cookie(&t.app).await;
        let host = host_cookie(&t.app);
        let req = HttpRequest::delete(format!("/api/links/{id}"))
            .header(header::COOKIE, &host)
            .body(Body::empty());
        let res = router(t.app.clone()).oneshot(req.unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            get(&t.app, &format!("/r/{token}"), None).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(&t.app, "/ws", Some(&guest)).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&t.app, "/", Some(&guest)).await.headers()[header::LOCATION],
            "/login?e=2"
        );
    }

    fn upload(cookie: &str, filename: &str, bytes: &[u8]) -> HttpRequest<Body> {
        let head = format!(
            "--XX\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\r\n"
        );
        let mut b = head.into_bytes();
        b.extend_from_slice(bytes);
        b.extend_from_slice(b"\r\n--XX--\r\n");
        HttpRequest::post("/api/subs")
            .header(header::COOKIE, cookie)
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=XX")
            .body(Body::from(b))
            .unwrap()
    }

    #[tokio::test]
    async fn host_uploads_gbk_subtitles_guest_cannot() {
        if std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .is_err()
        {
            return eprintln!("ffmpeg not installed; skipping");
        }
        let t = setup();
        let (guest, _, _) = guest_cookie(&t.app).await;
        let root = t.app.root.clone();
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        let srt = "1
00:00:01,000 --> 00:00:04,000
你好，这是中文字幕。我们一起看电影吧。
";
        let (gbk, _, _) = encoding_rs::GBK.encode(srt);

        let res = router(t.app.clone())
            .oneshot(upload(&guest, "x.srt", &gbk))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        let res = router(t.app.clone())
            .oneshot(upload(&host_cookie(&t.app), "movie.srt", &gbk))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT, "{}", body(res).await);
        let (nonce, sub) = {
            let r = t.app.room.lock().unwrap();
            let c = r.current.as_ref().unwrap();
            (
                c.nonce.clone(),
                c.subs
                    .iter()
                    .map(|s| (s.id.clone(), s.name.clone()))
                    .next()
                    .unwrap(),
            )
        };
        assert_eq!(sub.1, "Chinese", "language guessed from the text");
        let vtt = body(get(&t.app, &format!("/m/{nonce}/sub/{}", sub.0), Some(&guest)).await).await;
        assert!(vtt.contains("你好，这是中文字幕。"), "{vtt}");

        // Kept with the video for next time.
        t.app.room.lock().unwrap().reset(None);
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        assert_eq!(
            t.app
                .room
                .lock()
                .unwrap()
                .current
                .as_ref()
                .unwrap()
                .subs
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn page_and_hls_js_are_gzipped_video_is_not() {
        let t = setup();
        let host = host_cookie(&t.app);
        let req = |path: &str| {
            HttpRequest::get(path)
                .header(header::COOKIE, &host)
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap()
        };
        for path in ["/hls.js", "/"] {
            let res = router(t.app.clone()).oneshot(req(path)).await.unwrap();
            assert_eq!(
                res.headers()
                    .get(header::CONTENT_ENCODING)
                    .map(|v| v.to_str().unwrap()),
                Some("gzip"),
                "{path}"
            );
        }
        let root = t.app.root.clone();
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        let nonce = t
            .app
            .room
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .nonce
            .clone();
        let res = router(t.app.clone())
            .oneshot(req(&format!("/m/{nonce}/video")))
            .await
            .unwrap();
        assert!(res.headers().get(header::CONTENT_ENCODING).is_none());
    }

    #[tokio::test]
    async fn slow_link_modes_are_host_only_and_media_is_cacheable() {
        let t = setup();
        let cmd: In = serde_json::from_str(r#"{"type":"mode","lite":false,"slow":true}"#).unwrap();
        handle(&t.app, 5, &Role::Guest { sid: "x".into() }, cmd).await;
        assert!(
            !t.app.room.lock().unwrap().mode.slow,
            "guests can't change the mode"
        );
        let cmd: In = serde_json::from_str(r#"{"type":"mode","slow":true}"#).unwrap();
        handle(&t.app, 1, &Role::Host, cmd).await;
        assert!(t.app.room.lock().unwrap().mode.slow);
        assert!(
            t.app
                .room
                .lock()
                .unwrap()
                .snapshot(0)
                .contains(r#""slow":true"#)
        );

        let root = t.app.root.clone();
        t.app
            .activate(
                "a.mp4".into(),
                Media::Direct(root.join("a.mp4")),
                Default::default(),
                0,
            )
            .await;
        let nonce = t
            .app
            .room
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .nonce
            .clone();
        let res = get(
            &t.app,
            &format!("/m/{nonce}/video"),
            Some(&host_cookie(&t.app)),
        )
        .await;
        assert_eq!(
            res.headers()[header::CACHE_CONTROL],
            "private, max-age=86400, immutable"
        );
    }

    #[tokio::test]
    async fn precache_waits_for_copies_to_finish_then_prepares() {
        if std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .is_err()
        {
            return eprintln!("ffmpeg not installed; skipping");
        }
        let t = setup();
        let src = t.app.root.join("new.mkv");
        let ok = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-f", "lavfi", "-i", "testsrc2=s=640x360:d=1"])
            .args(["-c:v", "libx264"])
            .arg(&src)
            .status()
            .unwrap();
        assert!(ok.success());
        let key = convert::cache_key(&src, "new.mkv").unwrap();
        let (mut seen, mut failed) = (Seen::new(), HashSet::new());
        precache_round(&t.app, Precache::Lite, &mut seen, &mut failed).await;
        assert!(
            !t.app.cache.join(&key).exists(),
            "not touched on first sight: it may still be copying"
        );
        precache_round(&t.app, Precache::Lite, &mut seen, &mut failed).await;
        assert!(convert::load_meta(&t.app.cache.join(&key)).is_some());
        assert!(
            lite_dir(&t.app.cache.join(&key))
                .join("master.m3u8")
                .exists()
        );
        // a.mp4 / b.mp4 are not real videos: they fail once and are not retried every round.
        assert_eq!(failed.len(), 2);
    }

    #[test]
    fn viewers_are_named_after_their_link() {
        let t = setup();
        let (_, token) = t.app.store.create_link("Lin".into(), None);
        let sid = t.app.store.redeem(&token).unwrap();
        assert_eq!(viewer_name(&t.app, &Role::Guest { sid }), "Lin");
        assert_eq!(viewer_name(&t.app, &Role::Host), "Host");
    }

    #[tokio::test]
    async fn links_survive_a_restart() {
        let t = setup();
        let (guest, _, token) = guest_cookie(&t.app).await;
        // A new app over the same data dir, as after a container restart.
        let data = t._dirs[2].path();
        let app2 = new_app(
            t.app.root.clone(),
            t.app.cache.clone(),
            Store::open(data).unwrap(),
            Some(Auth::new("pw")),
            convert::Encoder::X264,
        );
        assert_eq!(get(&app2, "/", Some(&guest)).await.status(), StatusCode::OK);
        assert_eq!(
            get(&app2, &format!("/r/{token}"), None).await.status(),
            StatusCode::SEE_OTHER
        );
    }

    #[tokio::test]
    async fn host_creates_links_and_sees_the_token_once() {
        let t = setup();
        let host = host_cookie(&t.app);
        let req = HttpRequest::post("/api/links")
            .header(header::COOKIE, &host)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"name":"Lin's link","days":30}"#))
            .unwrap();
        let b = body(router(t.app.clone()).oneshot(req).await.unwrap()).await;
        let made: serde_json::Value = serde_json::from_str(&b).unwrap();
        let path = made["path"].as_str().unwrap();
        assert!(path.starts_with("/r/"));
        assert!(made["expires"].as_u64().unwrap() > store::now());
        let listed = body(get(&t.app, "/api/links", Some(&host)).await).await;
        assert!(
            listed.contains("Lin's link") && !listed.contains(&path[3..]),
            "token never listed"
        );
        assert_eq!(
            get(&t.app, path, None).await.status(),
            StatusCode::SEE_OTHER
        );
    }
}
