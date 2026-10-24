use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Form, Json, Router,
    extract::{
        Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

mod convert;

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
    /// Password viewers must enter (prefer the W2G_PASSWORD env var: it stays out of `ps`)
    #[arg(long, env = "W2G_PASSWORD", hide_env_values = true)]
    password: Option<String>,
    /// Where converted (mkv -> mp4) copies are kept [default: ~/.cache/watch2gether]
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// After converting an mkv, keep the MP4 next to it in the library and delete the original mkv
    #[arg(long)]
    delete_source: bool,
    /// Allow anyone who can reach the port to join
    #[arg(long, conflicts_with = "password")]
    no_auth: bool,
}

struct Auth {
    password_digest: [u8; 32],
    /// Session cookie value, derived from the password so restarts keep sessions valid.
    token: String,
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

impl Auth {
    fn new(password: &str) -> Self {
        let token = Sha256::digest(format!("w2g-session:{password}"))
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        Auth { password_digest: digest(password), token }
    }
}

/// Authoritative playback state; position is extrapolated while playing.
struct Room {
    /// Library path of the selected video (for highlighting).
    file: Option<String>,
    /// Cache key of the converted copy to load instead of `file`, if it needed conversion.
    cached: Option<String>,
    /// Library path currently being converted; playback is paused until it is ready.
    preparing: Option<String>,
    playing: bool,
    pos: f64,
    at: Instant,
    /// Everyone connected.
    clients: HashSet<u64>,
    /// Viewers who have not confirmed they are ready (stalled, still loading, or just seeked).
    buffering: HashSet<u64>,
    /// Playback was paused because of `buffering` and should resume once it clears.
    resume: bool,
    /// Bumped on every deliberate position change (seek, new video) so clients can tell those
    /// apart from the room merely pausing to wait for someone.
    seq: u64,
}

impl Room {
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

    fn snapshot(&self, origin: u64) -> String {
        serde_json::to_string(&Out::State {
            playing: self.playing,
            t: self.position(),
            file: self.file.clone(),
            cached: self.cached.clone(),
            preparing: self.preparing.clone(),
            waiting: self.buffering.len(),
            seq: self.seq,
            origin,
        })
        .unwrap()
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum In {
    Play { t: f64 },
    Pause { t: f64 },
    Seek { t: f64 },
    /// `convert` is set once the user has agreed to converting a file that needs it.
    Select {
        path: String,
        #[serde(default)]
        convert: bool,
    },
    Buffering,
    Ready { t: f64, seq: u64 },
    /// Escape hatch: stop waiting for whoever is stalled.
    Force,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Out {
    State {
        playing: bool,
        t: f64,
        file: Option<String>,
        cached: Option<String>,
        preparing: Option<String>,
        waiting: usize,
        seq: u64,
        origin: u64,
    },
    Progress { path: String, pct: u8 },
    /// `path` needs converting first; whoever picked it is asked before anything starts.
    Confirm { path: String, delete_source: bool },
    Error { msg: String },
    Viewers { n: usize },
}

struct App {
    auth: Option<Auth>,
    /// Serializes login attempts so guessing is limited to ~1/sec.
    login_lock: tokio::sync::Mutex<()>,
    root: PathBuf,
    cache: PathBuf,
    delete_source: bool,
    /// Cache keys with a conversion in flight.
    jobs: Mutex<HashSet<String>>,
    room: Mutex<Room>,
    tx: broadcast::Sender<String>,
    viewers: Mutex<usize>,
    next_id: Mutex<u64>,
}

type Shared = Arc<App>;

#[tokio::main]
async fn main() {
    let args = Args::parse();
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
            eprintln!("set a password (W2G_PASSWORD or --password), or pass --no-auth to disable the check");
            std::process::exit(1);
        }
    };
    let cache = args.cache_dir.clone().unwrap_or_else(|| {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
            .unwrap_or_else(std::env::temp_dir)
            .join("watch2gether")
    });
    if let Err(e) = std::fs::create_dir_all(&cache) {
        eprintln!("cannot create cache dir {}: {e}", cache.display());
        std::process::exit(1);
    }
    convert::clean_partials(&cache);
    let (tx, _) = broadcast::channel(64);
    let app = Arc::new(App {
        auth,
        login_lock: tokio::sync::Mutex::new(()),
        root: root.clone(),
        cache: cache.clone(),
        delete_source: args.delete_source,
        jobs: Mutex::new(HashSet::new()),
        room: Mutex::new(Room {
            file: None,
            cached: None,
            preparing: None,
            playing: false,
            pos: 0.0,
            at: Instant::now(),
            clients: HashSet::new(),
            buffering: HashSet::new(),
            resume: false,
            seq: 0,
        }),
        tx,
        viewers: Mutex::new(0),
        next_id: Mutex::new(1),
    });

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

    let router = Router::new()
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        .route("/ws", get(ws_handler))
        .route("/api/files", get(list_files))
        .nest_service("/video", ServeDir::new(&root))
        .nest_service("/cache", ServeDir::new(&cache))
        .route("/login", get(login_page).post(login_post))
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .with_state(app);

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!("Sharing {} on http://{addr} (conversion cache: {})", root.display(), cache.display());
    axum::serve(listener, router).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(app): State<Shared>) -> impl IntoResponse {
    ws.on_upgrade(move |s| client(s, app))
}

fn broadcast_viewers(app: &App) {
    let n = *app.viewers.lock().unwrap();
    let _ = app.tx.send(serde_json::to_string(&Out::Viewers { n }).unwrap());
}

async fn client(socket: WebSocket, app: Shared) {
    let id = {
        let mut n = app.next_id.lock().unwrap();
        *n += 1;
        *n
    };
    *app.viewers.lock().unwrap() += 1;
    let mut rx = app.tx.subscribe();
    let (mut sink, mut stream) = socket.split();

    // A newcomer counts as not ready until they confirm, so a playing room pauses for them.
    let first = {
        let mut r = app.room.lock().unwrap();
        r.clients.insert(id);
        r.buffering(id);
        r.snapshot(0)
    };
    let _ = sink.send(Message::text(first.clone())).await;
    let _ = app.tx.send(first);
    broadcast_viewers(&app);

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    let app2 = app.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Text(text) = msg else { continue };
            let Ok(cmd) = serde_json::from_str::<In>(&text) else { continue };
            if let In::Select { path, convert } = cmd {
                if is_video(&app2.root, &path) {
                    select(&app2, path, id, convert).await;
                }
                continue;
            }
            let out = {
                let mut r = app2.room.lock().unwrap();
                match cmd {
                    In::Select { .. } => unreachable!(),
                    In::Buffering => r.buffering(id),
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
                }
                r.snapshot(id)
            };
            let _ = app2.tx.send(out);
        }
    });

    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
    *app.viewers.lock().unwrap() -= 1;
    broadcast_viewers(&app);
    // A viewer who left can no longer hold the room up.
    let snap = {
        let mut r = app.room.lock().unwrap();
        r.clients.remove(&id);
        r.buffering.remove(&id).then(|| {
            r.try_resume();
            r.snapshot(0)
        })
    };
    if let Some(snap) = snap {
        let _ = app.tx.send(snap);
    }
}

impl App {
    /// Make `path` the room's video, paused at 0:00, and tell everyone.
    fn switch_to(&self, path: String, cached: Option<String>, origin: u64) {
        let msg = {
            let mut r = self.room.lock().unwrap();
            r.file = Some(path);
            r.cached = cached;
            r.preparing = None;
            r.seq += 1;
            r.clear_hold();
            r.buffering = r.clients.clone();
            r.set(false, 0.0);
            r.snapshot(origin)
        };
        let _ = self.tx.send(msg);
    }

    fn send(&self, out: &Out) {
        let _ = self.tx.send(serde_json::to_string(out).unwrap());
    }
}

async fn select(app: &Shared, path: String, id: u64, confirmed: bool) {
    if !convert::needs_conversion(&path) {
        app.switch_to(path, None, id);
        return;
    }
    let src = app.root.join(&path);
    let Some(key) = convert::cache_key(&src, &path) else { return };
    // With delete_source the MP4 is written next to the original (which it then replaces);
    // otherwise it goes to the cache.
    let sibling = app
        .delete_source
        .then(|| std::path::Path::new(&path).with_extension("mp4").to_string_lossy().into_owned());
    let out = match &sibling {
        Some(rel) => app.root.join(rel),
        None => convert::cache_file(&app.cache, &key),
    };
    if out.exists() {
        match sibling {
            Some(rel) => app.switch_to(rel, None, id),
            None => app.switch_to(path, Some(key), id),
        }
        return;
    }
    // Never start a conversion unasked; the picker is prompted and re-sends with `convert`.
    if !confirmed && !app.jobs.lock().unwrap().contains(&key) {
        app.send(&Out::Confirm { path, delete_source: app.delete_source });
        return;
    }

    // Pause everyone and show "preparing" while ffmpeg remuxes in the background.
    let msg = {
        let mut r = app.room.lock().unwrap();
        r.preparing = Some(path.clone());
        let p = r.position();
        r.set(false, p);
        r.snapshot(id)
    };
    let _ = app.tx.send(msg);
    if !app.jobs.lock().unwrap().insert(key.clone()) {
        return; // already converting; its completion handler will switch the room
    }
    let app = app.clone();
    tokio::spawn(async move {
        let p2 = path.clone();
        let a2 = app.clone();
        let res = convert::run(&src, &out, move |pct| {
            a2.send(&Out::Progress { path: p2.clone(), pct })
        })
        .await;
        // Only delete the original once the converted copy has been checked.
        let res = match res {
            Ok(()) if sibling.is_some() => match convert::verify(&src, &out).await {
                Ok(()) => {
                    if let Err(e) = std::fs::remove_file(&src) {
                        eprintln!("converted {path} but could not delete the original: {e}");
                    }
                    Ok(())
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&out);
                    Err(format!("converted copy failed verification, original kept ({e})"))
                }
            },
            other => other,
        };
        app.jobs.lock().unwrap().remove(&key);
        let still_wanted = app.room.lock().unwrap().preparing.as_deref() == Some(path.as_str());
        match res {
            Ok(()) if still_wanted => match sibling {
                Some(rel) => app.switch_to(rel, None, 0),
                None => app.switch_to(path, Some(key), 0),
            },
            Ok(()) => {}
            Err(msg) => {
                eprintln!("conversion of {path} failed: {msg}");
                if still_wanted {
                    let snap = {
                        let mut r = app.room.lock().unwrap();
                        r.preparing = None;
                        r.snapshot(0)
                    };
                    let _ = app.tx.send(snap);
                }
                app.send(&Out::Error { msg: format!("Could not convert {path}: {msg}") });
            }
        }
    });
}

const EXTS: [&str; 5] = ["mp4", "webm", "mkv", "m4v", "mov"];

fn has_video_ext(p: &std::path::Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| EXTS.contains(&e.to_ascii_lowercase().as_str()))
}

/// True if `rel` resolves to a video file inside `root` (blocks `..` and symlink escapes).
fn is_video(root: &std::path::Path, rel: &str) -> bool {
    root.join(rel)
        .canonicalize()
        .is_ok_and(|p| p.starts_with(root) && p.is_file() && has_video_ext(&p))
}

fn scan(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_dir() {
            scan(root, &p, out);
        } else if ft.is_file() && has_video_ext(&p) {
            if let Ok(rel) = p.strip_prefix(root) {
                out.push(rel.to_string_lossy().into_owned());
            }
        }
    }
}

async fn list_files(State(app): State<Shared>) -> Json<Vec<String>> {
    let root = app.root.clone();
    let mut files = tokio::task::spawn_blocking(move || {
        let mut v = Vec::new();
        scan(&root, &root, &mut v);
        v
    })
    .await
    .unwrap_or_default();
    files.sort();
    Json(files)
}

fn has_session(req: &Request, token: &str) -> bool {
    req.headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .any(|c| c.trim().strip_prefix("w2g=") == Some(token))
}

async fn guard(State(app): State<Shared>, req: Request, next: Next) -> Response {
    let Some(auth) = &app.auth else { return next.run(req).await };
    let path = req.uri().path();
    if path == "/login" || has_session(&req, &auth.token) {
        return next.run(req).await;
    }
    if path == "/" {
        Redirect::to("/login").into_response()
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

async fn login_page() -> Html<&'static str> {
    Html(include_str!("../static/login.html"))
}

#[derive(Deserialize)]
struct LoginForm {
    password: String,
}

async fn login_post(State(app): State<Shared>, Form(f): Form<LoginForm>) -> Response {
    let Some(auth) = &app.auth else { return Redirect::to("/").into_response() };
    let _guard = app.login_lock.lock().await;
    if digest(&f.password) == auth.password_digest {
        let cookie = format!(
            "w2g={}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000",
            auth.token
        );
        ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
    } else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Redirect::to("/login?e=1").into_response()
    }
}
