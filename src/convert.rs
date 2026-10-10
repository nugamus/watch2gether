//! On-demand remux of formats Safari cannot play (mkv) into cached MP4 files.
//! Video is stream-copied; audio is copied if already AAC, otherwise re-encoded to stereo AAC.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
};

pub fn needs_conversion(rel: &str) -> bool {
    Path::new(rel)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("mkv"))
}

/// Cache key from path + size + mtime, so edited or replaced files get reconverted.
pub fn cache_key(src: &Path, rel: &str) -> Option<String> {
    let meta = std::fs::metadata(src).ok()?;
    let mtime = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let h = Sha256::digest(format!("{rel}|{}|{mtime}", meta.len()));
    Some(h[..16].iter().map(|b| format!("{b:02x}")).collect())
}

pub fn cache_file(cache: &Path, key: &str) -> PathBuf {
    cache.join(format!("{key}.mp4"))
}

/// Remove half-written conversions left by a crash or restart.
pub fn clean_partials(cache: &Path) {
    let Ok(rd) = std::fs::read_dir(cache) else { return };
    for e in rd.flatten() {
        if e.path().extension().is_some_and(|x| x == "part") {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

#[derive(Deserialize)]
struct Probe {
    #[serde(default)]
    streams: Vec<ProbeStream>,
    format: Option<ProbeFormat>,
}
#[derive(Deserialize)]
struct ProbeStream {
    codec_type: Option<String>,
    codec_name: Option<String>,
}
#[derive(Deserialize)]
struct ProbeFormat {
    duration: Option<String>,
}

async fn probe(src: &Path) -> Result<Probe, String> {
    let out = Command::new("ffprobe")
        .args(["-v", "error", "-show_entries", "stream=codec_type,codec_name:format=duration", "-of", "json"])
        .arg(src)
        .output()
        .await
        .map_err(|e| format!("could not run ffprobe ({e}); is ffmpeg installed?"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let reason = err.lines().last().unwrap_or("").rsplit(": ").next().unwrap_or("").trim();
        return Err(format!("unreadable video ({reason})"));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())
}

/// Convert `src` to `out` (atomically, via a `.part` file), reporting percent done.
pub async fn run(src: &Path, out: &Path, progress: impl Fn(u8)) -> Result<(), String> {
    let info = probe(src).await?;
    let codec = |kind: &str| {
        info.streams
            .iter()
            .find(|s| s.codec_type.as_deref() == Some(kind))
            .and_then(|s| s.codec_name.clone())
    };
    let Some(vcodec) = codec("video") else {
        return Err("no video stream found".into());
    };
    let acodec = codec("audio");
    let duration: f64 = info
        .format
        .and_then(|f| f.duration)
        .and_then(|d| d.parse().ok())
        .unwrap_or(0.0);

    let tmp = out.with_extension("part");
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-nostdin", "-y", "-v", "error", "-progress", "pipe:1", "-i"])
        .arg(src)
        .args(["-map", "0:v:0", "-map", "0:a:0?", "-c:v", "copy"]);
    if vcodec == "hevc" {
        cmd.args(["-tag:v", "hvc1"]); // Safari only accepts HEVC in MP4 under the hvc1 tag
    }
    if acodec.as_deref() == Some("aac") {
        cmd.args(["-c:a", "copy"]);
    } else {
        cmd.args(["-c:a", "aac", "-b:a", "192k", "-ac", "2"]);
    }
    cmd.args(["-movflags", "+faststart", "-f", "mp4"])
        .arg(&tmp)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("could not run ffmpeg ({e}); is it installed?"))?;
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut last = 0u8;
    while let Ok(Some(line)) = lines.next_line().await {
        let Some(us) = line.strip_prefix("out_time_us=").and_then(|v| v.parse::<f64>().ok()) else {
            continue;
        };
        if duration > 0.0 {
            let pct = (us / 1e6 / duration * 100.0).clamp(0.0, 99.0) as u8;
            if pct != last {
                last = pct;
                progress(pct);
            }
        }
    }
    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut err).await;
    }
    let status = child.wait().await.map_err(|e| e.to_string())?;
    if !status.success() {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("ffmpeg failed: {}", err.trim()));
    }
    std::fs::rename(&tmp, out).map_err(|e| e.to_string())
}
