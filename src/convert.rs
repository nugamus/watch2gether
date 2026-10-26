//! Preparing videos for browser playback.
//!
//! Files every browser can already play (H.264 MP4 with one AAC track, VP9/AV1 WebM) are served
//! as-is. Everything else is turned once into an HLS folder in the cache: the video is copied if
//! it is 8-bit H.264 and transcoded to H.264 otherwise (VAAPI / Quick Sync when available, libx264
//! if not), every audio track becomes its own rendition, and text subtitles are extracted as
//! WebVTT. HLS is what lets each viewer pick an audio track in Firefox, Chrome and Safari alike.

use std::{collections::VecDeque, path::Path, process::Stdio};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

use crate::subs::lang_name;

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
    Some(hex(&h[..16]))
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

/// Remove half-written conversions left by a crash or restart.
pub fn clean_partials(cache: &Path) {
    let Ok(rd) = std::fs::read_dir(cache) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "part") {
            let _ = std::fs::remove_dir_all(&p).or_else(|_| std::fs::remove_file(&p));
        }
    }
}

#[derive(Deserialize, Default)]
pub struct Probe {
    #[serde(default)]
    pub streams: Vec<Stream>,
    #[serde(default)]
    pub format: Format,
}

#[derive(Deserialize, Default)]
pub struct Stream {
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub codec_type: String,
    #[serde(default)]
    pub codec_name: String,
    pub pix_fmt: Option<String>,
    pub profile: Option<String>,
    pub level: Option<i64>,
    pub channels: Option<u32>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bit_rate: Option<String>,
    #[serde(default)]
    pub tags: Tags,
    #[serde(default)]
    pub disposition: Disposition,
}

#[derive(Deserialize, Default)]
pub struct Tags {
    pub language: Option<String>,
    pub title: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct Disposition {
    #[serde(default)]
    pub default: u8,
    #[serde(default)]
    pub attached_pic: u8,
}

#[derive(Deserialize, Default)]
pub struct Format {
    pub duration: Option<String>,
    pub bit_rate: Option<String>,
}

impl Probe {
    pub fn of<'a>(&'a self, kind: &'a str) -> impl Iterator<Item = &'a Stream> {
        self.streams.iter().filter(move |s| s.codec_type == kind)
    }
    /// The main video stream (not cover art).
    pub fn video(&self) -> Option<&Stream> {
        self.of("video").find(|s| s.disposition.attached_pic == 0)
    }
    pub fn duration(&self) -> f64 {
        self.format
            .duration
            .as_deref()
            .and_then(|d| d.parse().ok())
            .unwrap_or(0.0)
    }
}

impl Stream {
    fn lang(&self) -> Option<String> {
        self.tags
            .language
            .clone()
            .filter(|l| !l.is_empty() && l != "und")
    }
    /// Human-readable track name: its title, else its language, else `fallback`.
    fn name(&self, fallback: String) -> String {
        self.tags
            .title
            .clone()
            .filter(|t| !t.trim().is_empty())
            .or_else(|| {
                self.lang()
                    .map(|l| lang_name(&l).map(str::to_owned).unwrap_or(l))
            })
            .unwrap_or(fallback)
    }
}

pub async fn probe(src: &Path) -> Result<Probe, String> {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,pix_fmt,profile,level,channels,width,height,bit_rate\
             :stream_tags=language,title:stream_disposition=default,attached_pic:format=duration,bit_rate",
            "-of",
            "json",
        ])
        .arg(src)
        .output()
        .await
        .map_err(|e| format!("could not run ffprobe ({e}); is ffmpeg installed?"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let reason = err
            .lines()
            .last()
            .unwrap_or("")
            .rsplit(": ")
            .next()
            .unwrap_or("")
            .trim();
        return Err(format!("unreadable video ({reason})"));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())
}

const TEXT_SUBS: [&str; 6] = ["subrip", "ass", "ssa", "webvtt", "mov_text", "text"];

fn is_text_sub(s: &Stream) -> bool {
    TEXT_SUBS.contains(&s.codec_name.as_str())
}

/// 8-bit H.264 is the one video format every browser decodes, so it can be copied untouched.
fn is_plain_h264(s: &Stream) -> bool {
    s.codec_name == "h264" && matches!(s.pix_fmt.as_deref(), Some("yuv420p" | "yuvj420p"))
}

fn is_plain_aac(s: &Stream) -> bool {
    s.codec_name == "aac" && s.profile.as_deref() == Some("LC") && s.channels.unwrap_or(2) <= 2
}

/// True if browsers can play the file as-is, with nothing (audio tracks, subtitles) lost.
pub fn direct_play(rel: &str, p: &Probe) -> bool {
    let ext = Path::new(rel)
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let Some(v) = p.video() else { return false };
    let audio: Vec<&str> = p.of("audio").map(|s| s.codec_name.as_str()).collect();
    if audio.len() > 1 || p.of("subtitle").any(is_text_sub) {
        return false;
    }
    let a = audio.first().copied();
    match ext.as_str() {
        "mp4" | "m4v" | "mov" => is_plain_h264(v) && matches!(a, None | Some("aac")),
        "webm" => {
            matches!(v.codec_name.as_str(), "vp8" | "vp9" | "av1")
                && matches!(a, None | Some("opus" | "vorbis"))
        }
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Encoder {
    Vaapi,
    X264,
}

const DRI: &str = "/dev/dri/renderD128";

/// Use the Intel GPU (VAAPI, i.e. Quick Sync) if a one-frame test encode works on it.
pub async fn detect_encoder() -> Encoder {
    if !Path::new(DRI).exists() {
        return Encoder::X264;
    }
    let ok = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-init_hw_device",
            &format!("vaapi=va:{DRI}"),
        ])
        .args(["-f", "lavfi", "-i", "testsrc2=s=256x144:d=0.2"])
        .args(["-vf", "format=nv12,hwupload", "-filter_hw_device", "va"])
        .args(["-c:v", "h264_vaapi", "-f", "null", "-"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success());
    if ok { Encoder::Vaapi } else { Encoder::X264 }
}

/// What a finished HLS folder contains besides the playlists.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Meta {
    pub subs: Vec<EmbeddedSub>,
    /// Image-based subtitle tracks (PGS, VobSub) that browsers cannot show.
    pub skipped: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EmbeddedSub {
    pub file: String,
    pub name: String,
    pub lang: Option<String>,
}

pub fn load_meta(dir: &Path) -> Option<Meta> {
    serde_json::from_slice(&std::fs::read(dir.join("meta.json")).ok()?).ok()
}

/// Build the HLS folder `dir` for `src`, reporting percent done. Falls back to libx264 if the GPU
/// encoder fails on this particular file.
pub async fn run(
    src: &Path,
    dir: &Path,
    enc: Encoder,
    progress: impl Fn(u8),
) -> Result<Meta, String> {
    let p = probe(src).await?;
    match build(src, dir, &p, enc, &progress).await {
        Err(e) if enc == Encoder::Vaapi => {
            eprintln!("VAAPI encode failed, retrying with libx264: {e}");
            build(src, dir, &p, Encoder::X264, &progress).await
        }
        r => r,
    }
}

/// Output size for a transcode: same aspect, at most 1080 lines, even dimensions.
fn fit_1080(v: &Stream) -> (u32, u32) {
    let (w, h) = (v.width.unwrap_or(1920), v.height.unwrap_or(1080));
    if h <= 1080 {
        return (w & !1, h & !1);
    }
    (((w as u64 * 1080 / h as u64) as u32) & !1, 1080)
}

/// Transcode bitrate (bit/s): H.264 needs roughly 1.5x what HEVC does for the same quality.
fn target_rate(p: &Probe, v: &Stream) -> u64 {
    let parse = |s: &Option<String>| s.as_deref().and_then(|b| b.parse::<u64>().ok());
    let src = parse(&v.bit_rate)
        .or_else(|| parse(&p.format.bit_rate).map(|b| b.saturating_sub(300_000)))
        .unwrap_or(4_000_000);
    (src * 3 / 2).clamp(1_500_000, 5_000_000)
}

fn hls_out(cmd: &mut Command, dir: &Path) {
    cmd.args(["-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod"])
        .args([
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
        ])
        .arg("-hls_segment_filename")
        .arg(dir.join("seg%05d.m4s"))
        .arg(dir.join("index.m3u8"));
}

async fn build(
    src: &Path,
    dir: &Path,
    p: &Probe,
    enc: Encoder,
    progress: &impl Fn(u8),
) -> Result<Meta, String> {
    let v = p.video().ok_or("no video stream found")?;
    let tmp = dir.with_extension("part");
    let _ = std::fs::remove_dir_all(&tmp);
    let audio: Vec<&Stream> = p.of("audio").collect();
    for d in std::iter::once("v".to_string()).chain((0..audio.len()).map(|i| format!("a{i}"))) {
        std::fs::create_dir_all(tmp.join(d)).map_err(|e| format!("cannot write to cache: {e}"))?;
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-nostdin", "-y", "-v", "error", "-progress", "pipe:1"]);
    let transcode = !is_plain_h264(v);
    if transcode && enc == Encoder::Vaapi {
        cmd.args([
            "-hwaccel",
            "vaapi",
            "-hwaccel_device",
            DRI,
            "-hwaccel_output_format",
            "vaapi",
        ]);
    }
    cmd.arg("-i").arg(src);

    cmd.args(["-map", &format!("0:{}", v.index)]);
    if transcode {
        let (w, h) = fit_1080(v);
        let rate = target_rate(p, v);
        let (b, max, buf) = (
            rate.to_string(),
            (rate * 3 / 2).to_string(),
            (rate * 2).to_string(),
        );
        match enc {
            Encoder::Vaapi => cmd
                .args(["-vf", &format!("scale_vaapi=w={w}:h={h}:format=nv12")])
                .args([
                    "-c:v",
                    "h264_vaapi",
                    "-profile:v",
                    "high",
                    "-rc_mode",
                    "VBR",
                ]),
            Encoder::X264 => cmd
                .args(["-vf", &format!("scale={w}:{h}"), "-pix_fmt", "yuv420p"])
                .args([
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-profile:v",
                    "high",
                ]),
        };
        cmd.args([
            "-level:v", "4.1", "-b:v", &b, "-maxrate", &max, "-bufsize", &buf, "-g", "120",
        ]);
    } else {
        cmd.args(["-c:v", "copy"]);
    }
    hls_out(&mut cmd, &tmp.join("v"));

    for (i, a) in audio.iter().enumerate() {
        cmd.args(["-map", &format!("0:{}", a.index)]);
        if is_plain_aac(a) {
            cmd.args(["-c:a", "copy"]);
        } else {
            cmd.args(["-c:a", "aac", "-ac", "2", "-b:a", "160k"]);
        }
        hls_out(&mut cmd, &tmp.join(format!("a{i}")));
    }

    let mut meta = Meta::default();
    for (i, s) in p.of("subtitle").enumerate() {
        let name = s.name(format!("Subtitle {}", i + 1));
        if !is_text_sub(s) {
            meta.skipped.push(format!("{name} ({})", s.codec_name));
            continue;
        }
        let file = format!("s{i}.vtt");
        cmd.args([
            "-map",
            &format!("0:{}", s.index),
            "-c:s",
            "webvtt",
            "-f",
            "webvtt",
        ])
        .arg(tmp.join(&file));
        meta.subs.push(EmbeddedSub {
            file,
            name,
            lang: s.lang(),
        });
    }

    run_ffmpeg(cmd, p.duration(), progress).await?;

    // What the browser will receive: the source stream if copied, else High@4.1 at the new size.
    let out = if transcode {
        let (w, h) = fit_1080(v);
        Stream {
            width: Some(w),
            height: Some(h),
            profile: Some("High".into()),
            level: Some(41),
            ..Default::default()
        }
    } else {
        Stream {
            width: v.width,
            height: v.height,
            profile: v.profile.clone(),
            level: v.level,
            ..Default::default()
        }
    };
    let master = master_playlist(&tmp, &out, &audio, p.duration())?;
    std::fs::write(tmp.join("master.m3u8"), master).map_err(|e| e.to_string())?;
    std::fs::write(tmp.join("meta.json"), serde_json::to_vec(&meta).unwrap())
        .map_err(|e| e.to_string())?;
    let _ = std::fs::remove_dir_all(dir);
    std::fs::rename(&tmp, dir).map_err(|e| e.to_string())?;
    Ok(meta)
}

/// Run ffmpeg, turning its `-progress` output into percent. stderr is drained concurrently:
/// reading it only after stdout closes can deadlock once ffmpeg fills the pipe with warnings.
async fn run_ffmpeg(mut cmd: Command, duration: f64, progress: &impl Fn(u8)) -> Result<(), String> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("could not run ffmpeg ({e}); is it installed?"))?;
    let stderr = child.stderr.take().unwrap();
    let errors = tokio::spawn(async move {
        let mut tail = VecDeque::new();
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            if tail.len() == 20 {
                tail.pop_front();
            }
            tail.push_back(l);
        }
        Vec::from(tail).join("\n")
    });
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut last = 0u8;
    while let Ok(Some(line)) = lines.next_line().await {
        let Some(us) = line
            .strip_prefix("out_time_us=")
            .and_then(|v| v.parse::<f64>().ok())
        else {
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
    let status = child.wait().await.map_err(|e| e.to_string())?;
    let err = errors.await.unwrap_or_default();
    if !status.success() {
        return Err(format!("ffmpeg failed: {}", err.trim()));
    }
    Ok(())
}

/// RFC 6381 codec string for an H.264 stream, e.g. `avc1.640028`.
fn avc_codec(v: &Stream) -> String {
    let profile = match v.profile.as_deref() {
        Some("High") => "6400",
        Some("Main") => "4d40",
        Some("Baseline" | "Constrained Baseline") => "42e0",
        _ => "6400",
    };
    let level = v.level.filter(|l| (10..=62).contains(l)).unwrap_or(41);
    format!("avc1.{profile}{level:02x}")
}

fn master_playlist(
    tmp: &Path,
    v: &Stream,
    audio: &[&Stream],
    duration: f64,
) -> Result<String, String> {
    let video_bytes: u64 = std::fs::read_dir(tmp.join("v"))
        .map_err(|e| e.to_string())?
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    let bandwidth = (video_bytes as f64 * 8.0 / duration.max(1.0)) as u64 + 192_000;

    let mut m = String::from("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n");
    let default = audio
        .iter()
        .position(|a| a.disposition.default == 1)
        .unwrap_or(0);
    let mut names: Vec<String> = Vec::new();
    for (i, a) in audio.iter().enumerate() {
        // NAME must be unique within the group.
        let mut name = a.name(format!("Track {}", i + 1)).replace('"', "'");
        if names.contains(&name) {
            name = format!("{name} ({})", i + 1);
        }
        names.push(name.clone());
        let lang = a
            .lang()
            .map(|l| format!(",LANGUAGE=\"{}\"", l.replace('"', "")))
            .unwrap_or_default();
        let yes = if i == default { "YES" } else { "NO" };
        m += &format!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"{name}\"{lang},DEFAULT={yes},AUTOSELECT=YES,URI=\"a{i}/index.m3u8\"\n"
        );
    }
    let codecs = if audio.is_empty() {
        avc_codec(v)
    } else {
        format!("{},mp4a.40.2", avc_codec(v))
    };
    let group = if audio.is_empty() {
        ""
    } else {
        ",AUDIO=\"aud\""
    };
    m += &format!(
        "#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},CODECS=\"{codecs}\",RESOLUTION={}x{}{group}\nv/index.m3u8\n",
        v.width.unwrap_or(0),
        v.height.unwrap_or(0)
    );
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_json(j: &str) -> Probe {
        serde_json::from_str(j).unwrap()
    }

    #[test]
    fn direct_play_rule() {
        let mp4 = r#"{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p"},
                                 {"index":1,"codec_type":"audio","codec_name":"aac"}]}"#;
        assert!(direct_play("a/movie.mp4", &probe_json(mp4)));
        assert!(
            !direct_play("a/movie.mkv", &probe_json(mp4)),
            "mkv always goes through HLS"
        );

        let hevc = r#"{"streams":[{"index":0,"codec_type":"video","codec_name":"hevc","pix_fmt":"yuv420p"}]}"#;
        assert!(!direct_play("m.mp4", &probe_json(hevc)));

        let ten_bit = r#"{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p10le"}]}"#;
        assert!(!direct_play("m.mp4", &probe_json(ten_bit)));

        let two_audio = r#"{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p"},
            {"index":1,"codec_type":"audio","codec_name":"aac"},{"index":2,"codec_type":"audio","codec_name":"aac"}]}"#;
        assert!(
            !direct_play("m.mp4", &probe_json(two_audio)),
            "needs HLS for the audio picker"
        );

        let webm = r#"{"streams":[{"index":0,"codec_type":"video","codec_name":"vp9"},{"index":1,"codec_type":"audio","codec_name":"opus"}]}"#;
        assert!(direct_play("m.webm", &probe_json(webm)));
    }

    #[test]
    fn transcode_sizes() {
        let s = |w, h| Stream {
            width: Some(w),
            height: Some(h),
            ..Default::default()
        };
        assert_eq!(fit_1080(&s(3840, 2160)), (1920, 1080));
        assert_eq!(fit_1080(&s(1920, 800)), (1920, 800));
        assert_eq!(fit_1080(&s(3840, 1600)), (2592, 1080));
    }
}
