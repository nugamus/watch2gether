//! Subtitles: finding sidecar files, fetching them from URLs, fixing their text encoding and
//! converting everything to WebVTT, which every browser parses.

use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use tokio::{io::AsyncWriteExt, process::Command};

/// Largest subtitle file accepted from an upload or URL.
pub const MAX_BYTES: usize = 5 * 1024 * 1024;

const EXTS: [&str; 4] = ["srt", "vtt", "ass", "ssa"];

/// Display name for a language code as found in file names and stream tags.
pub fn lang_name(code: &str) -> Option<&'static str> {
    Some(match code.to_ascii_lowercase().as_str() {
        "en" | "eng" => "English",
        "zh" | "chi" | "zho" | "chn" => "Chinese",
        "chs" | "sc" | "zh-hans" | "zh-cn" | "gb" => "Chinese (Simplified)",
        "cht" | "tc" | "zh-hant" | "zh-tw" | "zh-hk" | "big5" => "Chinese (Traditional)",
        "ja" | "jpn" | "jp" => "Japanese",
        "ko" | "kor" => "Korean",
        "fr" | "fre" | "fra" => "French",
        "de" | "ger" | "deu" => "German",
        "es" | "spa" => "Spanish",
        "it" | "ita" => "Italian",
        "pt" | "por" => "Portuguese",
        "ru" | "rus" => "Russian",
        "nl" | "dut" | "nld" => "Dutch",
        "sv" | "swe" => "Swedish",
        "da" | "dan" => "Danish",
        "no" | "nor" | "nb" => "Norwegian",
        "fi" | "fin" => "Finnish",
        "pl" | "pol" => "Polish",
        "ar" | "ara" => "Arabic",
        "hi" | "hin" => "Hindi",
        "th" | "tha" => "Thai",
        "vi" | "vie" => "Vietnamese",
        _ => return None,
    })
}

/// Language and display name from the part of a file name between the video's name and the
/// extension: `zh` -> Chinese, `en.forced` -> English forced, `` -> External.
pub fn label(middle: &str) -> (Option<String>, String) {
    let tokens: Vec<&str> = middle.split('.').filter(|t| !t.is_empty()).collect();
    let lang = tokens
        .iter()
        .find(|t| lang_name(t).is_some())
        .map(|t| t.to_ascii_lowercase());
    let rest: Vec<&str> = tokens
        .iter()
        .copied()
        .filter(|t| lang_name(t).is_none())
        .collect();
    let mut name = lang
        .as_deref()
        .and_then(lang_name)
        .unwrap_or("")
        .to_string();
    if !rest.is_empty() {
        if !name.is_empty() {
            name.push(' ');
        }
        name += &rest.join(" ");
    }
    if name.is_empty() {
        name = "External".into();
    }
    (lang, name)
}

/// Language and name for an added subtitle: from its file name (`Movie.2019.zh.srt`), else from
/// the script its text is written in, else the file name itself.
pub fn upload_label(file: &str, text: &str) -> (Option<String>, String) {
    let stem = Path::new(file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let lang = stem
        .rsplit('.')
        .find(|t| lang_name(t).is_some())
        .map(str::to_ascii_lowercase);
    let lang = lang.or_else(|| {
        let count =
            |r: std::ops::RangeInclusive<char>| text.chars().filter(|c| r.contains(c)).count();
        let kana = count('\u{3040}'..='\u{30ff}');
        let hangul = count('\u{ac00}'..='\u{d7af}');
        let han = count('\u{4e00}'..='\u{9fff}');
        let letters = text.chars().filter(|c| c.is_alphabetic()).count().max(1);
        if kana * 10 > letters {
            Some("ja".into())
        } else if hangul * 4 > letters {
            Some("ko".into())
        } else if han * 4 > letters {
            Some("zh".into())
        } else {
            None
        }
    });
    let name = match lang.as_deref().and_then(lang_name) {
        Some(n) => n.to_string(),
        None if !stem.is_empty() => stem.chars().take(40).collect(),
        None => "Added".into(),
    };
    (lang, name)
}

pub struct Sidecar {
    pub path: PathBuf,
    pub lang: Option<String>,
    pub name: String,
}

/// Subtitle files next to `video` named after it: `Movie.srt`, `Movie.zh.srt`, `Movie.en.vtt`...
pub fn sidecars(video: &Path) -> Vec<Sidecar> {
    let (Some(dir), Some(stem)) = (video.parent(), video.file_stem().and_then(|s| s.to_str()))
    else {
        return Vec::new();
    };
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<Sidecar> = rd
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_name()?.to_str()?;
            let ext = path.extension()?.to_str()?.to_ascii_lowercase();
            if !EXTS.contains(&ext.as_str()) || !path.is_file() {
                return None;
            }
            let middle = name
                .strip_prefix(stem)?
                .strip_suffix(path.extension()?.to_str()?)?;
            let middle = middle.strip_prefix('.')?.strip_suffix('.').unwrap_or("");
            let (lang, name) = label(middle);
            Some(Sidecar { path, lang, name })
        })
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

/// Text of a subtitle file in whatever encoding it came in. Chinese subtitles are often GBK /
/// GB18030 or Big5 rather than UTF-8, which would otherwise show up as garbage.
pub fn decode(b: &[u8]) -> String {
    if let Some((enc, bom)) = encoding_rs::Encoding::for_bom(b) {
        return enc.decode_without_bom_handling(&b[bom..]).0.into_owned();
    }
    if let Ok(s) = std::str::from_utf8(b) {
        return s.to_owned();
    }
    let mut det = chardetng::EncodingDetector::new(chardetng::Iso2022JpDetection::Allow);
    det.feed(b, true);
    det.guess(None, chardetng::Utf8Detection::Allow)
        .decode_without_bom_handling(b)
        .0
        .into_owned()
}

fn format_of(text: &str) -> Option<&'static str> {
    let t = text.trim_start();
    if t.starts_with("WEBVTT") {
        Some("webvtt")
    } else if t.contains("[Script Info]") || t.contains("[Events]") {
        Some("ass")
    } else if t.contains("-->") {
        Some("srt")
    } else {
        None
    }
}

/// Convert an SRT / ASS / SSA / WebVTT file (any encoding) to UTF-8 WebVTT. ASS styling and
/// positioning are dropped; the text, line breaks and italics are kept.
pub async fn to_vtt(bytes: &[u8]) -> Result<String, String> {
    let text = decode(bytes);
    let fmt = format_of(&text).ok_or("not a subtitle file (expected SRT, ASS or WebVTT)")?;
    let mut child = Command::new("ffmpeg")
        .args([
            "-nostdin", "-v", "error", "-f", fmt, "-i", "pipe:0", "-f", "webvtt", "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("could not run ffmpeg ({e})"))?;
    let mut stdin = child.stdin.take().unwrap();
    let input = text.into_bytes();
    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&input).await;
    });
    let out = child.wait_with_output().await.map_err(|e| e.to_string())?;
    let _ = writer.await;
    let vtt = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() || !vtt.contains("-->") {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "could not read the subtitles {}",
            err.lines().last().unwrap_or("")
        ));
    }
    Ok(vtt)
}

/// True for addresses on the public internet. Everything else (loopback, private and link-local
/// ranges, CGNAT, ULA, multicast, IPv4 tunnelled in IPv6...) could reach the home network.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64)
                || (o[0] == 192 && o[1] == 0 && o[2] == 0)
                || (o[0] == 198 && (o[1] & 0xfe) == 18)
                || o[0] >= 240)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public(v4.into());
            }
            let s = v6.segments();
            (s[0] & 0xe000) == 0x2000 // global unicast 2000::/3 only
                && !(s[0] == 0x2001 && (s[1] == 0x0db8 || s[1] == 0)) // documentation, Teredo
                && s[0] != 0x2002 // 6to4
        }
    }
}

/// Download a subtitle file. Only http(s) on the standard ports, only public addresses (checked
/// after DNS, and the connection is pinned to the checked address so a second lookup can't
/// point elsewhere), every redirect re-checked, 5 MB and 15 s at most.
pub async fn fetch(url: &str) -> Result<Vec<u8>, String> {
    let mut url = reqwest::Url::parse(url.trim()).map_err(|_| "not a valid URL")?;
    for _ in 0..4 {
        if !matches!(url.scheme(), "http" | "https") {
            return Err("only http and https links work".into());
        }
        let host = url.host_str().ok_or("the URL has no host")?.to_string();
        let port = url.port_or_known_default().unwrap_or(0);
        if port != 80 && port != 443 {
            return Err("only the standard ports (80, 443) are allowed".into());
        }
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.trim_matches(['[', ']']), port))
            .await
            .map_err(|_| format!("could not find {host}"))?
            .collect();
        if addrs.is_empty() || !addrs.iter().all(|a| is_public(a.ip())) {
            return Err("that address is on a private or local network".into());
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .resolve_to_addrs(&host, &addrs)
            .build()
            .map_err(|e| e.to_string())?;
        let mut res = client
            .get(url.clone())
            .send()
            .await
            .map_err(|e| format!("download failed: {e}"))?;
        if res.status().is_redirection() {
            let loc = res
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|l| l.to_str().ok())
                .ok_or("redirect without a location")?;
            url = url.join(loc).map_err(|_| "bad redirect")?;
            continue;
        }
        if !res.status().is_success() {
            return Err(format!("the server answered {}", res.status()));
        }
        if res.content_length().is_some_and(|l| l > MAX_BYTES as u64) {
            return Err("the file is larger than 5 MB".into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = res
            .chunk()
            .await
            .map_err(|e| format!("download failed: {e}"))?
        {
            body.extend_from_slice(&chunk);
            if body.len() > MAX_BYTES {
                return Err("the file is larger than 5 MB".into());
            }
        }
        return Ok(body);
    }
    Err("too many redirects".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn languages_from_file_names() {
        assert_eq!(label("zh"), (Some("zh".into()), "Chinese".into()));
        assert_eq!(
            label("en.forced"),
            (Some("en".into()), "English forced".into())
        );
        assert_eq!(
            label("chs"),
            (Some("chs".into()), "Chinese (Simplified)".into())
        );
        assert_eq!(label(""), (None, "External".into()));
        assert_eq!(label("director"), (None, "director".into()));
    }

    #[test]
    fn labels_uploads() {
        assert_eq!(
            upload_label("The.Movie.2019.1080p.zh.srt", ""),
            (Some("zh".into()), "Chinese".into())
        );
        let vtt = "WEBVTT\n\n00:00:01.000 --> 00:00:04.000\n你好，这是中文字幕。\n";
        assert_eq!(
            upload_label("download.srt", vtt),
            (Some("zh".into()), "Chinese".into())
        );
        assert_eq!(
            upload_label("subs.srt", "WEBVTT\n\nHello there"),
            (None, "subs".into())
        );
    }

    #[test]
    fn finds_sidecars() {
        let d = tempfile::tempdir().unwrap();
        for f in [
            "Movie.mkv",
            "Movie.srt",
            "Movie.zh.srt",
            "Movie.en.vtt",
            "Movie.ass",
            "Other.srt",
            "Movie.nfo",
        ] {
            std::fs::write(d.path().join(f), "x").unwrap();
        }
        let found: Vec<(String, String)> = sidecars(&d.path().join("Movie.mkv"))
            .into_iter()
            .map(|s| {
                (
                    s.path.file_name().unwrap().to_string_lossy().into_owned(),
                    s.name,
                )
            })
            .collect();
        let want = [
            ("Movie.ass", "External"),
            ("Movie.en.vtt", "English"),
            ("Movie.srt", "External"),
            ("Movie.zh.srt", "Chinese"),
        ];
        assert_eq!(found, want.map(|(a, b)| (a.to_string(), b.to_string())));
    }

    const ZH: &str = "1\r\n00:00:01,000 --> 00:00:04,000\r\n你好，这是中文字幕。我们一起看电影吧，今天晚上的电影非常好看。\r\n\r\n2\r\n00:00:05,000 --> 00:00:09,000\r\n他说：“我不知道你在说什么，但是我会帮助你的。”\r\n";
    const ZH_TW: &str = "1\r\n00:00:01,000 --> 00:00:04,000\r\n你好，這是中文字幕。我們一起看電影吧，今天晚上的電影非常好看。\r\n\r\n2\r\n00:00:05,000 --> 00:00:09,000\r\n他說：「我不知道你在說什麼，但是我會幫助你的。」\r\n";

    #[test]
    fn decodes_chinese_encodings() {
        let (gbk, _, _) = encoding_rs::GBK.encode(ZH);
        assert!(std::str::from_utf8(&gbk).is_err());
        assert_eq!(decode(&gbk), ZH);
        let (big5, _, _) = encoding_rs::BIG5.encode(ZH_TW);
        assert_eq!(decode(&big5), ZH_TW);
        let mut bom = vec![0xef, 0xbb, 0xbf];
        bom.extend_from_slice(ZH.as_bytes());
        assert_eq!(decode(&bom), ZH);
        let utf16: Vec<u8> = [0xff, 0xfe]
            .into_iter()
            .chain(ZH.encode_utf16().flat_map(u16::to_le_bytes))
            .collect();
        assert_eq!(decode(&utf16), ZH);
    }

    #[tokio::test]
    async fn converts_gbk_srt_and_ass_to_vtt() {
        if std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .is_err()
        {
            return eprintln!("ffmpeg not installed; skipping");
        }
        let (gbk, _, _) = encoding_rs::GBK.encode(ZH);
        let vtt = to_vtt(&gbk).await.unwrap();
        assert!(
            vtt.starts_with("WEBVTT") && vtt.contains("00:01.000 --> 00:04.000"),
            "{vtt}"
        );
        assert!(vtt.contains("你好，这是中文字幕。"), "{vtt}");

        let ass = "[Script Info]\nScriptType: v4.00+\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\nDialogue: 0,0:00:01.00,0:00:04.00,Default,,0,0,0,,{\\an8}第一行\\N第二行\n";
        let vtt = to_vtt(ass.as_bytes()).await.unwrap();
        assert!(
            vtt.contains("第一行\n第二行") && !vtt.contains("an8"),
            "{vtt}"
        );

        assert!(to_vtt(b"just some text").await.is_err());
    }

    #[test]
    fn only_public_addresses() {
        for bad in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.10",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "::ffff:192.168.1.1",
            "2002:c0a8:101::1",
            "64:ff9b::a00:1",
        ] {
            assert!(!is_public(bad.parse().unwrap()), "{bad}");
        }
        for good in ["1.1.1.1", "140.82.112.3", "2606:4700:4700::1111"] {
            assert!(is_public(good.parse().unwrap()), "{good}");
        }
    }

    #[tokio::test]
    async fn fetch_refuses_local_targets() {
        for url in [
            "http://127.0.0.1/x.srt",
            "http://localhost/x.srt",
            "http://[::1]/x.srt",
            "http://192.168.1.1/x.srt",
            "http://example.com:8080/x.srt",
            "file:///etc/passwd",
            "ftp://example.com/x.srt",
        ] {
            assert!(fetch(url).await.is_err(), "{url}");
        }
    }
}
