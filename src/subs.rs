//! Subtitles: language names, encoding detection and conversion to WebVTT.

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
