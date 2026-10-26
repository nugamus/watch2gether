//! App state that must survive a restart (share links, guest sessions, uploaded subtitles, subtitle
//! offsets), kept as one JSON file in the data folder.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::convert::hex;

/// Guest sessions last as long as their cookie.
pub const SESSION_SECS: u64 = 30 * 24 * 3600;

#[derive(Serialize, Deserialize, Default)]
pub struct Data {
    #[serde(default)]
    pub links: Vec<Link>,
    /// sha256(session id) -> session.
    #[serde(default)]
    pub sessions: HashMap<String, Session>,
    /// Video id -> subtitles the host added for it.
    #[serde(default)]
    pub subs: HashMap<String, Vec<Upload>>,
    /// Video id -> subtitle delay in seconds.
    #[serde(default)]
    pub offsets: HashMap<String, f64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Link {
    pub id: String,
    pub name: String,
    /// sha256 of the token; the token itself is only ever shown to the host once.
    pub token_hash: [u8; 32],
    pub created: u64,
    pub expires: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub link: String,
    pub created: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Upload {
    pub id: String,
    pub name: String,
    pub lang: Option<String>,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `n` random bytes as URL-safe text.
pub fn random_token(n: usize) -> String {
    use base64::Engine;
    let mut b = vec![0u8; n];
    getrandom::fill(&mut b).expect("system RNG");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

fn sha(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

impl Link {
    fn live(&self, now: u64) -> bool {
        self.expires.is_none_or(|e| now < e)
    }
}

pub struct Store {
    path: PathBuf,
    data: Mutex<Data>,
}

impl Store {
    pub fn open(dir: &Path) -> std::io::Result<Store> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("state.json");
        let data = match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice(&b).map_err(std::io::Error::other)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Data::default(),
            Err(e) => return Err(e),
        };
        Ok(Store {
            path,
            data: Mutex::new(data),
        })
    }

    /// The data folder.
    pub fn dir(&self) -> &Path {
        self.path.parent().unwrap()
    }

    pub fn read<R>(&self, f: impl FnOnce(&Data) -> R) -> R {
        f(&self.data.lock().unwrap())
    }

    /// Change the state and write it to disk (atomically, readable only by us).
    pub fn update<R>(&self, f: impl FnOnce(&mut Data) -> R) -> R {
        let mut d = self.data.lock().unwrap();
        let r = f(&mut d);
        let tmp = self.path.with_extension("tmp");
        let res = std::fs::write(&tmp, serde_json::to_vec_pretty(&*d).unwrap()).and_then(|_| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
            }
            std::fs::rename(&tmp, &self.path)
        });
        if let Err(e) = res {
            eprintln!("could not save {}: {e}", self.path.display());
        }
        r
    }

    /// New link; returns it with its token (shown to the host once, never stored).
    pub fn create_link(&self, name: String, expires: Option<u64>) -> (Link, String) {
        let token = random_token(32);
        let link = Link {
            id: random_token(9),
            name,
            token_hash: sha(&token),
            created: now(),
            expires,
        };
        self.update(|d| d.links.push(link.clone()));
        (link, token)
    }

    /// Give a link a new token. The old token and every session made with it stop working.
    pub fn regenerate(&self, id: &str) -> Option<String> {
        let token = random_token(32);
        self.update(|d| {
            let l = d.links.iter_mut().find(|l| l.id == id)?;
            l.token_hash = sha(&token);
            d.sessions.retain(|_, s| s.link != id);
            Some(token)
        })
    }

    pub fn revoke(&self, id: &str) -> bool {
        self.update(|d| {
            let before = d.links.len();
            d.links.retain(|l| l.id != id);
            d.sessions.retain(|_, s| s.link != id);
            d.links.len() != before
        })
    }

    /// Swap a valid link token for a new session id (the cookie value).
    pub fn redeem(&self, token: &str) -> Option<String> {
        let h = sha(token);
        let t = now();
        // Compare against every link in constant time, so timing reveals nothing about the hashes.
        let link = self.read(|d| {
            let mut found = None;
            for l in &d.links {
                if bool::from(l.token_hash.ct_eq(&h)) && l.live(t) {
                    found = Some(l.id.clone());
                }
            }
            found
        })?;
        let sid = random_token(32);
        self.update(|d| {
            d.sessions.retain(|_, s| s.created + SESSION_SECS > t);
            d.sessions
                .insert(hex(&sha(&sid)), Session { link, created: t });
        });
        Some(sid)
    }

    /// The link id a guest session belongs to, if the session and its link are still valid.
    pub fn guest(&self, sid: &str) -> Option<String> {
        let t = now();
        self.read(|d| {
            let s = d.sessions.get(&hex(&sha(sid)))?;
            let ok =
                s.created + SESSION_SECS > t && d.links.iter().any(|l| l.id == s.link && l.live(t));
            ok.then(|| s.link.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_sessions_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        let (link, token) = s.create_link("Lin".into(), None);
        assert!(token.len() >= 43, "256-bit token");
        assert!(s.redeem("wrong").is_none());
        let sid = s.redeem(&token).unwrap();
        assert_eq!(s.guest(&sid).as_deref(), Some(link.id.as_str()));

        // Restart: everything is read back from disk.
        drop(s);
        let s = Store::open(dir.path()).unwrap();
        assert_eq!(s.guest(&sid).as_deref(), Some(link.id.as_str()));
        assert!(s.redeem(&token).is_some(), "the link keeps working");

        // Regenerating kills the old token and its sessions.
        let token2 = s.regenerate(&link.id).unwrap();
        assert!(s.redeem(&token).is_none());
        assert!(s.guest(&sid).is_none());
        let sid2 = s.redeem(&token2).unwrap();

        assert!(s.revoke(&link.id));
        assert!(s.redeem(&token2).is_none());
        assert!(s.guest(&sid2).is_none());
    }

    #[test]
    fn expired_links_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        let (_, old) = s.create_link("old".into(), Some(now() - 1));
        assert!(s.redeem(&old).is_none());
        let (l, tok) = s.create_link("soon".into(), Some(now() + 3600));
        let sid = s.redeem(&tok).unwrap();
        s.update(|d| d.links.iter_mut().find(|x| x.id == l.id).unwrap().expires = Some(now() - 1));
        assert!(
            s.guest(&sid).is_none(),
            "sessions die with their link's expiry"
        );
    }
}
