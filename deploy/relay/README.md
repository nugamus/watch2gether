# Relay for distant viewers

A small nginx box close to the viewers (here an Oracle Cloud Always Free VM in Tokyo, for viewers
in mainland China) that forwards everything to the home server and caches video it has fetched.

- Viewers' TCP connections end in Tokyo (~150 ms from South China instead of ~250 ms to Europe),
  and the VM runs BBR, which keeps throughput up on the lossy China leg.
- Video (`/m/...`) is cached in 4 MB slices for a day, so seeks and a second viewer don't cross to
  Europe again. Media URLs are secret per-video nonces, so the cache only serves people who
  already have the URL.
- Everything else (the page, login, share links, the WebSocket) is passed straight through, so
  the relay needs no configuration of the app. Cookies are set for the relay's own hostname.

Viewers use `https://streamcn.nugamus.quest` instead of `https://stream.nugamus.quest`: in a share
link, replace the hostname (`https://streamcn.nugamus.quest/r/<token>`).

## Setting it up

1. A VM with Ubuntu 24.04 and a public IP; open TCP 22, 80 and 443 in its security list.
2. DNS: an `A` record for the relay hostname pointing at the VM, **DNS only** (not proxied).
3. On the VM, as root: `sh setup.sh` (BBR and TCP buffers, firewall, nginx, certbot).
4. `certbot certonly --webroot -w /var/www/html -d streamcn.nugamus.quest --agree-tos
   --register-unsafely-without-email --deploy-hook "systemctl reload nginx"`
5. Copy `nginx.conf` to `/etc/nginx/sites-available/w2g`, link it into `sites-enabled`, remove
   `sites-enabled/default`, then `nginx -t && systemctl reload nginx`.

`/speedtest.bin` (20 MB of random bytes) is there for measuring viewer-to-relay speed with online
test sites. If the home IP changes, `systemctl reload nginx` on the relay so it resolves again.
