#!/bin/sh
# One-time setup of the relay VM (Ubuntu 24.04). Safe to run again.
set -e
cat > /etc/sysctl.d/90-w2g.conf <<'S'
# Long, lossy paths (China <-> Tokyo <-> Europe): BBR keeps throughput up where loss-based
# congestion control collapses, and big buffers fit a 250 ms round trip at 100+ Mbit/s.
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_mtu_probing = 1
net.core.rmem_max = 33554432
net.core.wmem_max = 33554432
net.ipv4.tcp_rmem = 4096 131072 33554432
net.ipv4.tcp_wmem = 4096 131072 33554432
S
modprobe tcp_bbr || true
sysctl --system >/dev/null
# Oracle's Ubuntu images reject everything but SSH in iptables; open the web ports.
for p in 80 443; do
  iptables -C INPUT -p tcp --dport $p -j ACCEPT 2>/dev/null || iptables -I INPUT 5 -p tcp --dport $p -j ACCEPT
done
DEBIAN_FRONTEND=noninteractive apt-get install -y -q netfilter-persistent iptables-persistent >/dev/null
netfilter-persistent save >/dev/null
DEBIAN_FRONTEND=noninteractive apt-get install -y -q nginx certbot >/dev/null
mkdir -p /var/cache/nginx/w2g
sysctl net.ipv4.tcp_congestion_control
