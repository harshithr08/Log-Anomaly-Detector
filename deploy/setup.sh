#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu server for the public demo:
#
#   curl -fsSL https://raw.githubusercontent.com/harshithr08/Log-Anomaly-Detector/main/deploy/setup.sh | bash
#
# Installs Docker, opens ports 80/443, clones the repo, writes .env (a free
# <ip>.sslip.io domain and a random API key) and starts everything.
set -euo pipefail
REPO=https://github.com/harshithr08/Log-Anomaly-Detector.git
DIR=$HOME/Log-Anomaly-Detector

if ! command -v docker > /dev/null; then
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
fi

# Oracle's Ubuntu images block everything but SSH in iptables; allow web traffic.
for port in 80 443; do
    sudo iptables -C INPUT -p tcp --dport $port -j ACCEPT 2> /dev/null ||
        sudo iptables -I INPUT 1 -p tcp --dport $port -j ACCEPT
done
if command -v netfilter-persistent > /dev/null; then sudo netfilter-persistent save; fi

# Small machines: add swap so building the image cannot run out of memory.
if [ "$(free -m | awk '/Mem:/ {print $2}')" -lt 3000 ] && ! swapon --show | grep -q .; then
    sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
    sudo mkswap /swapfile > /dev/null && sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab > /dev/null
fi

[ -d "$DIR" ] || git clone -q "$REPO" "$DIR"
cd "$DIR"
if [ ! -f .env ]; then
    ip=$(curl -fsS https://api.ipify.org)
    {
        echo "DOMAIN=${ip//./-}.sslip.io"
        echo "LOGAD_API_KEY=$(openssl rand -hex 16)"
        echo "TRAFFIC_RATE=10"
    } > .env
fi

sg docker -c ./deploy/up.sh  # the new docker group, without logging in again
echo "Your API key (skips the Break limits): $(grep -oP '^LOGAD_API_KEY=\K.*' .env)"
