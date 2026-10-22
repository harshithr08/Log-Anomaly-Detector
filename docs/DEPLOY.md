# Deploying the public demo

The same `docker compose` stack on one small cloud server, plus
[docker-compose.prod.yml](../docker-compose.prod.yml):

- **Caddy** in front: HTTPS with a free Let's Encrypt certificate, renewed
  automatically. Only ports 80 and 443 are open; everything else stays inside
  Docker's network.
- **Break buttons open to visitors, within limits:** each fault fixes itself
  after 2 minutes, and each visitor may break things 3 times a minute. The API
  key in `.env` skips both limits.
- **Retention:** data older than a day is deleted; container logs are capped.
- **Auto-deploy:** after CI passes on `main`, GitHub Actions logs into the
  server and rebuilds ([.github/workflows/deploy.yml](../.github/workflows/deploy.yml)).

No domain needed: [sslip.io](https://sslip.io) turns the server's IP into
one (`203-0-113-7.sslip.io` points to `203.0.113.7`).

## 1. A server

Any Ubuntu 22.04/24.04 machine with **2 GB+ RAM** and a public IP. Free option:
[Oracle Cloud Always Free](https://www.oracle.com/cloud/free/) (a card is
needed for sign-up, but Always Free resources are never charged).

1. **Compute → Instances → Create instance**
   - Image: **Canonical Ubuntu 24.04**
   - Shape: **Ampere → VM.Standard.A1.Flex**, 2 OCPUs, 12 GB memory (Always Free)
   - SSH keys: **Generate a key pair**, download the private key
2. Open the web ports: on the instance page, click the **subnet → its security
   list → Add ingress rules**: source `0.0.0.0/0`, TCP, destination port `80`;
   again for `443`.

If Oracle says "out of capacity", try another availability domain or later;
or use any other provider (e.g. a Hetzner CX22, ~€4/month).

## 2. One-time setup

```bash
ssh -i <downloaded-key> ubuntu@<server-ip>
curl -fsSL https://raw.githubusercontent.com/harshithr08/Log-Anomaly-Detector/main/deploy/setup.sh | bash
```

It installs Docker, opens the firewall, clones the repo, writes `.env` (domain
and a random API key) and starts everything, ~5 minutes the first time. At the
end it prints the link (`https://<ip-with-dashes>.sslip.io`) and the key.

## 3. Auto-deploy on push (optional)

On your laptop, make a key just for GitHub Actions and let it into the server:

```bash
ssh-keygen -t ed25519 -f deploy_key -N "" -C github-deploy
ssh -i <downloaded-key> ubuntu@<server-ip> "cat >> ~/.ssh/authorized_keys" < deploy_key.pub
ssh-keyscan <server-ip>                   # the server's identity, for DEPLOY_KNOWN_HOSTS
```

GitHub → repo **Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Value |
|---|---|
| `DEPLOY_HOST` | the server's IP |
| `DEPLOY_USER` | `ubuntu` |
| `DEPLOY_SSH_KEY` | the contents of `deploy_key` (the private one) |
| `DEPLOY_KNOWN_HOSTS` | the output of `ssh-keyscan` |

Then delete `deploy_key*` from your laptop. Every push to `main` that passes CI
now updates the server; **Actions → Deploy → Run workflow** deploys by hand.

## Day to day

```bash
cd ~/Log-Anomaly-Detector
./deploy/up.sh                                                    # restart / update
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f api   # logs
docker compose -f docker-compose.yml -f docker-compose.prod.yml down          # stop
```
