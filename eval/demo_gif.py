"""
demo_gif.py — record the 2-minute demo as docs/demo.gif.

Drives the real dashboard in headless Chrome (DevTools protocol) on a running
stack: normal traffic, Break payment (skip), the alert appearing, its detail
view, Fix, the alert resolving. A screenshot every few seconds becomes a frame.

Usage (stack running, Chrome installed):
    python eval/demo_gif.py
"""

import base64
import io
import json
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import httpx
from PIL import Image
from websockets.sync.client import connect

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DASHBOARD = "http://localhost:8200/"
OUT = Path("docs/demo.gif")
WIDTH, HEIGHT = 1280, 820


class Browser:
    def __init__(self):
        self.process = subprocess.Popen(
            [
                CHROME,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                "--remote-debugging-port=9334",
                f"--user-data-dir={tempfile.mkdtemp()}",
                f"--window-size={WIDTH},{HEIGHT}",
                "about:blank",
            ]
        )
        for _ in range(50):
            try:
                tabs = json.load(urllib.request.urlopen("http://127.0.0.1:9334/json"))
                page = next(t for t in tabs if t["type"] == "page")
                break
            except Exception:
                time.sleep(0.2)
        self.ws = connect(page["webSocketDebuggerUrl"], max_size=50_000_000)
        self.n = 0
        self.call(
            "Emulation.setDeviceMetricsOverride",
            width=WIDTH,
            height=HEIGHT,
            deviceScaleFactor=1,
            mobile=False,
        )
        self.call(
            "Emulation.setEmulatedMedia",
            features=[{"name": "prefers-color-scheme", "value": "light"}],
        )

    def call(self, method, **params):
        self.n += 1
        self.ws.send(json.dumps({"id": self.n, "method": method, "params": params}))
        while True:
            message = json.loads(self.ws.recv())
            if message.get("id") == self.n:
                return message.get("result", {})

    def js(self, expression):
        return self.call("Runtime.evaluate", expression=expression, returnByValue=True)

    def frame(self, caption: str) -> Image.Image:
        self.js(f"window.__caption && window.__caption({json.dumps(caption)})")
        data = self.call("Page.captureScreenshot", format="png")["data"]
        return Image.open(io.BytesIO(base64.b64decode(data))).convert("RGB")

    def close(self):
        self.ws.close()
        self.process.kill()


CAPTION = """
window.__caption = (text) => {
  let el = document.getElementById("demo-caption");
  if (!el) {
    el = document.createElement("div"); el.id = "demo-caption";
    el.style.cssText = "position:fixed;left:50%;bottom:18px;transform:translateX(-50%);" +
      "background:#0b0b0b;color:#fff;padding:8px 16px;border-radius:8px;font:600 15px system-ui;" +
      "z-index:9999;box-shadow:0 2px 10px rgba(0,0,0,.25)";
    document.body.append(el);
  }
  el.textContent = text;
};
"""


def open_alerts(service: str) -> int:
    items = httpx.get("http://127.0.0.1:8200/api/alerts?status=open").json()["items"]
    return sum(a["service"] == service for a in items)


def main():
    frames, durations = [], []
    browser = Browser()
    try:
        browser.call("Page.navigate", url=DASHBOARD)
        time.sleep(8)
        browser.js(CAPTION)

        def shoot(caption, ms=900, n=1, every=1.5):
            for _ in range(n):
                frames.append(browser.frame(caption))
                durations.append(ms)
                time.sleep(every)

        shoot("Live: the shop's logs streaming through the pipeline", n=4)
        browser.js(
            '[...document.querySelectorAll("#services .service-row")]'
            '.find(r => r.textContent.includes("payment"))'
            '.querySelectorAll("button")[2].click()'
        )  # Skip
        shoot("Break payment: it silently skips the fraud check", ms=1400)
        while not open_alerts("payment"):
            shoot("Waiting for the detectors...", ms=500, every=1)
        shoot("Alert: which service, what is wrong, caught by the LSTM", ms=1500, n=3)
        browser.js('document.querySelector("#open-alerts .alert").click()')
        time.sleep(2)
        shoot("Example requests: the unexpected step is highlighted", ms=2500, n=2, every=2)
        browser.js('document.getElementById("d-close").click()')
        browser.js(
            '[...document.querySelectorAll("#services .service-row")]'
            '.find(r => r.textContent.includes("payment"))'
            '.querySelector("button.fix").click()'
        )
        shoot("Fix: the alert resolves by itself once things are normal", ms=1200)
        while open_alerts("payment"):
            shoot("Fixed, waiting for a quiet minute...", ms=350, every=4)
        shoot("Resolved", ms=2500, n=2)
    finally:
        browser.close()

    small = [f.resize((960, int(960 * f.height / f.width)), Image.LANCZOS) for f in frames]
    OUT.parent.mkdir(exist_ok=True)
    small[0].save(
        OUT, save_all=True, append_images=small[1:], duration=durations, loop=0, optimize=True
    )
    print(f"{OUT}: {len(frames)} frames, {OUT.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
