#!/usr/bin/env python3
"""
Скачивает Bootstrap, Bootstrap Icons и Chart.js в static/vendor/ для работы без интернета.
Запустите один раз при наличии сети: python scripts/download_vendor.py
"""
import os
import sys
import urllib.request

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR = os.path.join(BASE, "static", "vendor")

ASSETS = [
    ("https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css", "bootstrap", "bootstrap.min.css"),
    ("https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js", "bootstrap", "bootstrap.bundle.min.js"),
    ("https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css", "bootstrap-icons", "bootstrap-icons.min.css"),
    ("https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/fonts/bootstrap-icons.woff2", "bootstrap-icons/fonts", "bootstrap-icons.woff2"),
    ("https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js", "chart.js", "chart.umd.min.js"),
]

def main():
    for url, subdir, name in ASSETS:
        out_dir = os.path.join(VENDOR, subdir)
        out_path = os.path.join(out_dir, name)
        os.makedirs(out_dir, exist_ok=True)
        try:
            print(f"Downloading {name}...", end=" ", flush=True)
            urllib.request.urlretrieve(url, out_path)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
            sys.exit(1)
    print("All vendor files are in place. The app can work offline.")

if __name__ == "__main__":
    main()
