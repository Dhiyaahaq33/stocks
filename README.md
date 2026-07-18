# IDX Stock Scanner

Bot scanner saham otomatis untuk **Bursa Efek Indonesia (BEI/IDX)**. Program single-file Python ini memantau puluhan saham blue-chip & likuid dari berbagai sektor secara berkala, mencari saham yang *undervalue*, likuid, dan sedang momentum kuat berdasarkan kombinasi analisis teknikal + fundamental + kondisi makro, lalu mengirim sinyal beli/watch ke Telegram setiap 30 menit selama jam bursa berlangsung.

Bot dikendalikan penuh lewat perintah di Telegram (scan manual, detail sinyal per saham, data makro, ranking sektor, riwayat & win rate sinyal, mini backtest, dll).

## Fitur Utama

- **Scan otomatis** setiap 30 menit selama jam bursa BEI (09:00–15:50 WIB), otomatis skip di akhir pekan & libur nasional (kalender libur BEI 2026 sudah ditanam di kode).
- **Universe saham** ± 75 ticker BEI lintas sektor (Banking, Commodity, Consumer, Telco, Property, Healthcare, Industrial, Finance, Agri, dll), dengan validasi otomatis (ticker yang gagal fetch berturut-turut dikeluarkan dari universe aktif).
- **Analisis teknikal** lengkap: RSI, MACD, Bollinger Bands, Stochastic, ADX/DMI, OBV, VWAP, ATR, ROC, Williams %R, plus deteksi pola candlestick (Morning Star, Bullish/Bearish Engulfing, Hammer, Shooting Star, Harami, Doji) untuk menghasilkan skor teknikal komposit.
- **Filter fundamental**: PBV, PER, ROE, rata-rata volume & turnover minimum.
- **Deteksi market regime** IHSG (BULL/BEAR/CHOP) berbasis EMA20/EMA50 yang memengaruhi keputusan sinyal.
- **Sector momentum ranking** — return relatif tiap sektor terhadap IHSG 10 hari terakhir, dipakai sebagai penyesuai (adjustment) confidence sinyal.
- **Data makro**: USD/IDR, DXY, harga Brent Oil, Gold, US 10Y yield (opsional via FRED API), BI Rate (scraping bi.go.id), dan foreign net buy/sell (scraping IDX) — dirangkum jadi skor sentimen makro POSITIVE/NEGATIVE/NEUTRAL.
- **Position sizing** otomatis berbasis risk % dari ukuran portofolio yang dikonfigurasi user.
- **Riwayat sinyal & win rate** tersimpan di SQLite, termasuk pengecekan outcome (kena Take Profit / Stop Loss / Pending / Expired) otomatis tiap sore jam 15:55 WIB.
- **Mini backtest** per ticker dari data historis.
- **Fallback berlapis**: kalau `yfinance` gagal, otomatis fallback ke scraping Yahoo Finance chart API; kalau data fundamental kosong, fallback scraping Stockbit.
- **Kontrol penuh via Telegram bot commands**:
  - `/scan` — scan manual
  - `/detail BBCA` — sinyal lengkap satu saham (dengan fallback ke database jika sedang tidak ada sinyal aktif)
  - `/makro` — data makro terkini
  - `/regime` — status market regime IHSG
  - `/sectors` — ranking momentum sektor
  - `/history [KODE]` — riwayat sinyal
  - `/winrate` — performa 30 hari terakhir
  - `/top` — ticker dengan performa terbaik
  - `/backtest BBCA 30` — mini backtest dari data historis
  - `/holiday` — libur BEI berikutnya
  - `/status` — statistik kesehatan scanner
  - `/setportfolio 25000000` — atur ukuran portofolio untuk position sizing
  - `/help` — daftar perintah

## Tech Stack

- **Python 3** (single file: `idx_scanner.py`)
- [`yfinance`](https://pypi.org/project/yfinance/) — sumber data harga OHLCV utama (data BEI via ticker `*.JK`)
- `requests` + `beautifulsoup4` — web scraping fallback (Yahoo Finance chart API, Stockbit, situs BI, situs IDX)
- `pandas`, `numpy` — komputasi indikator teknikal
- `sqlite3` (bawaan Python) — penyimpanan riwayat sinyal & outcome
- Telegram Bot API — notifikasi & antarmuka perintah

## Instalasi

```bash
git clone <url-repo-ini>
cd stocks
pip install yfinance pandas numpy requests beautifulsoup4
```

## Konfigurasi

Sebelum menjalankan, edit bagian `CONFIG` di bagian atas `idx_scanner.py`:

```python
TELEGRAM_BOT_TOKEN = "GANTI_DENGAN_TOKEN_BOT_TELEGRAM_ANDA"
TELEGRAM_CHAT_ID   = "GANTI_DENGAN_CHAT_ID_ANDA"

FRED_API_KEY = ""   # opsional, untuk data US 10Y Treasury yield yang lebih akurat
```

- `TELEGRAM_BOT_TOKEN` — token bot Telegram, didapat dari [@BotFather](https://t.me/BotFather).
- `TELEGRAM_CHAT_ID` — chat ID (personal atau grup) tujuan pengiriman sinyal.
- `FRED_API_KEY` — opsional, key dari [FRED (St. Louis Fed)](https://fred.stlouisfed.org/docs/api/api_key.html) untuk data yield US Treasury 10 tahun. Jika kosong, bot pakai fallback data dari Yahoo Finance (`^TNX`).

Parameter lain yang bisa disesuaikan di dictionary `CONFIG`, `FILTER`, dan `RISK` antara lain: interval scan, ambang PBV/PER/ROE/volume minimum, rasio risk-reward, kelipatan ATR untuk stop loss/target, dan ukuran portofolio default.

**Jangan pernah commit token/API key asli ke repository.** Simpan kredensial di file terpisah (mis. environment variable) jika repo akan dipublikasikan lebih lanjut.

## Menjalankan

```bash
python idx_scanner.py
```

Setelah berjalan, bot akan:
1. Memvalidasi universe saham yang aktif.
2. Mengirim pesan status "aktif" ke Telegram.
3. Melakukan scan otomatis tiap 30 menit selama jam bursa, dan mendengarkan perintah `/command` dari Telegram di thread terpisah.

Log aktivitas tersimpan di `scanner.log`, dan riwayat sinyal tersimpan di `scanner_history.db` (SQLite, dibuat otomatis).

## Disclaimer

Project ini dibuat untuk tujuan edukasi dan riset pribadi. Sinyal yang dihasilkan berbasis indikator teknikal, data fundamental terbatas, dan data scraping dari sumber pihak ketiga yang **tidak dijamin akurat atau real-time**. Ini **bukan nasihat/rekomendasi finansial**. Segala keputusan investasi/trading sepenuhnya menjadi tanggung jawab pengguna sendiri — selalu lakukan riset mandiri (DYOR) sebelum mengambil keputusan.
