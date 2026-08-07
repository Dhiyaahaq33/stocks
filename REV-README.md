# Radar Saham BEI: Mesin Pemburu Momentum yang Tak Pernah Tidur

Nama asli proyek: IDX Stock Scanner (idx_scanner.py, versi 2.1 / v3 feature set)

## Apa Ini?

Ini adalah bot screener saham otomatis untuk Bursa Efek Indonesia (BEI/IDX). Bot ini berjalan terus-menerus selama jam bursa, memantau puluhan saham dari berbagai sektor, menghitung skor teknikal dan fundamental setiap saham, lalu mengirim sinyal BUY/WATCH lengkap dengan entry, stop loss, dan target profit ke Telegram — setiap 30 menit.

Semuanya ada dalam satu file Python (`idx_scanner.py`, sekitar 1.900 baris). Bot ini juga punya "otak" untuk membaca kondisi makro ekonomi, regime pasar (bullish/bearish/sideways), momentum per sektor, dan bahkan aliran dana asing (foreign flow), sebelum akhirnya memutuskan apakah sebuah saham layak direkomendasikan atau tidak.

Ada juga fitur interaktif via Telegram (perintah `/scan`, `/detail`, `/backtest`, dsb) sehingga user bisa tanya langsung ke bot kapan saja, tidak cuma menunggu sinyal otomatis.

## Fitur Utama

- **Scan otomatis tiap 30 menit** selama jam bursa BEI (09:00–15:50 WIB), otomatis skip di hari libur bursa (kalender libur BEI 2026 sudah ditanam di kode).
- **Skor teknikal dari 10+ indikator**: RSI, MACD, Bollinger Bands (%B), Stochastic, ADX + DI, OBV, VWAP, Rate of Change, Williams %R, dan rasio volume vs rata-rata 20 hari. Semua digabung jadi satu raw score (maksimal 14.5) yang dinormalisasi ke skala 0–10.
- **Deteksi pola candlestick**: Morning Star, Bullish/Bearish Engulfing, Hammer, Bullish Harami, Shooting Star, Doji — masing-masing memberi bonus atau penalti ke skor akhir.
- **Filter fundamental**: PBV maksimal, PER maksimal, ROE minimal, volume rata-rata minimal, dan turnover harian minimal — supaya saham yang direkomendasikan tidak asal murah tapi juga tidak "gorengan" tanpa likuiditas.
- **Deteksi regime pasar IHSG** (BULL/BEAR/CHOP) — kalau market lagi BEAR, sinyal kategori WATCH otomatis ditahan (tidak dikirim).
- **Ranking momentum sektor** (Banking, Commodity, Consumer, Telco, Property, Healthcare, Industrial, Finance, Agri, dll.) — sinyal saham bisa didowngrade kalau sektornya sedang lemah.
- **Foreign flow / bandarmology sederhana** — kalau ada indikasi jual asing besar-besaran, sinyal BUY berkepercayaan tinggi otomatis diturunkan ke MEDIUM dengan catatan peringatan.
- **Data makro** (BI rate, dan indikator lain via scraping/FRED) yang ikut menyesuaikan skor akhir (makro positif/negatif memberi penyesuaian +1/-1 ke skor).
- **Position sizing otomatis** berdasarkan risk-reward ratio dan ukuran portofolio yang diset user (`/setportfolio`).
- **Riwayat sinyal & tracking hasil** disimpan di SQLite (`scanner_history.db`), termasuk pengecekan otomatis apakah target/stop-loss sudah tersentuh (outcome tracking).
- **Mini backtest** (`/backtest KODE HARI`) — menghitung win rate, average win/loss, profit factor, breakdown performa per regime pasar, dan pola candlestick terbaik dari data historis.
- **Statistik performa**: win rate 30 hari (`/winrate`), top performer (`/top`), riwayat sinyal per saham (`/history`).
- **Heartbeat monitoring** — bot mengirim laporan kesehatan (`/status`, heartbeat berkala tiap 1 jam) supaya user tahu bot masih hidup dan bekerja normal.
- **Multi-source fallback** — kalau data dari Yahoo Finance gagal, otomatis coba sumber alternatif (fallback Stockbit untuk data fundamental, fallback OHLCV alternatif).
- **Validasi universe saham** — mengecek dulu ticker mana yang datanya valid sebelum discan, supaya tidak buang waktu di ticker yang errornya banyak.
- **Perintah Telegram lengkap**: `/scan`, `/detail KODE`, `/makro`, `/regime`, `/sectors`, `/history`, `/winrate`, `/top`, `/backtest`, `/holiday`, `/status`, `/setportfolio`, `/help`.

## Teknologi yang Dipakai

- **Python 3.11+** (bahasa utama, satu file besar `idx_scanner.py`)
- **pandas & numpy** — olah data harga historis dan perhitungan indikator teknikal
- **yfinance** — sumber data harga (OHLCV) dan sebagian data fundamental dari Yahoo Finance
- **requests** — komunikasi ke Telegram Bot API dan scraping data makro/fundamental
- **BeautifulSoup4 (bs4)** — parsing HTML untuk fallback data (misalnya BI rate, data Stockbit)
- **sqlite3** (bawaan Python) — database lokal untuk riwayat sinyal dan tracking performa
- **ThreadPoolExecutor (concurrent.futures)** — scan banyak saham sekaligus secara paralel (multi-threading, default 16 worker)
- **Telegram Bot API** — kanal notifikasi dan kontrol interaktif bot
- **FRED API** (opsional) — data makro ekonomi tambahan

## Cara Instalasi

Laptop Windows 11 kamu sudah punya Python 3.11.6 dan pip 26.1.1, jadi tinggal install library Python-nya saja — tidak perlu tool tambahan seperti conda atau winget untuk proyek ini.

1. Buka PowerShell, masuk ke folder proyek:
   ```powershell
   cd "D:\BOT\MONEY\STOCKS"
   ```
2. (Opsional tapi disarankan) buat virtual environment supaya rapi:
   ```powershell
   py -3.11 -m venv venv
   .\venv\Scripts\Activate.ps1
   ```
3. Install semua library yang dibutuhkan:
   ```powershell
   pip install yfinance pandas numpy requests beautifulsoup4
   ```
4. Buka `idx_scanner.py`, cari bagian `CONFIG — WAJIB DIISI` di dekat baris atas file, lalu isi:
   - `TELEGRAM_BOT_TOKEN` — token bot Telegram kamu (dari @BotFather)
   - `TELEGRAM_CHAT_ID` — chat ID Telegram tujuan notifikasi
   - `FRED_API_KEY` — opsional, isi kalau mau pakai data makro dari FRED

## Cara Menjalankan

Jalankan langsung dengan Python dari PowerShell:

```powershell
cd "D:\BOT\MONEY\STOCKS"
python idx_scanner.py
```

Setelah berjalan, bot akan:
- Otomatis scan seluruh universe saham setiap 30 menit selama jam bursa (09:00–15:50 WIB), skip otomatis saat weekend/libur BEI.
- Mengirim sinyal BUY/WATCH ke Telegram sesuai chat ID yang dikonfigurasi.
- Bisa dikontrol lewat chat Telegram dengan perintah seperti `/scan`, `/detail BBCA`, `/backtest BBCA 30`, `/status`, dll (lihat daftar lengkap perintah di docstring awal file `idx_scanner.py`).

Kalau ingin bot berjalan terus di background (tidak tertutup saat PowerShell ditutup), bisa dijalankan lewat Task Scheduler Windows atau dibungkus dengan tool seperti `pm2`/`nssm`, atau paling simpel jalankan di jendela terminal yang dibiarkan terbuka.

File pendukung yang akan muncul otomatis saat dijalankan:
- `scanner_history.db` — database SQLite riwayat sinyal (tidak di-commit ke git, lihat `.gitignore`)
- `scanner.log` — log aktivitas bot
- `backtest_out.txt` — contoh/hasil output perintah backtest

## Catatan Penting

- **JANGAN pernah membagikan isi `TELEGRAM_BOT_TOKEN` atau `TELEGRAM_CHAT_ID`** di file `idx_scanner.py` ke publik (misalnya saat push ke GitHub atau screenshot). Kalau token sudah pernah bocor/ter-commit, segera revoke dan generate token baru lewat @BotFather di Telegram.
- File `.gitignore` proyek ini sudah mengatur agar file sensitif (`.env`, `secrets.toml`, `*.db`, `*.key`, `credentials.json`, `token.json`, dll) tidak ikut ter-commit ke git — pastikan kebiasaan ini tetap dijaga, terutama karena token Telegram saat ini ditulis langsung di source code (`idx_scanner.py`), bukan di file env terpisah.
- `FRED_API_KEY` juga termasuk kredensial — isi lokal saja, jangan disebar.
- Dokumentasi lama yang jauh lebih lengkap dan detail (mencakup penjelasan tiap indikator, skema skor, dan konfigurasi lengkap) sudah tersedia di file **`DOCUMENTATION.docx`** di folder yang sama (`D:\BOT\MONEY\STOCKS`). File `REV-README.md` ini adalah ringkasan pelengkap dalam Bahasa Indonesia, bukan pengganti dokumentasi tersebut.
- Bot ini murni untuk keperluan riset/screening pribadi — sinyal yang dihasilkan (BUY/WATCH) bukan rekomendasi finansial resmi. Selalu lakukan verifikasi dan pertimbangan risiko sendiri sebelum benar-benar bertransaksi.



## Riwayat Perbaikan (2026-07-22)

**Bug serius: angka `/winrate` dan `/backtest` tidak bisa dipercaya — statistiknya bias.** Root cause ada di `check_and_save_outcomes()` (fungsi yang mengecek tiap hari apakah sinyal sudah kena TP/SL):

1. **Sinyal `PENDING` dibekukan permanen.** Query lama (`LEFT JOIN outcomes o ... WHERE o.id IS NULL`) memilih sinyal yang *belum pernah* dicatat outcome-nya sama sekali. Begitu sinyal dicek sekali dan hasilnya `PENDING` (belum kena TP maupun SL), satu baris `outcomes` langsung tertulis untuk sinyal itu — dan sejak itu `o.id IS NULL` jadi `False` selamanya, jadi sinyal itu **tidak pernah dicek ulang lagi**. Karena `db_get_winrate()` sudah benar memfilter `outcome_status NOT IN ('PENDING','EXPIRED')`, sinyal yang macet di PENDING itu efektif hilang dari statistik — win-rate yang tersisa cuma dihitung dari sinyal yang kebetulan resolve cepat (kebanyakan kena stop-loss duluan), bukan gambaran performa sebenarnya.
   **Fix:** query diganti supaya re-cek sinyal manapun yang **belum mencapai status terminal** (`TP1_HIT`/`SL_HIT`/`EXPIRED`), bukan cuma yang belum pernah dicek sama sekali. Sinyal PENDING sekarang terus dicek ulang tiap hari sampai benar-benar resolve.
2. **Lookahead bias untuk sinyal hari yang sama.** Kalau sebuah sinyal baru dibuat hari ini lalu langsung dicek di hari yang sama, `day_high`/`day_low` yang dipakai untuk deteksi TP/SL mencakup **seluruh pergerakan harga hari itu — termasuk sebelum sinyal itu sendiri dibuat**. Ini bisa mencatat "TP kena" padahal itu cuma harga pagi sebelum sinyal ada.
   **Fix:** ditambahkan filter `signal_date < today` — sinyal baru tidak pernah dicek di hari yang sama saat dibuat, evaluasi dimulai keesokan harinya.

**Belum diverifikasi dengan data real** (butuh menunggu siklus scan+outcome-check berjalan beberapa hari untuk lihat statistik baru terisi), tapi sudah lolos `py_compile` dan logic query sudah diverifikasi manual sesuai skema tabel `signals`/`outcomes`.

## Kebutuhan API LLM

- **Butuh API LLM?** Tidak — semua logika (skor teknikal, deteksi pola candlestick, regime pasar, foreign flow, position sizing) berbasis rumus dan aturan numerik/statistik, bukan pemrosesan bahasa alami. Data makro pun diambil lewat scraping/API terstruktur (FRED), bukan diringkas dari teks bebas.
- **Bisa pakai API Claude (Anthropic)?** Tidak relevan untuk fungsi inti bot — tapi kalau mau dikembangkan lebih jauh, Claude API bisa ditambahkan sebagai fitur opsional untuk meringkas berita/sentimen pasar dalam bahasa natural sebelum digabung ke skor (misalnya lewat **Claude Haiku 4.5** untuk klasifikasi sentimen berita cepat, atau **Claude Sonnet 5** untuk analisis naratif kondisi makro lebih dalam) — namun ini bukan kebutuhan wajib, sistem skoring saat ini sudah berjalan penuh tanpa LLM.

## Instalasi & Eksekusi Offline

- **Bisa instalasi offline?** Sebagian. Instalasi pertama (`pip install yfinance pandas numpy requests beautifulsoup4`) WAJIB online karena pip narik package dari PyPI. Kalau semua package itu sudah pernah terinstall/tersimpan di pip cache lokal (`%LOCALAPPDATA%\pip\cache`), instalasi ulang di virtual environment baru bisa jalan offline dari cache tersebut.
- **Bisa dijalankan offline (setelah terinstall)?** Tidak bisa offline penuh — bot ini secara aktif butuh internet tiap siklus scan untuk narik data harga real-time dari Yahoo Finance (`yfinance`), kirim notifikasi ke Telegram Bot API, dan (opsional) scraping data makro/FRED. Kalau internet putus, scan akan gagal ambil data dan notifikasi Telegram tidak akan terkirim, meskipun kode Python-nya sendiri jalan lokal.
