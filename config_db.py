# -*- coding: utf-8 -*-
"""
NewsTelASCL v2.0 - Konfigürasyon ve Veritabanı Ayarları
Tüm API anahtarları, bağlantı bilgileri ve sabitler burada tanımlanır.
"""

# --- API ANAHTARLARI ---
GEMINI_API_KEYS = [
    "2-xBugyd-UgKsdvsdvTywtp9HlYg",
    "hebdfbsvsdvsdvsdvsdvsdvsdv2",
]

# --- TELEGRAM AYARLARI ---
TG_API_ID = 3455
TG_API_HASH = " "
TG_PHONE = "+ "
TARGET_CHANNEL_ID = -1002964835173

# --- METATRADER 5 AYARLARI ---
MT5_TERMINAL_PATH = r"C:\PyTorchMT5\New_bot\MT5\terminal64.exe"
MT5_SERVER = "Tickmill-Live"
MT5_LOGIN =  131313
MT5_PASSWORD = "k3+emXS]>B5n"
MT5_SYMBOL = "XAUUSD"

# --- VERİTABANI AYARLARI ---
IMPACT_DB_PATH = "data/impact_history.db"
SIGNALS_DIR = "signals"
DATA_DIR = "data"

# --- ZAMAN AYARLARI ---
NEWS_BUFFER_FLUSH_INTERVAL_MIN = 10  # Haber biriktirme süresi (dakika)
PORTFOLIO_TIME_WINDOW_MIN = 10        # Aynı portföy sayılacak zaman penceresi
STANDARD_SLEEP_SECONDS = 14400       # Boşta bekleme süresi
PRE_NEWS_WINDOW_MIN = 15             # Haber öncesi uyarı penceresi
POST_NEWS_WINDOW_MIN = 60            # Haber sonrası takip penceresi

# --- DISPLAY TIMEZONE ---
DISPLAY_TIMEZONE = "Europe/Istanbul"  # Kullanıcıya gösterilecek saat dilimi

# --- RSS KAYNAKLARI ---
RSS_URLS = [
    "https://www.forexlive.com/feed",
    "https://www.investing.com/rss/news_1.rss",
    "https://www.investing.com/rss/news_11.rss",
    "https://www.fxstreet.com/rss/analysis",
    "https://www.dailyfx.com/feeds/forex-market-news"
]

# --- HABER TETİKLEYİCİ ANAHTAR KELİMELER ---
TRIGGER_KEYWORDS = [
    "trump", "fed", "powell", "fomc", "nfp", "payroll", "pce", "cpi", "ppi",
    "gold analyst", "jobless", "claims", "rate", "inflation", "war", "strike",
    "tariff", "forecast", "preview", "analyst", "opinion", "sentiment",
    "outlook", "goldman", "jpmorgan", "retail sales", "gdp", "housing",
    "manufacturing", "services", "pmi", "ism", "durable goods"
]

# --- OLAY SINIFLANDIRMA HARİTASI ---
EVENT_CLASSIFICATION_MAP = {
    "NFP": ["nfp", "non-farm", "payroll", "employment change"],
    "CPI": ["cpi", "consumer price", "inflation rate"],
    "PPI": ["ppi", "producer price"],
    "FOMC": ["fomc", "federal reserve", "fed rate", "interest rate decision"],
    "Jobless_Claims": ["jobless", "unemployment", "claims", "initial claims"],
    "GDP": ["gdp", "gross domestic", "economic growth"],
    "Retail_Sales": ["retail sales", "consumer spending"],
    "Fed_Speech": ["powell", "fed chair", "federal reserve speech", "fed speak"],
    "Housing": ["housing starts", "building permits", "home sales", "existing home"],
    "PMI": ["pmi", "purchasing managers", "ism manufacturing", "ism services"],
    "Durable_Goods": ["durable goods", "factory orders"],
    "Trade_Balance": ["trade balance", "trade deficit", "exports", "imports"],
    "Consumer_Confidence": ["consumer confidence", "consumer sentiment", "michigan"],
    "PCE": ["pce", "personal consumption", "core pce"],
}

# --- IMPACT SCORING AYARLARI ---
IMPACT_WEIGHTS = {
    "High": 1.0,
    "Medium": 0.6,
    "Low": 0.3
}

GEO_KEYWORDS = {
    "war", "iran", "trump", "fomc", "nfp", "tariff", "strike", 
    "geopolitical", "sanctions", "missile", "attack", "invasion"
}

# --- SEKANS HESAPLAMA ---
SEKANS_DIVISOR = 10000  # Fiyatın 1/10000'i = 1 sekans

# --- LOT VE DELAY HESAPLAMA PARAMETRELERİ ---
LOT_IMPACT_FACTOR = 0.85      # Impact'in lot çarpanına etkisi
MIN_LOT_MULTIPLIER = 0.1
MAX_LOT_MULTIPLIER = 1.0
MIN_DELAY_MIN = 5
MAX_DELAY_MIN = 45
DELAY_IMPACT_FACTOR = 25
DELAY_SPREAD_FACTOR = 5

# --- LOG AYARLARI ---
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"
