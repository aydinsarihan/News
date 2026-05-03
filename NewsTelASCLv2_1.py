# -*- coding: utf-8 -*-
"""
Proje: AI Destekli XAUUSD Sinyal ve Telegram Botu (Kurumsal Fon Mimarisi)
Versiyon: 2.1.0 (Batch Memory & Noise Reduction Edition)
Yenilikler:
    - Saat Başı (XX:55) Toplu Haber Değerlendirmesi (Hourly Summary)
    - Minör Veri ve Düşük Volatilite Filtrasyonu (Noise Reduction)
    - Gelişmiş Telegram Saatlik Bülten Formatı
    - UTC Zaman Standardizasyonu
    - Ekonomik Etki Veritabanı (ImpactHistoryDB)
    - Sekans Hesaplayıcı (Fiyat hareketi birimi)
"""

import os
import logging
import json
import threading
import asyncio
import xml.etree.ElementTree as ET
import time
import sqlite3
import multiprocessing
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, Any, List
from collections import defaultdict, deque
from dataclasses import dataclass, asdict, field

import pytz
import requests
import feedparser
from flask import Flask, jsonify
from telethon import TelegramClient
import MetaTrader5 as mt5
from difflib import SequenceMatcher
import psutil
import numpy as np

# --- KONFİGÜRASYON IMPORT ---
from config_db import (
    GEMINI_API_KEYS, TG_API_ID, TG_API_HASH, TG_PHONE, TARGET_CHANNEL_ID,
    MT5_TERMINAL_PATH, MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, MT5_SYMBOL,
    IMPACT_DB_PATH, SIGNALS_DIR, DATA_DIR, NEWS_BUFFER_FLUSH_INTERVAL_MIN,
    PORTFOLIO_TIME_WINDOW_MIN, STANDARD_SLEEP_SECONDS, PRE_NEWS_WINDOW_MIN,
    POST_NEWS_WINDOW_MIN, DISPLAY_TIMEZONE, RSS_URLS, TRIGGER_KEYWORDS,
    EVENT_CLASSIFICATION_MAP, IMPACT_WEIGHTS, GEO_KEYWORDS, SEKANS_DIVISOR,
    LOT_IMPACT_FACTOR, MIN_LOT_MULTIPLIER, MAX_LOT_MULTIPLIER, MIN_DELAY_MIN,
    MAX_DELAY_MIN, DELAY_IMPACT_FACTOR, DELAY_SPREAD_FACTOR, LOG_FORMAT, LOG_LEVEL
)

# --- DİZİN OLUŞTURMA ---
os.makedirs(SIGNALS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# --- LOGGING AYARLARI ---
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)
logging.getLogger("telethon").setLevel(logging.WARNING)

# --- GLOBAL KİLİTLER VE CACHE ---
json_lock = threading.Lock()
db_lock = threading.Lock()
RECENT_NEWS_CACHE = []
LAST_EVENT_STATE = {"id": "", "phase": ""}


# ============================================================================
# BÖLÜM 1: UTC ZAMAN YÖNETİCİSİ
# ============================================================================

class UTCTimeManager:
    @staticmethod
    def now() -> datetime:
        return datetime.now(pytz.UTC)
    
    @staticmethod
    def to_utc(dt: datetime, source_tz: str = None) -> datetime:
        if dt.tzinfo is None:
            if source_tz:
                source = pytz.timezone(source_tz)
                dt = source.localize(dt)
            else:
                dt = pytz.UTC.localize(dt)
        return dt.astimezone(pytz.UTC)
    
    @staticmethod
    def parse_calendar_time(date_str: str, time_str: str) -> datetime:
        eastern = pytz.timezone('US/Eastern')
        try:
            dt = eastern.localize(datetime.strptime(f"{date_str} {time_str}", "%m-%d-%Y %I:%M%p"))
            return dt.astimezone(pytz.UTC)
        except ValueError:
            return None
    
    @staticmethod
    def format_display(dt_utc: datetime, display_tz: str = DISPLAY_TIMEZONE) -> str:
        if dt_utc is None:
            return "N/A"
        local = dt_utc.astimezone(pytz.timezone(display_tz))
        tz_abbr = display_tz.split('/')[-1][:2].upper()
        return f"{dt_utc.strftime('%H:%M')} UTC ({local.strftime('%H:%M')} {tz_abbr})"
    
    @staticmethod
    def get_time_window_key(dt: datetime, window_min: int = 5) -> str:
        rounded_minute = (dt.minute // window_min) * window_min
        rounded = dt.replace(minute=rounded_minute, second=0, microsecond=0)
        return rounded.isoformat()


# ============================================================================
# BÖLÜM 2: SEKANS HESAPLAYICI
# ============================================================================

class SekansCalculator:
    @staticmethod
    def calculate(price_before: float, price_after: float) -> float:
        if price_before <= 0:
            return 0.0
        
        price_change = abs(price_after - price_before)
        factor = price_before / SEKANS_DIVISOR  
        sekans = price_change * factor
        
        if price_after < price_before:
            sekans = -sekans
        
        return round(sekans, 2)
    
    @staticmethod
    def to_price_change(sekans: float, reference_price: float) -> float:
        if reference_price <= 0:
            return 0.0
        factor = reference_price / SEKANS_DIVISOR
        if factor <= 0:
            return 0.0
        return round(abs(sekans) / factor, 2)
    
    @staticmethod
    def calculate_from_atr(atr: float, price: float) -> float:
        if price <= 0:
            return 0.0
        factor = price / SEKANS_DIVISOR
        return round(atr * factor, 2)
    
    @staticmethod
    def calculate_r_targets(entry_price: float, sl_price: float, direction: str = "BUY") -> Dict:
        sl_distance = abs(entry_price - sl_price)
        factor = entry_price / SEKANS_DIVISOR
        sl_sekans = sl_distance * factor
        
        r_multipliers = {"TP1": 0.7, "TP2": 1.2, "TP3": 2.5, "TP4": 4.0}
        
        targets = {
            "entry": entry_price,
            "sl": sl_price,
            "sl_sekans": round(sl_sekans, 2),
            "direction": direction
        }
        
        for tp_name, r_mult in r_multipliers.items():
            tp_sekans = sl_sekans * r_mult
            tp_distance = sl_distance * r_mult
            
            if direction == "BUY":
                tp_price = entry_price + tp_distance
            else:
                tp_price = entry_price - tp_distance
            
            targets[tp_name] = {
                "price": round(tp_price, 2),
                "sekans": round(tp_sekans, 2),
                "r_value": r_mult
            }
        
        return targets
    
    @staticmethod
    def format_sekans(sekans: float, reference_price: float = None) -> str:
        direction = "+" if sekans >= 0 else ""
        result = f"{direction}{sekans:.2f} sk"
        if reference_price and reference_price > 0:
            price_change = SekansCalculator.to_price_change(sekans, reference_price)
            result += f" (${direction}{price_change:.2f})"
        return result


# ============================================================================
# BÖLÜM 3: ETKİ VERİTABANI (ImpactHistoryDB)
# ============================================================================

@dataclass
class ImpactRecord:
    event_type: str
    event_time_utc: datetime
    forecast: Optional[str] = None
    actual: Optional[str] = None
    surprise_direction: str = "UNKNOWN"  
    impact_5min_sekans: float = 0.0
    impact_1hour_sekans: float = 0.0
    impact_direction: str = "FLAT"  
    pre_event_atr: float = 0.0
    pre_event_price: float = 0.0
    spread_at_release: float = 0.0
    notes: str = ""

class ImpactHistoryDB:
    def __init__(self, db_path: str = IMPACT_DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        self._create_tables()
    
    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    def _create_tables(self):
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS impact_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        event_time_utc TEXT NOT NULL,
                        forecast TEXT,
                        actual TEXT,
                        surprise_direction TEXT,
                        impact_5min_sekans REAL DEFAULT 0,
                        impact_1hour_sekans REAL DEFAULT 0,
                        impact_direction TEXT DEFAULT 'FLAT',
                        pre_event_atr REAL DEFAULT 0,
                        pre_event_price REAL DEFAULT 0,
                        spread_at_release REAL DEFAULT 0,
                        notes TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("CREATE INDEX IF NOT EXISTS idx_event_type ON impact_records(event_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_event_time ON impact_records(event_time_utc)")
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pending_1hour_updates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        impact_record_id INTEGER NOT NULL,
                        check_time_utc TEXT NOT NULL,
                        pre_price REAL NOT NULL,
                        completed INTEGER DEFAULT 0,
                        FOREIGN KEY (impact_record_id) REFERENCES impact_records(id)
                    )
                """)
                
                conn.commit()
            finally:
                conn.close()
    
    def record_impact(self, record: ImpactRecord) -> int:
        with db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("""
                    INSERT INTO impact_records 
                    (event_type, event_time_utc, forecast, actual, surprise_direction,
                     impact_5min_sekans, impact_1hour_sekans, impact_direction,
                     pre_event_atr, pre_event_price, spread_at_release, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.event_type,
                    record.event_time_utc.isoformat() if isinstance(record.event_time_utc, datetime) else record.event_time_utc,
                    record.forecast, record.actual, record.surprise_direction,
                    record.impact_5min_sekans, record.impact_1hour_sekans,
                    record.impact_direction, record.pre_event_atr,
                    record.pre_event_price, record.spread_at_release, record.notes
                ))
                conn.commit()
                return cursor.lastrowid
            finally:
                conn.close()
    
    def schedule_1hour_update(self, record_id: int, check_time_utc: datetime, pre_price: float):
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT INTO pending_1hour_updates 
                    (impact_record_id, check_time_utc, pre_price)
                    VALUES (?, ?, ?)
                """, (record_id, check_time_utc.isoformat(), pre_price))
                conn.commit()
            finally:
                conn.close()
    
    def update_1hour_impact(self, record_id: int, impact_1hour_sekans: float):
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE impact_records SET impact_1hour_sekans = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (impact_1hour_sekans, record_id))
                conn.execute("UPDATE pending_1hour_updates SET completed = 1 WHERE impact_record_id = ?", (record_id,))
                conn.commit()
            finally:
                conn.close()
    
    def get_pending_1hour_updates(self) -> List[Dict]:
        with db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("""
                    SELECT id, impact_record_id, check_time_utc, pre_price
                    FROM pending_1hour_updates
                    WHERE completed = 0 AND datetime(check_time_utc) <= datetime('now')
                """)
                rows = cursor.fetchall()
                return [{"id": r[0], "record_id": r[1], "check_time": r[2], "pre_price": r[3]} for r in rows]
            finally:
                conn.close()
    
    def get_historical_average(self, event_type: str, last_n: int = 20) -> Dict:
        with db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("""
                    SELECT 
                        AVG(impact_5min_sekans) as avg_5min,
                        AVG(impact_1hour_sekans) as avg_1hour,
                        AVG(ABS(impact_5min_sekans)) as avg_abs_5min,
                        AVG(ABS(impact_1hour_sekans)) as avg_abs_1hour,
                        COUNT(*) as sample_count,
                        SUM(CASE WHEN impact_direction='UP' THEN 1 ELSE 0 END) as up_count,
                        SUM(CASE WHEN impact_direction='DOWN' THEN 1 ELSE 0 END) as down_count,
                        MAX(ABS(impact_5min_sekans)) as max_5min,
                        MIN(impact_5min_sekans) as min_5min
                    FROM (
                        SELECT * FROM impact_records 
                        WHERE event_type = ?
                        ORDER BY event_time_utc DESC
                        LIMIT ?
                    )
                """, (event_type, last_n))
                row = cursor.fetchone()
                
                sample_count = row[4] or 0
                up_count = row[5] or 0
                down_count = row[6] or 0
                total_directional = up_count + down_count
                
                return {
                    "event_type": event_type,
                    "avg_5min_sekans": round(row[0] or 0, 2),
                    "avg_1hour_sekans": round(row[1] or 0, 2),
                    "avg_abs_5min_sekans": round(row[2] or 0, 2),
                    "avg_abs_1hour_sekans": round(row[3] or 0, 2),
                    "sample_count": sample_count,
                    "up_count": up_count,
                    "down_count": down_count,
                    "bullish_probability": round(up_count / total_directional, 2) if total_directional > 0 else 0.5,
                    "max_5min_sekans": round(row[7] or 0, 2),
                    "min_5min_sekans": round(row[8] or 0, 2),
                    "has_sufficient_data": sample_count >= 5
                }
            finally:
                conn.close()
    
    def get_surprise_statistics(self, event_type: str) -> Dict:
        with db_lock:
            conn = self._get_connection()
            try:
                result = {}
                for direction in ["BEAT", "MISS", "INLINE"]:
                    cursor = conn.execute("""
                        SELECT 
                            AVG(impact_5min_sekans),
                            AVG(impact_1hour_sekans),
                            COUNT(*)
                        FROM impact_records
                        WHERE event_type = ? AND surprise_direction = ?
                    """, (event_type, direction))
                    row = cursor.fetchone()
                    result[direction] = {
                        "avg_5min": round(row[0] or 0, 2),
                        "avg_1hour": round(row[1] or 0, 2),
                        "count": row[2] or 0
                    }
                return result
            finally:
                conn.close()


# ============================================================================
# BÖLÜM 4: HABER BİRİKTİRİCİ (SAATLİK BATCH - YENİ ENTEGRASYON)
# ============================================================================

@dataclass
class BufferedNews:
    title: str
    source: str
    received_utc: datetime
    event_type: str = "OTHER"
    raw_data: Dict = field(default_factory=dict)

class NewsBuffer:
    def __init__(self):
        self.buffer: List[BufferedNews] = []
        self.last_flush_hour: int = -1
        self._lock = threading.Lock()
    
    def _is_duplicate_in_buffer(self, title: str, threshold: float = 0.6) -> bool:
        title_lower = title.lower().strip()
        for existing in self.buffer:
            existing_lower = existing.title.lower().strip()
            if title_lower == existing_lower:
                return True
            ratio = SequenceMatcher(None, title_lower, existing_lower).ratio()
            if ratio >= threshold:
                return True
        return False
    
    def add(self, title: str, source: str, raw_data: Dict = None) -> bool:
        with self._lock:
            if self._is_duplicate_in_buffer(title):
                return self._should_flush()
            
            news = BufferedNews(
                title=title,
                source=source,
                received_utc=UTCTimeManager.now(),
                raw_data=raw_data or {}
            )
            self.buffer.append(news)
            logger.info(f"[NewsBuffer] 📥 Tampon +1: {title[:50]}... (Toplam: {len(self.buffer)})")
            return self._should_flush()
    
    def _should_flush(self) -> bool:
        """Saat 55. ile 59. dakikalar arasında ve o saat içinde henüz flush yapılmadıysa True"""
        now = UTCTimeManager.now()
        if 55 <= now.minute <= 59 and self.last_flush_hour != now.hour:
            return True
        return False
    
    def force_flush_check(self) -> bool:
        return self._should_flush() and len(self.buffer) > 0
    
    def flush(self) -> Dict[str, List[BufferedNews]]:
        with self._lock:
            if not self.buffer:
                return {}
            
            groups = {"HOURLY_BATCH": list(self.buffer)}
            flushed_count = len(self.buffer)
            self.buffer = []
            self.last_flush_hour = UTCTimeManager.now().hour
            
            logger.info(f"[NewsBuffer] 🚀 Saatlik Flush tamamlandı: {flushed_count} haber")
            return groups
    
    def get_buffer_status(self) -> Dict:
        with self._lock:
            return {
                "count": len(self.buffer),
                "last_flush_hour": self.last_flush_hour,
                "titles": [n.title[:40] for n in self.buffer[-5:]]
            }


# ============================================================================
# BÖLÜM 5: OLAY SINIFLANDIRICI
# ============================================================================

class EventClassifier:
    @staticmethod
    def classify(title: str) -> str:
        title_lower = title.lower()
        for event_type, keywords in EVENT_CLASSIFICATION_MAP.items():
            if any(kw in title_lower for kw in keywords):
                return event_type
        return "OTHER"
    
    @staticmethod
    def get_surprise_direction(forecast: str, actual: str) -> str:
        if not forecast or not actual or forecast == "Yok" or actual == "Yok":
            return "UNKNOWN"
        try:
            def extract_number(s: str) -> float:
                import re
                s = s.replace('%', '').replace('K', '000').replace('M', '000000').replace('k', '000').replace('m', '000000')
                numbers = re.findall(r'-?\d+\.?\d*', s)
                return float(numbers[0]) if numbers else None
            
            f_val = extract_number(forecast)
            a_val = extract_number(actual)
            
            if f_val is None or a_val is None:
                return "UNKNOWN"
            
            diff_pct = abs(a_val - f_val) / abs(f_val) * 100 if f_val != 0 else 0
            
            if diff_pct < 5: return "INLINE"
            elif a_val > f_val: return "BEAT"
            else: return "MISS"
                
        except Exception:
            return "UNKNOWN"
    
    @staticmethod
    def is_portfolio_event(news_group: List) -> bool:
        return len(news_group) >= 2


# ============================================================================
# BÖLÜM 6: VERİ MODELİ - XAUUSDSignal
# ============================================================================

@dataclass
class XAUUSDSignal:
    symbol: str = "XAUUSD"
    direction: str = "WAIT"
    risk_level: str = "MEDIUM"
    message: str = "System initializing..."
    news_event: str = ""
    event_type: str = "OTHER"
    surprise_sigma: float = 0.0
    expected_pips: int = 0
    expected_sekans: float = 0.0
    expected_duration_min: int = 0
    current_phase: str = "NORMALIZE"
    tp_multiplier: float = 1.0
    sl_multiplier: float = 1.0
    lot_multiplier: float = 1.0
    pause_duration_min: int = 0
    mt5_bid: float = 0.0
    spread: float = 0.0
    atr_14: float = 0.0
    atr_sekans: float = 0.0
    impact_score: float = 0.0
    consensus_strength: float = 0.0
    historical_avg_5min: float = 0.0
    historical_avg_1hour: float = 0.0
    bullish_probability: float = 0.5
    portfolio_mode: bool = False
    portfolio_events: List[str] = field(default_factory=list)
    last_update: str = field(default_factory=lambda: UTCTimeManager.now().isoformat())

    def to_dict(self) -> Dict:
        return asdict(self)

    def save_to_file(self):
        with json_lock:
            try:
                with open(f"{SIGNALS_DIR}/XAUUSD_live.json", "w", encoding="utf-8") as f:
                    json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Sinyal dosyaya yazılamadı: {e}")

current_signal: XAUUSDSignal = XAUUSDSignal()


# ============================================================================
# BÖLÜM 7: IMPACT HESAPLAYICI
# ============================================================================

class NewsImpactEngine:
    @staticmethod
    def calculate_impact(title: str, calendar_impact: str = "Medium", 
                         atr_14: float = 0.0, spread: float = 0.0,
                         historical_data: Dict = None) -> float:
        score = IMPACT_WEIGHTS.get(calendar_impact, 0.5)
        
        lower = title.lower()
        geo_count = sum(1 for k in GEO_KEYWORDS if k in lower)
        score += geo_count * 0.15
        
        if atr_14 > 0 or spread > 0:
            vol_factor = min(1.0, (atr_14 / 50.0) + (spread / 50.0))
            score = score * (1 + vol_factor * 0.3)
        
        if historical_data and historical_data.get('has_sufficient_data'):
            avg_abs = historical_data.get('avg_abs_5min_sekans', 0)
            if avg_abs > 50: score += 0.15
            elif avg_abs > 25: score += 0.08
        
        return round(min(1.0, max(0.1, score)), 2)
    
    @staticmethod
    def map_to_params(impact: float, spread: float) -> Tuple[float, int]:
        lot_mult = max(MIN_LOT_MULTIPLIER, round(MAX_LOT_MULTIPLIER - impact * LOT_IMPACT_FACTOR, 1))
        delay_min = max(MIN_DELAY_MIN, int(round(impact * DELAY_IMPACT_FACTOR + (spread / DELAY_SPREAD_FACTOR))))
        return lot_mult, min(MAX_DELAY_MIN, delay_min)
    
    @staticmethod
    def calculate_expected_sekans(event_type: str, impact_db: ImpactHistoryDB,
                                   surprise_direction: str = None) -> Tuple[float, float]:
        historical = impact_db.get_historical_average(event_type)
        if not historical.get('has_sufficient_data'):
            return 25.0, 50.0
        
        if surprise_direction and surprise_direction != "UNKNOWN":
            surprise_stats = impact_db.get_surprise_statistics(event_type)
            if surprise_direction in surprise_stats:
                s_data = surprise_stats[surprise_direction]
                if s_data['count'] >= 3:
                    return abs(s_data['avg_5min']), abs(s_data['avg_1hour'])
        
        return historical['avg_abs_5min_sekans'], historical['avg_abs_1hour_sekans']


# ============================================================================
# BÖLÜM 8: PORTFÖY ANALİZCİSİ
# ============================================================================

class PortfolioAnalyzer:
    def __init__(self, impact_db: ImpactHistoryDB):
        self.impact_db = impact_db
    
    def analyze_portfolio(self, news_group: List[BufferedNews], mt5_context: Dict) -> Dict:
        if len(news_group) < 2: return None
        
        analysis_data = []
        total_weight = 0
        weighted_bullish = 0
        total_expected_5min = 0
        total_expected_1hour = 0
        
        for news in news_group:
            historical = self.impact_db.get_historical_average(news.event_type)
            weight = historical.get('sample_count', 1)
            analysis_data.append({
                "title": news.title,
                "event_type": news.event_type,
                "historical": historical,
                "weight": weight
            })
            total_weight += weight
            weighted_bullish += historical.get('bullish_probability', 0.5) * weight
            total_expected_5min += historical.get('avg_abs_5min_sekans', 20)
            total_expected_1hour += historical.get('avg_abs_1hour_sekans', 40)
        
        final_bullish_prob = weighted_bullish / total_weight if total_weight > 0 else 0.5
        
        if final_bullish_prob > 0.58:
            direction = "BUY"
            confidence = (final_bullish_prob - 0.5) * 2
        elif final_bullish_prob < 0.42:
            direction = "SELL"
            confidence = (0.5 - final_bullish_prob) * 2
        else:
            direction = "WAIT"
            confidence = 0.3
        
        avg_impact = NewsImpactEngine.calculate_impact(
            " | ".join([n.title for n in news_group]), "High",
            mt5_context.get('atr', 0), mt5_context.get('spread', 0)
        )
        
        lot_mult, delay = NewsImpactEngine.map_to_params(avg_impact, mt5_context.get('spread', 0))
        
        return {
            "type": "PORTFOLIO",
            "direction": direction,
            "risk_level": "HIGH" if avg_impact > 0.7 else "MEDIUM",
            "confidence": round(confidence, 2),
            "event_count": len(news_group),
            "events": [{"title": n.title, "type": n.event_type} for n in news_group],
            "lot_multiplier": lot_mult,
            "pause_duration_min": delay,
            "impact_score": avg_impact,
            "expected_5min_sekans": round(total_expected_5min, 1),
            "expected_1hour_sekans": round(total_expected_1hour, 1),
            "bullish_probability": round(final_bullish_prob, 2),
            "analysis_details": analysis_data
        }


# ============================================================================
# BÖLÜM 9: TEK HABER ANALİZCİSİ (PRE/POST FAZ)
# ============================================================================

class SingleNewsAnalyzer:
    def __init__(self, impact_db: ImpactHistoryDB):
        self.impact_db = impact_db
    
    def analyze_pre_news(self, event: Dict, mt5_context: Dict) -> Dict:
        event_type = EventClassifier.classify(event.get('title', ''))
        historical = self.impact_db.get_historical_average(event_type)
        exp_5min, exp_1hour = NewsImpactEngine.calculate_expected_sekans(event_type, self.impact_db)
        
        impact = NewsImpactEngine.calculate_impact(
            event.get('title', ''), event.get('impact', 'High'),
            mt5_context.get('atr', 0), mt5_context.get('spread', 0), historical
        )
        lot_mult, delay = NewsImpactEngine.map_to_params(impact, mt5_context.get('spread', 0))
        
        return {
            "type": "PRE_NEWS",
            "direction": "WAIT",
            "risk_level": "HIGH" if impact > 0.7 else "MEDIUM",
            "event_title": event.get('title', ''),
            "event_type": event_type,
            "event_time_utc": event.get('time_utc'),
            "forecast": event.get('forecast', 'N/A'),
            "lot_multiplier": lot_mult,
            "pause_duration_min": delay + 5,
            "impact_score": impact,
            "expected_5min_sekans": exp_5min,
            "expected_1hour_sekans": exp_1hour,
            "historical_bullish_prob": historical.get('bullish_probability', 0.5),
            "sample_count": historical.get('sample_count', 0),
            "recommendation": "Avoid opening positions before news. Volatility spike expected."
        }
    
    def analyze_post_news(self, event: Dict, mt5_context: Dict, pre_price: float, post_price: float) -> Dict:
        event_type = EventClassifier.classify(event.get('title', ''))
        historical = self.impact_db.get_historical_average(event_type)
        actual_5min_sekans = SekansCalculator.calculate(pre_price, post_price)
        surprise = EventClassifier.get_surprise_direction(event.get('forecast', ''), event.get('actual', ''))
        
        if actual_5min_sekans > 10: direction, risk = "BUY", "MEDIUM"
        elif actual_5min_sekans < -10: direction, risk = "SELL", "MEDIUM"
        else: direction, risk = "WAIT", "LOW"
        
        if historical.get('has_sufficient_data'):
            avg_move = historical.get('avg_abs_5min_sekans', 25)
            if abs(actual_5min_sekans) > avg_move * 1.5: risk = "HIGH"
        
        impact = NewsImpactEngine.calculate_impact(
            event.get('title', ''), event.get('impact', 'High'),
            mt5_context.get('atr', 0), mt5_context.get('spread', 0), historical
        )
        lot_mult, delay = NewsImpactEngine.map_to_params(impact, mt5_context.get('spread', 0))
        _, exp_1hour = NewsImpactEngine.calculate_expected_sekans(event_type, self.impact_db, surprise)
        
        return {
            "type": "POST_NEWS",
            "direction": direction,
            "risk_level": risk,
            "event_title": event.get('title', ''),
            "event_type": event_type,
            "forecast": event.get('forecast', 'N/A'),
            "actual": event.get('actual', 'N/A'),
            "surprise_direction": surprise,
            "lot_multiplier": lot_mult,
            "pause_duration_min": delay,
            "impact_score": impact,
            "actual_5min_sekans": actual_5min_sekans,
            "expected_1hour_sekans": exp_1hour,
            "pre_price": pre_price,
            "post_price": post_price,
            "price_change": round(post_price - pre_price, 2)
        }


# ============================================================================
# BÖLÜM 10: GEMİNİ AI ENTEGRASYONU (MİNÖR FİLTRESİ EKLENDİ)
# ============================================================================

class GeminiAnalyzer:
    def __init__(self):
        self.api_index = 0
        self.last_call_time = 0
        self.min_call_interval = 2
    
    def analyze(self, data_text: str, phase: str, mt5_context: Dict, historical_context: str = "") -> Optional[Dict]:
        elapsed = time.time() - self.last_call_time
        if elapsed < self.min_call_interval:
            time.sleep(self.min_call_interval - elapsed)
        
        context_str = f"""
[CANLI MT5] Bid:{mt5_context.get('bid', 0):.2f} Spread:{mt5_context.get('spread', 0)} ATR14:{mt5_context.get('atr', 0):.2f}
{historical_context}
"""
        base_json = '''{
    "direction": "BUY/SELL/WAIT",
    "risk_level": "LOW/MEDIUM/HIGH",
    "lot_multiplier": 0.1-1.0,
    "pause_duration_min": 5-45,
    "impact_score": 0.1-1.0,
    "message": "Detailed analysis..."
}'''
        
        prompts = {
            "HOURLY_SUMMARY": f"""Hourly News Accumulation (Batch Evaluation): {data_text}
{context_str}
Filter out minor noise. Summarize the overall macroeconomic sentiment for XAUUSD from these headlines. If the news is minor or lacks market impact, set impact_score below 0.4 and risk_level to LOW.
RESPOND EXACTLY IN JSON: {base_json}""",

            "PRE_NEWS": f"""Upcoming Economic Event: {data_text}
{context_str}
Analyze pre-release market conditions. Consider historical patterns and current positioning.
RESPOND EXACTLY IN JSON: {base_json}""",

            "POST_NEWS": f"""News Released! {data_text}
{context_str}
Analyze immediate market reaction and expected continuation.
RESPOND EXACTLY IN JSON: {base_json}""",

            "BREAKING": f"""Breaking News: "{data_text}"
{context_str}
Analyze impact on XAUUSD. If the news is trivial, set impact_score to < 0.4 and risk_level to LOW.
RESPOND EXACTLY IN JSON: {base_json}""",

            "PORTFOLIO": f"""Multiple Events Released: {data_text}
{context_str}
Analyze combined impact. Weight events by importance.
RESPOND EXACTLY IN JSON: {base_json}"""
        }
        
        prompt = prompts.get(phase, prompts["BREAKING"])
        headers = {'Content-Type': 'application/json'}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        
        for key_attempt in range(len(GEMINI_API_KEYS)):
            api_key = GEMINI_API_KEYS[(self.api_index + key_attempt) % len(GEMINI_API_KEYS)]
            api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
            
            for attempt in range(2):
                try:
                    self.last_call_time = time.time()
                    response = requests.post(api_url, headers=headers, json=payload, timeout=25)
                    
                    if response.status_code == 429:
                        time.sleep(5 + (attempt * 3))
                        continue
                    
                    if response.status_code == 200:
                        text = response.json()['candidates'][0]['content']['parts'][0]['text']
                        clean_text = text.replace("```json", "").replace("```", "").strip()
                        result = json.loads(clean_text)
                        
                        self.api_index = (self.api_index + key_attempt) % len(GEMINI_API_KEYS)
                        logger.info(f"[AI] ✅ Analiz başarılı (Key {key_attempt+1})")
                        return result
                    
                    response.raise_for_status()
                    
                except json.JSONDecodeError as je:
                    logger.warning(f"[AI] JSON parse hatası: {je}")
                    continue
                except Exception as e:
                    logger.warning(f"[AI] ❌ Deneme {attempt+1}/2 (Key {key_attempt+1}): {e}")
                    time.sleep(2)
            
            self.api_index = (self.api_index + 1) % len(GEMINI_API_KEYS)
        
        logger.error(f"[AI] ❌ Tüm API key'ler başarısız oldu")
        return None


# ============================================================================
# BÖLÜM 11: SİNYAL BİRLEŞTİRİCİ VE ZENGİNLEŞTİRİCİ
# ============================================================================

class SignalAggregator:
    def __init__(self):
        self.recent_signals: deque = deque(maxlen=10)
    
    def add_signal(self, title: str, ai_decision: Dict, impact: float):
        self.recent_signals.append({
            "title": title,
            "direction": ai_decision.get("direction", "WAIT"),
            "impact": impact,
            "timestamp": time.time()
        })
    
    def get_consensus(self) -> Optional[Dict]:
        current_time = time.time()
        valid_signals = [s for s in self.recent_signals if current_time - s["timestamp"] <= 2700]
        self.recent_signals = deque(valid_signals, maxlen=10)
        
        if len(self.recent_signals) < 2: return None
        
        buy_score = sum(s["impact"] for s in self.recent_signals if s["direction"] == "BUY")
        sell_score = sum(s["impact"] for s in self.recent_signals if s["direction"] == "SELL")
        total = buy_score + sell_score
        
        if total < 0.3: return None
        
        direction = "BUY" if buy_score > sell_score else "SELL"
        avg_impact = sum(s["impact"] for s in self.recent_signals) / len(self.recent_signals)
        lot, delay = NewsImpactEngine.map_to_params(avg_impact, current_signal.spread)
        
        return {
            "direction": direction,
            "risk_level": "HIGH" if avg_impact > 0.7 else "MEDIUM",
            "lot_multiplier": lot,
            "pause_duration_min": delay,
            "impact_score": round(avg_impact, 2),
            "consensus_strength": round(abs(buy_score - sell_score) / total, 2) if total > 0 else 0,
            "message": f"Consolidated from {len(self.recent_signals)} events"
        }

class SignalEnricher:
    @staticmethod
    def enrich(signal: XAUUSDSignal, analysis: Dict, phase: str) -> XAUUSDSignal:
        signal.direction = analysis.get("direction", "WAIT")
        signal.risk_level = analysis.get("risk_level", "MEDIUM")
        signal.lot_multiplier = float(analysis.get("lot_multiplier", 0.5))
        signal.pause_duration_min = int(analysis.get("pause_duration_min", 15))
        signal.impact_score = float(analysis.get("impact_score", 0.5))
        signal.current_phase = phase
        signal.message = analysis.get("message", "")
        signal.news_event = analysis.get("event_title", "")
        signal.event_type = analysis.get("event_type", "OTHER")
        signal.expected_sekans = float(analysis.get("expected_5min_sekans", 0))
        signal.historical_avg_5min = float(analysis.get("historical_avg_5min", 0))
        signal.historical_avg_1hour = float(analysis.get("historical_avg_1hour", 0))
        signal.bullish_probability = float(analysis.get("bullish_probability", 0.5))
        signal.consensus_strength = float(analysis.get("consensus_strength", 0))
        
        if analysis.get("type") == "PORTFOLIO" or phase == "HOURLY_SUMMARY":
            signal.portfolio_mode = True
            signal.portfolio_events = [e.get("title", "") if isinstance(e, dict) else e for e in analysis.get("events", [])]
        else:
            signal.portfolio_mode = False
            signal.portfolio_events = []
        
        signal.last_update = UTCTimeManager.now().isoformat()
        signal.save_to_file()
        return signal


# ============================================================================
# BÖLÜM 12: TELEGRAM MESAJ FORMATLAYICI
# ============================================================================

class TelegramNotifier:
    def __init__(self, api_id: int, api_hash: str, phone: str, target_channel: int):
        self.target_channel = target_channel
        self.client = TelegramClient('session_name', api_id, api_hash)
        self.phone = phone
        self.connected = False

    async def connect(self) -> None:
        try:
            await self.client.start(phone=self.phone)
            self.connected = True
            logger.info("✅ Telegram Client bağlandı.")
        except Exception as e:
            logger.error(f"❌ Telegram bağlantı hatası: {e}")
            self.connected = False

    async def send_message(self, text: str, retry_count: int = 3) -> bool:
        if not self.connected:
            logger.warning("[Telegram] ⚠️ Bağlantı yok, yeniden bağlanılıyor...")
            await self.connect()
        
        for attempt in range(retry_count):
            try:
                await self.client.send_message(self.target_channel, text, parse_mode='md')
                logger.info(f"[Telegram] ✅ Mesaj gönderildi ({len(text)} karakter)")
                return True
            except Exception as e:
                logger.warning(f"[Telegram] ❌ Gönderim hatası (Deneme {attempt+1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    await asyncio.sleep(2)
                    try: await self.connect()
                    except: pass
        
        logger.error(f"[Telegram] ❌ Mesaj gönderilemedi (Tüm denemeler başarısız)")
        return False

    @staticmethod
    def format_hourly_summary(analysis: Dict, news_list: List[BufferedNews]) -> str:
        events_str = "\n".join([f"🔸 {n.title[:60]}..." for n in news_list])
        return f"""🗞️ *HOURLY MARKET BRIEF* (Batch Summary)

*Collected Headlines:*
{events_str}

🧠 *AI Synthesis:*
{analysis.get('message', 'Market exhibits standard volatility.')}

🎯 *Consensus Signal:*
├ Direction: *{analysis.get('direction', 'WAIT')}*
├ Risk: {analysis.get('risk_level', 'MEDIUM')}
└ Impact Score: {analysis.get('impact_score', 0.5):.2f}"""

    @staticmethod
    def format_pre_news(event: Dict, analysis: Dict) -> str:
        time_display = UTCTimeManager.format_display(event.get('time_utc'))
        hist_info = ""
        if analysis.get('sample_count', 0) > 0:
            hist_info = f"""
📈 *Historical Data ({analysis.get('sample_count', 0)} samples):*
├ Avg 5min impact: {analysis.get('expected_5min_sekans', 0):.2f} sk
├ Avg 1hour impact: {analysis.get('expected_1hour_sekans', 0):.2f} sk
└ Bullish probability: {analysis.get('historical_bullish_prob', 0.5)*100:.0f}%
"""
        return f"""🚨 *UPCOMING CRITICAL EVENT*
⏰ {time_display}

🇺🇸 *{event.get('title', 'Economic Data')}*
📊 Forecast: {event.get('forecast', 'N/A')}
{hist_info}
⚠️ *Risk Assessment:*
├ Impact Score: {analysis.get('impact_score', 0):.2f}
├ Risk Level: {analysis.get('risk_level', 'HIGH')}
└ Pause Duration: {analysis.get('pause_duration_min', 15)} mins

🛡️ *Advice:* {analysis.get('recommendation', 'Avoid opening positions before news release.')}
🤖 *EA Directive:* SAFE MODE - Lot: {analysis.get('lot_multiplier', 0.2)}x"""

    @staticmethod
    def format_post_news(event: Dict, analysis: Dict) -> str:
        surprise_emoji = {"BEAT": "✅", "MISS": "❌", "INLINE": "➖"}.get(analysis.get('surprise_direction', 'UNKNOWN'), "❓")
        sekans = analysis.get('actual_5min_sekans', 0)
        sekans_display = SekansCalculator.format_sekans(sekans, analysis.get('pre_price', 0))
        
        return f"""⚡ *NEWS RELEASED*

🇺🇸 *{event.get('title', 'Economic Data')}*
📊 Forecast: {event.get('forecast', 'N/A')} | Actual: {event.get('actual', 'N/A')} {surprise_emoji}

📈 *Immediate Impact:*
├ 5min Move: {sekans_display}
├ Price: {analysis.get('pre_price', 0):.2f} → {analysis.get('post_price', 0):.2f}
└ Expected 1hr: ~{analysis.get('expected_1hour_sekans', 0):.2f} sk

🎯 *Signal:*
├ Direction: *{analysis.get('direction', 'WAIT')}*
├ Risk: {analysis.get('risk_level', 'MEDIUM')}
├ Lot Multiplier: {analysis.get('lot_multiplier', 0.5)}x
└ Delay: {analysis.get('pause_duration_min', 15)} mins"""

    @staticmethod
    def format_breaking(title: str, analysis: Dict) -> str:
        return f"""🚨 *BREAKING NEWS*
📰 {title}

🧠 *AI Analysis:*
{analysis.get('message', 'Analysis in progress...')}

🎯 *Signal:*
├ Direction: *{analysis.get('direction', 'WAIT')}* | Risk: {analysis.get('risk_level', 'MEDIUM')}
├ Lot Multiplier: {analysis.get('lot_multiplier', 0.5)}x (Impact: {analysis.get('impact_score', 0.5):.2f})
└ Delay: {analysis.get('pause_duration_min', 15)} mins"""


# ============================================================================
# BÖLÜM 13: MARKET ANALİZCİSİ (TAKVİM)
# ============================================================================

class MarketAnalyzer:
    def __init__(self, impact_db: ImpactHistoryDB):
        self.impact_db = impact_db
        self.single_analyzer = SingleNewsAnalyzer(impact_db)
        self.ai_analyzer = GeminiAnalyzer()
    
    def get_timeline_state(self) -> Tuple[str, Dict[str, Any], int]:
        global LAST_EVENT_STATE
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
        now_utc = UTCTimeManager.now()
        events = []
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            
            for event in root.findall('event'):
                if event.findtext('country') == "USD" and event.findtext('impact') == "High":
                    date_str = event.findtext('date')
                    time_str = event.findtext('time')
                    if not time_str or ("am" not in time_str.lower() and "pm" not in time_str.lower()):
                        continue
                    dt_utc = UTCTimeManager.parse_calendar_time(date_str, time_str)
                    if dt_utc is None:
                        continue
                    events.append({
                        "id": f"{event.findtext('title')}_{dt_utc.isoformat()}",
                        "title": event.findtext('title'),
                        "forecast": event.findtext('forecast', 'N/A'),
                        "actual": event.findtext('actual', 'N/A'),
                        "impact": event.findtext('impact', 'High'),
                        "time_utc": dt_utc
                    })
        except Exception as e:
            logger.error(f"[CALENDAR] ❌ XML hatası: {e}")
            return "IDLE", {}, 60
        
        events.sort(key=lambda x: x["time_utc"])
        
        for e in events:
            t = e["time_utc"]
            pre_start = t - timedelta(minutes=PRE_NEWS_WINDOW_MIN)
            if pre_start <= now_utc < t:
                if LAST_EVENT_STATE.get("id") != e["id"] or LAST_EVENT_STATE.get("phase") != "PRE_NEWS":
                    LAST_EVENT_STATE.update({"id": e["id"], "phase": "PRE_NEWS"})
                    sleep_to_post = (t + timedelta(minutes=5) - now_utc).total_seconds()
                    return "PRE_NEWS", e, int(max(sleep_to_post, 60))
            
            post_start = t + timedelta(minutes=5)
            post_end = t + timedelta(minutes=POST_NEWS_WINDOW_MIN)
            if post_start <= now_utc < post_end:
                if LAST_EVENT_STATE.get("id") != e["id"] or LAST_EVENT_STATE.get("phase") != "POST_NEWS":
                    LAST_EVENT_STATE.update({"id": e["id"], "phase": "POST_NEWS"})
                    sleep_to_norm = (post_end - now_utc).total_seconds()
                    return "POST_NEWS", e, int(max(sleep_to_norm, 60))
        
        upcoming = [e for e in events if e['time_utc'] > now_utc]
        if upcoming:
            next_e = upcoming[0]
            sleep_time = (next_e['time_utc'] - timedelta(minutes=PRE_NEWS_WINDOW_MIN) - now_utc).total_seconds()
            return "IDLE", next_e, int(max(sleep_time, 60))
        
        return "IDLE", {}, STANDARD_SLEEP_SECONDS
    
    def analyze_with_ai(self, data_text: str, phase: str, mt5_context: Dict) -> Optional[Dict]:
        event_type = EventClassifier.classify(data_text)
        historical = self.impact_db.get_historical_average(event_type)
        hist_context = ""
        if historical.get('has_sufficient_data'):
            hist_context = f"\n[TARİHSEL VERİ - {event_type}]\nÖrnek: {historical['sample_count']}\nOrt 5dk etki: {historical['avg_5min_sekans']:.1f} sk\nBullish olasılık: %{historical['bullish_probability']*100:.0f}"
        return self.ai_analyzer.analyze(data_text, phase, mt5_context, hist_context)


# ============================================================================
# BÖLÜM 14: MT5 VERİ TOPLAYICI
# ============================================================================

class MT5DataCollector:
    def __init__(self, path: str, server: str, login: int, password: str, symbol: str):
        self.path = path
        self.server = server
        self.login = login
        self.password = password
        self.symbol = symbol
        self.connected = False
        self.pre_news_price = None
    
    def _kill_own_mt5(self):
        target_path = os.path.normpath(self.path).lower()
        for proc in psutil.process_iter(['pid', 'name', 'exe']):
            try:
                if proc.info['name'] == 'terminal64.exe':
                    proc_exe = proc.info['exe']
                    if proc_exe and os.path.normpath(proc_exe).lower() == target_path:
                        logger.warning(f"[MT5] Asılı process sonlandırılıyor (PID: {proc.info['pid']})")
                        proc.kill()
                        proc.wait(timeout=3)
            except: pass
    
    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="MT5_Collector").start()
    
    def _loop(self):
        global current_signal
        logger.info(f"[MT5] 🟢 {self.symbol} veri toplayıcı başlatıldı.")
        while True:
            try:
                if not self.connected:
                    self._kill_own_mt5()
                    time.sleep(1)
                    self.connected = mt5.initialize(path=self.path, login=self.login, password=self.password, server=self.server, portable=True)
                    if not self.connected:
                        logger.error(f"[MT5] ❌ Bağlantı hatası: {mt5.last_error()}")
                        time.sleep(5)
                        continue
                    if not mt5.symbol_select(self.symbol, True):
                        logger.warning(f"[MT5] ⚠️ {self.symbol} Market Watch'ta yok!")
                        time.sleep(5)
                        continue
                    logger.info("[MT5] ✅ Bağlantı başarılı.")
                
                self._fetch_live_data()
            except Exception as e:
                logger.error(f"[MT5] 💥 Hata: {e}")
                self.connected = False
            time.sleep(0.5)
    
    def _fetch_live_data(self):
        global current_signal
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            self.connected = False
            return
        point = mt5.symbol_info(self.symbol).point
        spread = round((tick.ask - tick.bid) / point) if point else (tick.ask - tick.bid) * 10
        current_signal.mt5_bid = tick.bid
        current_signal.spread = spread
        
        rates = mt5.copy_rates_from_pos(self.symbol, mt5.TIMEFRAME_H1, 0, 15)
        if rates is not None and len(rates) >= 14:
            high = np.array([r['high'] for r in rates])
            low = np.array([r['low'] for r in rates])
            close = np.array([r['close'] for r in rates])
            tr = np.maximum(high[1:] - low[1:], np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1]))
            atr = np.mean(tr[-14:])
            current_signal.atr_14 = round(atr, 2)
            current_signal.atr_sekans = SekansCalculator.calculate_from_atr(atr, tick.bid)
        
        current_signal.last_update = UTCTimeManager.now().isoformat()
        current_signal.save_to_file()
    
    def get_current_price(self) -> float: return current_signal.mt5_bid
    def store_pre_news_price(self):
        self.pre_news_price = current_signal.mt5_bid
        logger.info(f"[MT5] 📌 Pre-news fiyat kaydedildi: {self.pre_news_price}")
    def get_pre_news_price(self) -> float: return self.pre_news_price or current_signal.mt5_bid


# ============================================================================
# BÖLÜM 15: HABER CACHE YÖNETİCİSİ
# ============================================================================

def is_news_redundant(new_title: str, similarity_threshold: float = 0.55, cooldown_minutes: int = 60) -> bool:
    global RECENT_NEWS_CACHE
    now = UTCTimeManager.now()
    RECENT_NEWS_CACHE = [news for news in RECENT_NEWS_CACHE if now - news['time'] < timedelta(minutes=cooldown_minutes)]
    new_title_normalized = new_title.lower().strip()
    
    for news in RECENT_NEWS_CACHE:
        cached_title = news['title'].lower().strip()
        if new_title_normalized == cached_title: return True
        ratio = SequenceMatcher(None, new_title_normalized, cached_title).ratio()
        if ratio >= similarity_threshold: return True
        new_words, cached_words = set(new_title_normalized.split()), set(cached_title.split())
        if len(new_words) > 3 and len(cached_words) > 3:
            common_words = new_words.intersection(cached_words)
            if len(common_words) / min(len(new_words), len(cached_words)) >= 0.7: return True
    return False

def add_to_news_cache(title: str):
    global RECENT_NEWS_CACHE
    RECENT_NEWS_CACHE.append({'title': title, 'time': UTCTimeManager.now()})


# ============================================================================
# BÖLÜM 16: 1 SAATLİK ETKİ GÜNCELLEYİCİ
# ============================================================================

class HourlyImpactUpdater:
    def __init__(self, impact_db: ImpactHistoryDB, mt5_collector: MT5DataCollector):
        self.impact_db = impact_db
        self.mt5 = mt5_collector
    
    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="HourlyUpdater").start()
    
    def _loop(self):
        while True:
            try:
                pending = self.impact_db.get_pending_1hour_updates()
                for item in pending:
                    current_price = self.mt5.get_current_price()
                    sekans_1hour = SekansCalculator.calculate(item['pre_price'], current_price)
                    self.impact_db.update_1hour_impact(item['record_id'], sekans_1hour)
            except Exception as e: logger.error(f"[HourlyUpdater] Hata: {e}")
            time.sleep(60)


# ============================================================================
# BÖLÜM 17: ANA TİCARET MOTORU (NOISE REDUCTION VE SAATLİK BATCH ENTEGRE)
# ============================================================================

class TradingEngine:
    def __init__(self):
        self.impact_db = ImpactHistoryDB()
        self.telegram = TelegramNotifier(TG_API_ID, TG_API_HASH, TG_PHONE, TARGET_CHANNEL_ID)
        self.market_analyzer = MarketAnalyzer(self.impact_db)
        self.portfolio_analyzer = PortfolioAnalyzer(self.impact_db)
        self.single_analyzer = SingleNewsAnalyzer(self.impact_db)
        self.mt5_collector = MT5DataCollector(MT5_TERMINAL_PATH, MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, MT5_SYMBOL)
        self.news_buffer = NewsBuffer()
        self.aggregator = SignalAggregator()
        self.hourly_updater = HourlyImpactUpdater(self.impact_db, self.mt5_collector)
        self.ai_analyzer = GeminiAnalyzer()
    
    def _get_mt5_context(self) -> Dict:
        return {"bid": current_signal.mt5_bid, "spread": current_signal.spread, "atr": current_signal.atr_14, "atr_sekans": current_signal.atr_sekans}
    
    async def calendar_loop(self):
        global current_signal
        while True:
            try:
                phase, event_data, sleep_time = self.market_analyzer.get_timeline_state()
                mt5_ctx = self._get_mt5_context()
                
                if phase == "PRE_NEWS":
                    logger.info(f"[PRE_NEWS] 📅 {event_data.get('title', '')}")
                    self.mt5_collector.store_pre_news_price()
                    analysis = self.single_analyzer.analyze_pre_news(event_data, mt5_ctx)
                    ai_result = self.market_analyzer.analyze_with_ai(f"{event_data.get('title', '')} | Forecast: {event_data.get('forecast', '')}", "PRE_NEWS", mt5_ctx)
                    if ai_result: analysis['message'] = ai_result.get('message', '')
                    current_signal = SignalEnricher.enrich(current_signal, analysis, "PRE_NEWS")
                    msg = TelegramNotifier.format_pre_news(event_data, analysis)
                    await self.telegram.send_message(msg)
                
                elif phase == "POST_NEWS":
                    logger.info(f"[POST_NEWS] 📊 {event_data.get('title', '')}")
                    pre_price = self.mt5_collector.get_pre_news_price()
                    post_price = self.mt5_collector.get_current_price()
                    analysis = self.single_analyzer.analyze_post_news(event_data, mt5_ctx, pre_price, post_price)
                    ai_result = self.market_analyzer.analyze_with_ai(f"{event_data.get('title', '')} | Actual: {event_data.get('actual', '')} | Forecast: {event_data.get('forecast', '')}", "POST_NEWS", mt5_ctx)
                    if ai_result:
                        analysis['message'] = ai_result.get('message', '')
                        analysis['direction'] = ai_result.get('direction', analysis['direction'])
                    
                    record = ImpactRecord(
                        event_type=analysis.get('event_type', 'OTHER'),
                        event_time_utc=event_data.get('time_utc', UTCTimeManager.now()),
                        forecast=event_data.get('forecast'),
                        actual=event_data.get('actual'),
                        surprise_direction=analysis.get('surprise_direction', 'UNKNOWN'),
                        impact_5min_sekans=analysis.get('actual_5min_sekans', 0),
                        impact_direction="UP" if analysis.get('actual_5min_sekans', 0) > 5 else "DOWN" if analysis.get('actual_5min_sekans', 0) < -5 else "FLAT",
                        pre_event_atr=mt5_ctx.get('atr', 0),
                        pre_event_price=pre_price,
                        spread_at_release=mt5_ctx.get('spread', 0)
                    )
                    record_id = self.impact_db.record_impact(record)
                    check_time = UTCTimeManager.now() + timedelta(hours=1)
                    self.impact_db.schedule_1hour_update(record_id, check_time, pre_price)
                    
                    current_signal = SignalEnricher.enrich(current_signal, analysis, "POST_NEWS")
                    msg = TelegramNotifier.format_post_news(event_data, analysis)
                    await self.telegram.send_message(msg)
                
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logger.error(f"[Calendar Loop] Hata: {e}")
                await asyncio.sleep(60)
    
    async def news_sniper_loop(self):
        global current_signal
        logger.info("[News Sniper] 🎯 RSS Tarama ve Saatlik Batch devrede")
        while True:
            try:
                news_added = 0
                for url in RSS_URLS:
                    try:
                        feed = feedparser.parse(url)
                        source = url.split('/')[2]
                        for entry in feed.entries[:5]:
                            title = entry.title
                            if any(k in title.lower() for k in TRIGGER_KEYWORDS):
                                if not is_news_redundant(title):
                                    self.news_buffer.add(title, source, {'link': entry.get('link', '')})
                                    add_to_news_cache(title)
                                    news_added += 1
                    except Exception as e: logger.warning(f"[RSS] {url} hatası: {e}")
                
                if news_added > 0: logger.info(f"[News Sniper] 📰 Bu turda {news_added} yeni haber eklendi")
                
                if self.news_buffer.force_flush_check():
                    logger.info("[News Sniper] ⏰ Saat başı yaklaştı (XX:55), haberler işleniyor...")
                    await self._process_hourly_batch()
                
                await asyncio.sleep(180)
            except Exception as e:
                logger.error(f"[News Sniper] Döngü hatası: {e}")
                await asyncio.sleep(60)
    
    async def _process_hourly_batch(self):
        global current_signal
        groups = self.news_buffer.flush()
        if not groups: return
        
        mt5_ctx = self._get_mt5_context()
        for time_key, news_group in groups.items():
            logger.info(f"[Buffer] 📦 İşleniyor: {len(news_group)} haber (Saatlik)")
            try:
                titles = " | ".join([n.title for n in news_group])
                analysis = self.ai_analyzer.analyze(titles, "HOURLY_SUMMARY", mt5_ctx)
                
                if analysis:
                    impact = float(analysis.get('impact_score', 0))
                    risk = analysis.get('risk_level', 'LOW')
                    
                    # === NOISE REDUCTION (MİNÖR FİLTRESİ) ===
                    if impact < 0.40 or risk == 'LOW':
                        logger.info(f"[FİLTRE] 🛑 Minör piyasa etkisi (Impact: {impact}). Telegram bildirimi engellendi.")
                        continue
                    
                    analysis['events'] = [n.title for n in news_group]
                    current_signal = SignalEnricher.enrich(current_signal, analysis, "HOURLY_SUMMARY")
                    msg = TelegramNotifier.format_hourly_summary(analysis, news_group)
                    await self.telegram.send_message(msg)
                    logger.info(f"[Telegram] ✅ Saatlik bülten gönderildi ({len(news_group)} başlık).")
            except Exception as e:
                logger.error(f"[Buffer] Saatlik Grup işleme hatası: {e}")

    async def run(self):
        set_engine_instance(self)
        await self.telegram.connect()
        self.mt5_collector.start()
        self.hourly_updater.start()
        logger.info("🚀 NewsTelASCL v2.1.0 başlatıldı!")
        await asyncio.gather(self.calendar_loop(), self.news_sniper_loop())


# ============================================================================
# BÖLÜM 18: FLASK API
# ============================================================================

app = Flask(__name__)
_engine_instance = None

def set_engine_instance(engine):
    global _engine_instance
    _engine_instance = engine

@app.route('/api/signal', methods=['GET'])
def get_signal(): return jsonify(current_signal.to_dict())

@app.route('/api/buffer', methods=['GET'])
def get_buffer_status():
    global _engine_instance
    if _engine_instance and hasattr(_engine_instance, 'news_buffer'):
        return jsonify(_engine_instance.news_buffer.get_buffer_status())
    return jsonify({"status": "Engine not initialized", "count": 0, "titles": []})

@app.route('/api/news', methods=['GET'])
def get_news_cache():
    global RECENT_NEWS_CACHE
    return jsonify({
        "cache_count": len(RECENT_NEWS_CACHE),
        "news": [{"title": n['title'][:80], "time": n['time'].isoformat()} for n in RECENT_NEWS_CACHE[-20:]]
    })

@app.route('/api/history/<event_type>', methods=['GET'])
def get_history(event_type: str):
    try:
        db = ImpactHistoryDB()
        data = db.get_historical_average(event_type)
        data['surprise_statistics'] = db.get_surprise_statistics(event_type)
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_all_event_types():
    try:
        db = ImpactHistoryDB()
        conn = db._get_connection()
        cursor = conn.execute("SELECT DISTINCT event_type, COUNT(*) as count FROM impact_records GROUP BY event_type ORDER BY count DESC")
        rows = cursor.fetchall()
        conn.close()
        return jsonify({"event_types": [{"type": r[0], "count": r[1]} for r in rows]})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    global _engine_instance
    buffer_count = len(_engine_instance.news_buffer.buffer) if _engine_instance and hasattr(_engine_instance, 'news_buffer') else 0
    return jsonify({
        "status": "healthy", "version": "2.1.0", "timestamp": UTCTimeManager.now().isoformat(),
        "mt5_connected": current_signal.mt5_bid > 0, "mt5_bid": current_signal.mt5_bid,
        "mt5_spread": current_signal.spread, "buffer_count": buffer_count,
        "cache_count": len(RECENT_NEWS_CACHE), "current_phase": current_signal.current_phase
    })

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "name": "NewsTelASCL v2.1.0 API",
        "endpoints": {
            "GET /": "Endpoint listesi", "GET /health": "Sistem sağlık durumu",
            "GET /api/signal": "Güncel XAUUSD sinyali", "GET /api/buffer": "Haber tamponu",
            "GET /api/news": "Son işlenen haberler", "GET /api/history": "Event tipleri",
            "GET /api/history/<event_type>": "Event tipi detay"
        }
    })


# ============================================================================
# BÖLÜM 19: BAŞLATICI
# ============================================================================

def run_async_engine():
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(TradingEngine().run())
    except Exception as e: logger.critical(f"💥 Motor çöktü: {e}"); os._exit(1)

def application_process():
    threading.Thread(target=run_async_engine, daemon=True).start()
    app.run(host='127.0.0.1', port=5050, debug=False)

def main_watchdog():
    while True:
        p = multiprocessing.Process(target=application_process)
        p.start()
        p.join()
        if p.exitcode == 0: break
        logger.warning("⚠️ Process çöktü, 5sn sonra yeniden başlatılıyor...")
        time.sleep(5)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    
    if not os.path.exists("session_name.session"):
        logger.info("📱 Telegram oturumu oluşturuluyor...")
        if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tc = TelegramClient('session_name', TG_API_ID, TG_API_HASH)
        loop.run_until_complete(tc.start(phone=TG_PHONE))
        loop.run_until_complete(tc.disconnect())
        loop.close()
    
    logger.info("""
╔═══════════════════════════════════════════════════════════╗
║         NewsTelASCL v2.1.0 - Kurumsal Fon Mimarisi        ║
║═══════════════════════════════════════════════════════════║
║  ✅ Saat Başı (XX:55) Toplu Haber Analizi                 ║
║  ✅ Düşük Etki (Noise) Filtrasyonu                        ║
║  ✅ Ekonomik Etki Veritabanı (SQLite)                     ║
║  ✅ Sekans Hesaplayıcı & Portföy Analizi                  ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    main_watchdog()