# NewsTelASCL v2.1.0 - AI-Powered XAUUSD Trading Engine

NewsTelASCL, asenkron mimari ile çalışan, kurumsal fon seviyesinde bir algoritmik haber ve piyasa analiz motorudur. Gemini AI, MetaTrader 5 (MT5) ve Telegram entegrasyonlarını birleştirerek XAUUSD (Altın) piyasasındaki makroekonomik olayları gerçek zamanlı analiz eder. 

Sistem, fiyat hareketlerini standart pips yerine, asset değerinin 1/10,000'ini baz alan tescilli **"Sekanas"** metrik formülü ile ölçümlemektedir.

## 🚀 Temel Özellikler

- **Asenkron MT5 Entegrasyonu:** Main thread'i bloklamayan Daemon Thread mimarisi ile gerçek zamanlı spread, bid ve ATR verisi takibi.
- **Sekanas Çekirdeği:** Fiyat hareketlerini ve kar/zarar (R) oranlarını bağımsız `SekansCalculator` modülü ile hesaplar.
- **AI Tabanlı Gürültü Azaltma (Noise Reduction):** Gemini 2.5 Flash modeli ile haberlerin piyasa etkisini (Impact Score) hesaplar. Düşük etkili (Minor) haberleri filtreleyerek sinyal kirliliğini önler.
- **Saatlik Toplu İşleme (Hourly Batch Processing):** Parça parça gelen haberleri tamponda (buffer) biriktirerek saat başı (XX:55) kümülatif piyasa duyarlılık (sentiment) analizi yapar.
- **Gelişmiş Etki Veritabanı (ImpactHistoryDB):** Açıklanan verilerin 5 dakikalık ve 1 saatlik etki sekanslarını SQLite veritabanına kaydederek tarihsel başarı ve volatilite profilleri çıkarır.

## 🏗 Mimari Yapı

Proje, çoklu asenkron işlemleri yönetmek için Mq5 Yapısal Prosedürel Modeli prensiplerine göre tasarlanmıştır:
- **Watchdog & Pre-Auth:** Otomatik süreç yönetimi ve çökme koruması.
- **Flask Web API:** Dış sistemlerin mevcut durumu ve sinyalleri çekebilmesi için RESTful arayüz.
- **Telethon:** Asenkron, düşük gecikmeli Telegram bildirim servisi.
- **Event Classifier:** NFP, CPI, FOMC gibi kritik verileri otonom sınıflandıran algoritma.

## ⚙️ Kurulum & Çalıştırma

### Gereksinimler
- Python 3.9+
- MetaTrader 5 Terminali (Windows/Wine)
- Telegram API Credentials (API_ID, API_HASH)
- Google Gemini AI API Anahtarı

### Adımlar

1. Repository'i klonlayın:
   ```bash
   git clone [https://github.com/aydinsarihan/News.git](https://github.com/aydinsarihan/News.git)
   cd News
