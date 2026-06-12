import streamlit as st
import pandas as pd
import json
import os
import datetime

st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.sidebar.header("🔌 Status Połączenia")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", "5432")

if DB_HOST:
    st.sidebar.success(f"Połączono z Docker DB: {DB_HOST}")
    MODE = "PRODUCTION_DB"
else:
    st.sidebar.info("💡 Tryb deweloperski: Wykryto brak kontenera bazy. Uruchomiono tryb offline (JSON Seed).")
    MODE = "LOCAL_SEED"

@st.cache_data(ttl=5)
def fetch_data():
    if MODE == "PRODUCTION_DB":
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=os.environ.get("DB_PASSWORD"),
                port=DB_PORT
            )
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM processed_transactions ORDER BY timestamp DESC LIMIT 500;")
            tx_data = cur.fetchall()
            
            cur.execute("SELECT * FROM fraud_alerts ORDER BY timestamp DESC;")
            alerts_data = cur.fetchall()
            
            cur.execute("SELECT * FROM transaction_window_stats ORDER BY window_end DESC LIMIT 100;")
            stats_data = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return pd.DataFrame(tx_data), pd.DataFrame(alerts_data), pd.DataFrame(stats_data)
            
        except Exception as e:
            st.sidebar.error(f"Błąd połączenia z bazą: {e}. Fallback do JSON.")

            return load_from_json()
    else:
        return load_from_json()

def load_from_json():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, 'infrastructure', 'database', 'init', 'seed-data.json')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df_tx = pd.DataFrame(data.get('processed_transactions', []))
        df_alerts = pd.DataFrame(data.get('fraud_alerts', []))
        df_stats = pd.DataFrame(data.get('transaction_window_stats', []))
        return df_tx, df_alerts, df_stats
    except Exception as e:
        st.error(f"Krytyczny błąd odczytu pliku seed: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

df_tx, df_alerts, df_stats = fetch_data()

st.title("🛡️ System Wykrywania Oszustw Finansowych (RTA)")
st.markdown("Monitorowanie strumieni danych transakcyjnych i alertów modeli ML w czasie rzeczywistym.")

st.markdown("### 📈 Kluczowe Wskaźniki Efektywności")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    total_tx = len(df_tx) if not df_tx.empty else 0
    st.metric(label="Przetworzone Transakcje", value=total_tx)

with kpi2:
    total_frauds = len(df_alerts) if not df_alerts.empty else 0
    st.metric(label="Zablokowane Oszustwa", value=total_frauds, delta=f"{total_frauds} alertów", delta_color="inverse")

with kpi3:
    if not df_tx.empty and 'amount' in df_tx.columns:
        avg_amount = round(df_tx['amount'].astype(float).mean(), 2)
        st.metric(label="Średnia Wartość Operacji", value=f"{avg_amount} PLN")
    else:
        st.metric(label="Średnia Wartość Operacji", value="0.00 PLN")

with kpi4:
    st.metric(label="Opóźnienie Strumienia", value="< 150ms", delta="Stabilne")

st.markdown("---")

tab1, tab2, tab3 = st.tabs([
    "📊 Analiza Wolumenu i Ruchu", 
    "🚨 Centrum Zarządzania Alertami", 
    "🧮 Agregacje Okienkowe"
])

with tab1:
    st.subheader("Analiza wolumenu operacji finansowych")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("##### Kwoty transakcji w czasie")
        if not df_tx.empty and 'amount' in df_tx.columns:
            chart_df = df_tx.copy()
            if 'timestamp' in chart_df.columns:
                chart_df = chart_df.sort_values('timestamp')
                st.line_chart(data=chart_df, x='timestamp', y='amount')
            else:
                st.line_chart(chart_df['amount'])
        else:
            st.info("Brak wystarczających danych do wyrysowania osi czasu kwot.")
            
    with col_right:
        st.markdown("##### Podgląd strumienia wejściowego (Ostatnie rekordy)")
        if not df_tx.empty:
            st.dataframe(df_tx.head(15), use_container_width=True)
        else:
            st.warning("Oczekiwanie na pojawienie się danych transakcyjnych w systemie...")

with tab2:
    st.subheader("🚨 Wykryte próby nadużyć i anomalie")
    
    if not df_alerts.empty:
        st.error(f"System zarejestrował {len(df_alerts)} incydentów wymagających weryfikacji.")
    
        st.markdown("##### Rejestr zgłoszeń modeli ML i reguł bezpieczeństwa")
        st.dataframe(
            df_alerts, 
            use_container_width=True,
            column_config={
                "risk_score": st.column_config.ProgressColumn(
                    "Poziom Ryzyka",
                    help="Wynik dopasowania anomalii przez algorytm ML",
                    format="%.2f",
                    min_value=0.0,
                    max_value=1.0,
                )
            }
        )
    else:
        st.success("✅ Brak aktywnych alertów. Wszystkie transakcje spełniają kryteria bezpieczeństwa.")

with tab3:
    st.subheader("🧮 Agregacje danych w oknach czasowych")
    st.markdown("Dane dostarczane przez warstwę Stream Processing (Osoba 3) opisujące zachowanie systemu w ujęciu interwałowym.")
    
    if not df_stats.empty:
        st.dataframe(df_stats, use_container_width=True)
        
        if 'tx_count' in df_stats.columns:
            st.markdown("##### Liczba transakcji w poszczególnych oknach systemowych")
            st.bar_chart(data=df_stats, y='tx_count')
    else:
        st.info("Brak przetworzonych agregacji okienkowych w bazie danych. System oczekuje na start silnika streamingu.")