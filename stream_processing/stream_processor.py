import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import numpy as np
import xgboost as xgb
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja Kafki i topiców
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

TOPIC_WEJSCIOWY  = "transactions"
TOPIC_PRZETWORZONE = "processed-transactions"
TOPIC_ALERTY     = "fraud-alerts"
TOPIC_STATYSTYKI = "transaction-window-stats"

TOPICI_WYJSCIOWE = [TOPIC_PRZETWORZONE, TOPIC_ALERTY, TOPIC_STATYSTYKI]

OKNO_STATYSTYK_SEKUNDY   = 60
CO_ILE_WYSYLAC_STATYSTYKI = 10

POLSKIE_MIASTA    = {"Warsaw", "Krakow", "Gdansk"}
ZAGRANICZNE_MIASTA = {"Berlin", "London", "Lagos"}

# Ścieżki do plików modelu ML (od Osoby 5)
ML_MODEL_PATH    = os.getenv("ML_MODEL_PATH",    "ML/xgboost_fraud_model_tuned.json")
ML_FEATURES_PATH = os.getenv("ML_FEATURES_PATH", "ML/expected_features.json")

# Próg powyżej którego model ML uznaje transakcję za podejrzaną
ML_PROG_ALERTU = 0.7


# ─────────────────────────────────────────────────────────────────────────────
# Ładowanie modelu ML
# ─────────────────────────────────────────────────────────────────────────────

def zaladuj_model_ml():
    """
    Wczytuje model XGBoost i listę cech z plików dostarczonych przez Osobę 5.
    Jeśli pliki nie istnieją, procesor działa dalej tylko na regułach biznesowych.
    """
    if not os.path.exists(ML_MODEL_PATH) or not os.path.exists(ML_FEATURES_PATH):
        print(f"UWAGA: Brak pliku modelu ML ({ML_MODEL_PATH}) lub cech ({ML_FEATURES_PATH}).")
        print("Processor będzie działał tylko na regułach biznesowych.")
        return None, None

    model = xgb.XGBClassifier()
    model.load_model(ML_MODEL_PATH)

    with open(ML_FEATURES_PATH, "r") as f:
        cechy = json.load(f)

    print(f"Model ML załadowany. Liczba cech: {len(cechy)}")
    return model, cechy


def przewidz_ml(model, cechy, transakcja):
    """
    Zwraca prawdopodobieństwo fraudu według modelu ML (liczba od 0 do 1).
    Jeśli model nie jest dostępny, zwraca None.
    """
    if model is None:
        return None

    try:
        timestamp = zamien_date_na_datetime(transakcja["timestamp"])
        kategoria = transakcja["merchant_category"]
        miasto    = transakcja["city"]

        # Budujemy wiersz danych w tym samym formacie co dane treningowe Osoby 5.
        wiersz = {
            "amount": float(transakcja["amount"]),
            "hour":   timestamp.hour,
            # One-hot encoding kategorii (tak jak w danych treningowych)
            "merchant_category_electronics": int(kategoria == "electronics"),
            "merchant_category_fuel":        int(kategoria == "fuel"),
            "merchant_category_grocery":     int(kategoria == "grocery"),
            "merchant_category_online_shop": int(kategoria == "online_shop"),
            "merchant_category_restaurant":  int(kategoria == "restaurant"),
            # One-hot encoding miasta
            "city_Gdansk":  int(miasto == "Gdansk"),
            "city_Krakow":  int(miasto == "Krakow"),
            "city_London":  int(miasto == "London"),
            "city_Warsaw":  int(miasto == "Warsaw"),
            "city_Wroclaw": int(miasto == "Wroclaw"),
        }

        # Układamy wartości w kolejności zgodnej z plikiem expected_features.json
        X = np.array([[wiersz[c] for c in cechy]])
        prawdopodobienstwo = model.predict_proba(X)[0][1]
        return round(float(prawdopodobienstwo), 4)

    except Exception as blad:
        print("Błąd predykcji ML:", blad)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Funkcje pomocnicze
# ─────────────────────────────────────────────────────────────────────────────

def zamien_date_na_datetime(timestamp):
    """Zamienia timestamp z wiadomości na obiekt datetime w UTC."""
    timestamp = timestamp.replace("Z", "+00:00")
    data = datetime.fromisoformat(timestamp)

    if data.tzinfo is None:
        data = data.replace(tzinfo=timezone.utc)

    return data.astimezone(timezone.utc)


def utworz_topici_jesli_nie_istnieja():
    """Tworzy topici wyjściowe, jeśli jeszcze nie istnieją."""
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="stream-processor-admin",
    )

    istniejace_topici    = set(admin.list_topics())
    topici_do_utworzenia = []

    for nazwa_topicu in TOPICI_WYJSCIOWE:
        if nazwa_topicu not in istniejace_topici:
            topici_do_utworzenia.append(
                NewTopic(
                    name=nazwa_topicu,
                    num_partitions=6,
                    replication_factor=1,
                )
            )

    if topici_do_utworzenia:
        try:
            admin.create_topics(topici_do_utworzenia)
            print("Utworzono topici:", [t.name for t in topici_do_utworzenia])
        except TopicAlreadyExistsError:
            pass

    admin.close()


def policz_poziom_ryzyka(wynik):
    """Zamienia punktowy wynik ryzyka na poziom opisowy."""
    if wynik >= 75:
        return "critical"
    if wynik >= 50:
        return "high"
    if wynik >= 25:
        return "medium"
    return "low"


# ─────────────────────────────────────────────────────────────────────────────
# Logika biznesowa: reguły wykrywania podejrzanych transakcji
# ─────────────────────────────────────────────────────────────────────────────

def policz_ryzyko_reguly(transakcja, historia_uzytkownikow):
    """
    Liczy ryzyko transakcji na podstawie reguł biznesowych.

    Zwraca:
    - wynik od 0 do 100,
    - listę powodów (risk_flags).
    """
    user_id          = transakcja["user_id"]
    kwota            = float(transakcja["amount"])
    kategoria        = transakcja["merchant_category"]
    miasto           = transakcja["city"]
    czas_transakcji  = zamien_date_na_datetime(transakcja["timestamp"])

    historia              = historia_uzytkownikow[user_id]
    poprzednie_transakcje = list(historia["transakcje"])

    wynik  = 0
    powody = []

    # Reguła 1: bardzo mała płatność online może oznaczać testowanie karty.
    if kategoria == "online_shop" and kwota <= 5:
        wynik += 35
        powody.append("very_low_online_transaction")

    # Reguła 2: duża transakcja w elektronice lub paliwie jest bardziej ryzykowna.
    if kwota >= 3000 and kategoria in {"electronics", "fuel"}:
        wynik += 45
        powody.append("high_value_sensitive_category")

    # Reguła 3: wypłata z bankomatu za granicą na wysoką kwotę.
    if miasto in ZAGRANICZNE_MIASTA and kategoria == "atm" and kwota >= 1000:
        wynik += 50
        powody.append("foreign_atm_withdrawal")

    # Reguła 4: wiele transakcji jednego użytkownika w krótkim czasie.
    if len(poprzednie_transakcje) >= 5:
        wynik += 20
        powody.append("high_user_velocity_5min")

    # Reguła 5: kilka bardzo małych płatności online pod rząd.
    male_platnosci_online = sum(
        1 for t in poprzednie_transakcje
        if t["merchant_category"] == "online_shop" and float(t["amount"]) <= 5
    )
    if kategoria == "online_shop" and kwota <= 5 and male_platnosci_online >= 2:
        wynik += 25
        powody.append("repeated_card_testing_pattern")

    # Reguła 6: szybka zmiana lokalizacji z Polski na zagranicę.
    ostatnie_miasto = historia["ostatnie_miasto"]
    ostatni_czas    = historia["ostatni_czas"]

    if ostatnie_miasto in POLSKIE_MIASTA and miasto in ZAGRANICZNE_MIASTA and ostatni_czas:
        roznica_minut = (czas_transakcji - ostatni_czas).total_seconds() / 60
        if 0 <= roznica_minut <= 30:
            wynik += 30
            powody.append("impossible_travel_pattern")

    # To pole pochodzi z generatora danych – zostawiamy jako informację.
    if transakcja.get("is_fraud") is True:
        powody.append("simulated_ground_truth_fraud")

    return min(wynik, 100), powody


def usun_stare_dane(historia_uzytkownikow, okno_globalne, aktualny_czas):
    """Czyści stare dane z pamięci."""
    granica_okna = aktualny_czas.timestamp() - OKNO_STATYSTYK_SEKUNDY

    while okno_globalne and okno_globalne[0]["event_ts"].timestamp() < granica_okna:
        okno_globalne.popleft()

    granica_historii = aktualny_czas.timestamp() - 5 * 60

    for user_id in list(historia_uzytkownikow.keys()):
        historia = historia_uzytkownikow[user_id]

        while historia["transakcje"] and historia["transakcje"][0]["event_ts"].timestamp() < granica_historii:
            historia["transakcje"].popleft()

        if not historia["transakcje"] and historia["ostatni_czas"]:
            if historia["ostatni_czas"].timestamp() < granica_historii:
                del historia_uzytkownikow[user_id]


def przetworz_transakcje(transakcja, historia_uzytkownikow, okno_globalne, model_ml, cechy_ml):
    """
    Przetwarza pojedynczą transakcję:
    - liczy wynik ryzyka regułami biznesowymi,
    - robi predykcję modelem ML,
    - łączy oba wyniki w końcową ocenę.
    """
    czas_transakcji = zamien_date_na_datetime(transakcja["timestamp"])
    usun_stare_dane(historia_uzytkownikow, okno_globalne, czas_transakcji)

    # Reguły biznesowe
    wynik_regul, powody = policz_ryzyko_reguly(transakcja, historia_uzytkownikow)

    # Predykcja ML
    ml_prawdopodobienstwo = przewidz_ml(model_ml, cechy_ml, transakcja)

    # Łączymy oba sygnały w końcowy wynik ryzyka.
    # Jeśli model ML jest dostępny: 50% wagi reguły + 50% wagi ML.
    # Jeśli ML niedostępny: tylko reguły.
    if ml_prawdopodobienstwo is not None:
        wynik_ml    = ml_prawdopodobienstwo * 100
        wynik_końcowy = round(0.5 * wynik_regul + 0.5 * wynik_ml)

        if ml_prawdopodobienstwo >= ML_PROG_ALERTU:
            powody.append(f"ml_high_probability:{ml_prawdopodobienstwo:.2f}")
    else:
        wynik_końcowy = wynik_regul

    wynik_końcowy = min(wynik_końcowy, 100)
    poziom_ryzyka = policz_poziom_ryzyka(wynik_końcowy)

    przetworzona = dict(transakcja)
    przetworzona["processed_at"]          = datetime.now(timezone.utc).isoformat()
    przetworzona["risk_score"]            = wynik_końcowy
    przetworzona["risk_score_rules"]      = wynik_regul
    przetworzona["risk_score_ml"]         = ml_prawdopodobienstwo
    przetworzona["risk_level"]            = poziom_ryzyka
    przetworzona["risk_flags"]            = powody
    przetworzona["requires_manual_review"] = poziom_ryzyka in {"high", "critical"}
    przetworzona["processor_version"]     = "3.0-ml-integrated"

    # Aktualizacja historii użytkownika.
    user_id  = transakcja["user_id"]
    historia = historia_uzytkownikow[user_id]

    transakcja_do_historii              = dict(transakcja)
    transakcja_do_historii["event_ts"]  = czas_transakcji
    transakcja_do_historii["risk_score"] = wynik_końcowy
    transakcja_do_historii["risk_level"] = poziom_ryzyka

    historia["transakcje"].append(transakcja_do_historii)
    historia["ostatnie_miasto"] = transakcja["city"]
    historia["ostatni_czas"]    = czas_transakcji

    okno_globalne.append(transakcja_do_historii)

    return przetworzona


# ─────────────────────────────────────────────────────────────────────────────
# Statystyki okienne
# ─────────────────────────────────────────────────────────────────────────────

def zbuduj_statystyki_okienne(okno_globalne):
    """Buduje statystyki z ostatnich 60 sekund."""
    transakcje        = list(okno_globalne)
    liczba_transakcji = len(transakcje)
    suma_kwot         = round(sum(float(t["amount"]) for t in transakcje), 2)

    liczba_alertow           = 0
    liczba_fraudow_symulowanych = 0
    po_miastach              = defaultdict(int)
    po_kategoriach           = defaultdict(int)

    for t in transakcje:
        if t["risk_level"] in {"high", "critical"}:
            liczba_alertow += 1
        if t.get("is_fraud") is True:
            liczba_fraudow_symulowanych += 1
        po_miastach[t["city"]] += 1
        po_kategoriach[t["merchant_category"]] += 1

    if liczba_transakcji > 0:
        srednia_kwota  = round(suma_kwot / liczba_transakcji, 2)
        udzial_alertow = round(liczba_alertow / liczba_transakcji, 4)
    else:
        srednia_kwota  = 0
        udzial_alertow = 0

    return {
        "window_seconds":          OKNO_STATYSTYK_SEKUNDY,
        "window_end":              datetime.now(timezone.utc).isoformat(),
        "transaction_count":       liczba_transakcji,
        "total_amount":            suma_kwot,
        "avg_amount":              srednia_kwota,
        "alerts_count":            liczba_alertow,
        "simulated_frauds_count":  liczba_fraudow_symulowanych,
        "alert_rate":              udzial_alertow,
        "by_city":                 dict(sorted(po_miastach.items())),
        "by_category":             dict(sorted(po_kategoriach.items())),
    }


def wyslij_statystyki_jesli_czas(producer, okno_globalne, ostatnia_wysylka):
    """Wysyła statystyki okienne co kilka sekund."""
    teraz = time.time()

    if teraz - ostatnia_wysylka < CO_ILE_WYSYLAC_STATYSTYKI:
        return ostatnia_wysylka

    statystyki = zbuduj_statystyki_okienne(okno_globalne)
    producer.send(TOPIC_STATYSTYKI, key="global", value=statystyki)
    producer.flush()

    print("Wysłano statystyki okienne:", statystyki)
    return teraz


# ─────────────────────────────────────────────────────────────────────────────
# Główna pętla programu
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Uruchamianie stream processora...")
    print("Kafka:", KAFKA_BOOTSTRAP_SERVERS)

    # Ładujemy model ML od Osoby 5
    model_ml, cechy_ml = zaladuj_model_ml()

    utworz_topici_jesli_nie_istnieja()

    consumer = KafkaConsumer(
        TOPIC_WEJSCIOWY,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fraud-stream-processor",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        key_deserializer=lambda x: x.decode("utf-8") if x else None,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8") if x else None,
    )

    historia_uzytkownikow = defaultdict(lambda: {
        "transakcje":    deque(),
        "ostatnie_miasto": None,
        "ostatni_czas":    None,
    })

    okno_globalne              = deque()
    ostatnia_wysylka_statystyk = time.time()

    print("Processor działa.")
    print(f"Czytam z topicu: {TOPIC_WEJSCIOWY}")
    print(f"Zapisuję do topiców: {TOPICI_WYJSCIOWE}")
    print(f"Model ML: {'aktywny' if model_ml else 'nieaktywny (tylko reguły)'}")

    while True:
        paczka_wiadomosci = consumer.poll(timeout_ms=1000, max_records=100)

        if not paczka_wiadomosci:
            ostatnia_wysylka_statystyk = wyslij_statystyki_jesli_czas(
                producer, okno_globalne, ostatnia_wysylka_statystyk,
            )
            continue

        for _partycja, wiadomosci in paczka_wiadomosci.items():
            for wiadomosc in wiadomosci:
                transakcja = wiadomosc.value

                try:
                    przetworzona = przetworz_transakcje(
                        transakcja,
                        historia_uzytkownikow,
                        okno_globalne,
                        model_ml,
                        cechy_ml,
                    )

                    user_id = przetworzona["user_id"]

                    # 1. Każda przetworzona transakcja trafia do Kafki.
                    producer.send(TOPIC_PRZETWORZONE, key=user_id, value=przetworzona)

                    # 2. Podejrzane transakcje generują alert.
                    if przetworzona["requires_manual_review"]:
                        alert = {
                            "alert_id":           "ALERT-" + przetworzona["transaction_id"],
                            "created_at":         datetime.now(timezone.utc).isoformat(),
                            "transaction_id":     przetworzona["transaction_id"],
                            "user_id":            przetworzona["user_id"],
                            "amount":             przetworzona["amount"],
                            "city":               przetworzona["city"],
                            "merchant_category":  przetworzona["merchant_category"],
                            "risk_score":         przetworzona["risk_score"],
                            "risk_score_rules":   przetworzona["risk_score_rules"],
                            "risk_score_ml":      przetworzona["risk_score_ml"],
                            "risk_level":         przetworzona["risk_level"],
                            "risk_flags":         przetworzona["risk_flags"],
                            "recommended_action": "manual_review",
                        }

                        if przetworzona["risk_level"] == "critical":
                            alert["recommended_action"] = "block_and_manual_review"

                        producer.send(TOPIC_ALERTY, key=user_id, value=alert)
                        print("ALERT:", alert)
                    else:
                        print(
                            "OK:",   przetworzona["transaction_id"],
                            "user=", przetworzona["user_id"],
                            "risk=", przetworzona["risk_score"],
                            "ml=",   przetworzona["risk_score_ml"],
                        )

                except Exception as blad:
                    print("Błąd przetwarzania wiadomości:", blad)
                    print("Wiadomość:", transakcja)

        producer.flush()
        ostatnia_wysylka_statystyk = wyslij_statystyki_jesli_czas(
            producer, okno_globalne, ostatnia_wysylka_statystyk,
        )


if __name__ == "__main__":
    main()
