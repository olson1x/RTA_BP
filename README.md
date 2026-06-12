# Analiza danych w czasie rzeczywistym
Projekt realizuje potok przetwarzania danych w czasie rzeczywistym przeznaczony do identyfikacji podejrzanych transakcji.

## Architektura potoku danych
Potok składa się z następujących komponentów:
Data Producer -> Apache Kafka -> Stream Processing -> Storage -> ML/Batch -> Dashboard/BI

## Moduł: Data Producer
Skrypt `producer.py` pełni rolę symulatora systemu bankowego. Generuje on ciągły strumień transakcji dla stałej puli 50 użytkowników (od `U-000` do `U-049`) i wysyła je na żywo do brokera Kafka (port `29092`, topic `transactions`). Klienci przypisani są do stałych identyfikatorów, co umożliwia analizę behawioralną i wykrywanie odchyleń w czasie.

### Format wiadomości (JSON)
Każde zdarzenie wysyłane do systemu posiada twardą strukturę danych:

- transaction_id: unikalny identyfikator transakcji (string)
- timestamp: czas rejestracji zdarzenia w formacie ISO 8601 (string)
- user_id: identyfikator klienta (string)
- amount: kwota transakcji wyrażona w PLN (float)
- currency: waluta transakcji, domyślnie PLN (string)
- merchant_category: kategoria punktu handlowego (grocery, electronics, fuel, restaurant, online_shop, atm) (string)
- city: miasto, w którym dokonano płatności (string)
- is_fraud: flaga oznaczająca anomalię (boolean: True/False)

## Logika Biznesowa i Scenariusze Fraudowe

Strumień danych zawiera celowo zaimplementowane wzorce anomalii (ustawione na poziomie 5% wszystkich transakcji). Dane zostały zbalansowane w taki sposób, aby algorytmy uczenia maszynowego musiały wykrywać nieliniowe zależności, a nie proste reguły kwotowe.

### 1. Ruch standardowy (95% strumienia)
Symuluje codzienne, legalne zachowanie klientów. 
- Kwoty transakcji mieszczą się w przedziale od 10 do 500 PLN.
- Lokalizacje są probabilistycznie zdominowane przez polskie miasta (Warszawa - 70%, Kraków - 15%, Gdańsk - 10%).
- Uwaga dla sekcji ML: Zwykli użytkownicy generują szum w danych, ponieważ sporadycznie podróżują (Berlin - 3%, Londyn - 2%), co oznacza, że sama obecność zagranicznego miasta nie definiuje automatycznie oszustwa.

### 2. Scenariusze oszustw (5% strumienia)
W przypadku aktywacji flagi `is_fraud = True`, generator losuje jeden z trzech realistycznych ataków:

- Scenariusz A: Testowanie skradzionej karty
  Oszust sprawdza ważność i limity karty przed dokonaniem większego zakupu.
  - Cechy: Bardzo niskie kwoty w przedziale 1.00 - 5.00 PLN.
  - Kategoria: Wyłącznie `online_shop`.
  - Wyzwanie analityczne: Wymaga odróżnienia od drobnych, codziennych zakupów spożywczych.

- Scenariusz B: Skok na kasę
  Gwałtowne wyczyszczenie limitu kredytowego poprzez zakup drogich i łatwych do upłynnienia towarów.
  - Cechy: Wysokie kwoty w przedziale 3000 - 8000 PLN.
  - Kategoria: Wyłącznie `electronics` lub `fuel`.

- Scenariusz C: Klonowanie karty za granicą
  Użycie danych z paska magnetycznego sklonowanej karty w zagranicznym bankomacie.
  - Cechy: Średnio-wysokie kwoty w przedziale 1000 - 4000 PLN.
  - Lokalizacja: Wyłącznie zagranica (Lagos, Berlin, Londyn).
  - Kategoria: Wyłącznie wypłata z bankomatu (`atm`).

## Infrastructure
Ten katalog zawiera lokalną infrastrukturę do odpalenia Kafki dla generatora transakcji.

## Usługi
- Zookeeper.
- Kafka.
- Schema Registry.
- Kafka UI.
- Init container tworzący topic `transactions`.
- Streamlit

## Uruchomienie
1. Zainstaluj Docker i Docker Compose.
2. Wejdź do katalogu z plikiem `docker-compose.yml`.
3. Odpal:

```bash
docker compose up -d
```

## Porty
- Kafka dla hosta: `localhost:29092`
- Kafka UI: `http://localhost:8080`
- Schema Registry: `http://localhost:8081`
- Zookeeper: `localhost:2181`
- Streamlit: `localhost:8501`

## Topic
Tworzony jest topic `transactions`:
- partitions: 6
- replication factor: 1
- retention.ms: 604800000 (7 dni)
- cleanup.policy: delete

## Jak sprawdzić działanie
- Wejdź do Kafka UI i sprawdź topic `transactions`.
- Odpal producenta z punktu 1, który wysyła dane na `localhost:29092`.
- Sprawdź, czy wiadomości pojawiają się w topicu.

## Uwagi
- Lokalnie używamy `replication-factor: 1`, bo to pojedynczy broker.
- `auto.create.topics.enable` jest wyłączone, żeby topic powstawał kontrolowanie.
- `user_id` jako key wiadomości pomaga trzymać dane jednego użytkownika w tej samej partycji.

- ## Moduł: Stream Processing

Skrypt `stream_processing/stream_processor.py` czyta transakcje z topicu `transactions`, ocenia ryzyko każdej z nich i publikuje wyniki do trzech topiców Kafki.

### Wyjścia

- `processed-transactions` — każda transakcja wzbogacona o ocenę ryzyka
- `fraud-alerts` — tylko transakcje z poziomem ryzyka `high` lub `critical`
- `transaction-window-stats` — statystyki agregowane co 10 sekund z ostatniej minuty

### Reguły wykrywania fraudu

- bardzo niska płatność online — możliwe testowanie karty
- wysoka transakcja w kategorii `electronics` lub `fuel`
- wypłata z bankomatu za granicą na wysoką kwotę
- duża liczba transakcji jednego użytkownika w ciągu 5 minut
- powtarzające się bardzo małe płatności online
- nagła zmiana lokalizacji z Polski na zagranicę w ciągu 30 minut

### Uruchomienie

```bash
pip install -r requirements.txt
python stream_processing/stream_processor.py
```
## PostgreSQL lokalnie

### Uruchomienie bazy
1. Zainstaluj Docker i Docker Compose.
2. Wejdź do katalogu z plikiem `docker-compose.yml`.
3. Uruchom bazę:

```bash
docker compose up -d
```

### Połączenie z bazą
Aby połączyć się z lokalnym PostgreSQL, użyj `psql`:

```bash
psql -h localhost -p 5432 -U rta_user -d rta
```

Dane logowania:
- user: `rta_user`
- password: `rta_password`
- database: `rta`

### Nawigacja po bazie
Po zalogowaniu do `psql` przydatne są komendy:

```sql
\dt
```

Wyświetla listę tabel.

```sql
SELECT COUNT(*) FROM processed_transactions;
SELECT COUNT(*) FROM fraud_alerts;
SELECT COUNT(*) FROM transaction_window_stats;
```

Pokazuje liczbę rekordów w tabelach.

```sql
SELECT * FROM processed_transactions LIMIT 5;
```

Wyświetla pierwsze rekordy z tabeli.

```sql
\q
```

Kończy sesję `psql`.

## Ładowanie danych do bazy

Skrypt `init/load-seed.py` wczytuje plik `database/init/seed-data.json` i zapisuje dane do bazy PostgreSQL w tabelach:
- `processed_transactions`
- `fraud_alerts`
- `transaction_window_stats`

Skrypt należy uruchomić po utworzeniu tabel oraz po wygenerowaniu pliku seed przez `stream_processing/stream_processor.py`.

### Zalecana kolejność uruchamiania
1. Uruchom infrastrukturę Kafka i PostgreSQL.
2. Uruchom `stream_processing/stream_processor.py`, aby wygenerował dane i zapisał `seed-data.json`.
3. Uruchom `init/load-seed.py`, aby załadować dane z JSON do bazy.

### Uruchomienie
```bash
python stream_processing/stream_processor.py
python init/load-seed.py
```

### Moduł Wizualizacji (Dashboard BI)

Moduł służący do monitorowania transakcji i wykrywania oszustw w czasie rzeczywistym. Został zbudowany przy użyciu frameworka **Streamlit**. 

Do momentu ostatecznej konteneryzacji i integracji całego środowiska w Dockerze, moduł obsługuje tryb deweloperski, w którym zaciąga dane bezpośrednio z pliku `seed-data.json`.

**Lokalne uruchomienie modułu:**

1. Upewnij się, że znajdujesz się w głównym katalogu projektu.
2. Zainstaluj wszystkie wymagane pakiety:
   ```bash
   pip install -r requirements.txt
   ```
## (Uwaga dla użytkowników Linuxa: jeśli system blokuje globalną instalację, użyj flagi --break-system-packages lub skonfiguruj wirtualne środowisko venv).

3. Uruchom serwer aplikacji komendą:
```bash
streamlit run dashboard.py
```