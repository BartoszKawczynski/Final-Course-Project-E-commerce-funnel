-- # Final-Course-Project-E-commerce-funnel
-- Creating a dashboard for managers of the marketing department to analyze conversions in an online store
SELECT -- główny select 1 ze wskaźnikami
  CAST (user_pseudo_id AS STRING) AS user_id,
  TIMESTAMP_MICROS (event_timestamp) AS event_ts, -- wskazuje czas zdarzenia w formacie timestamp
  CAST (event_name AS string) AS event_name, -- wskazuje nazwę zdarzenia
  CAST(COUNT(DISTINCT(SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id'))AS INT64) AS odwiedziny, -- zlicza liczbę unikalnych użytkowników, którzy odwiedzili stronę w danym okresie
  CAST(COUNT(DISTINCT ecommerce.transaction_id)AS INT64) AS zamowienia, -- zlicza liczbę unikalnych zamówień złożonych w danym okresie
  CAST(SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue ELSE 0 END)AS INT64) AS sprzedaz, --  sumuje przychody z zakupów dokonanych w danym okresie
  CAST(REGEXP_EXTRACT(
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location'AND value.string_value <> '' AND value.string_value NOT LIKE '%/404%'),
      r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)')AS STRING) as sciezka_docelowa, -- zawiera ścieżkę strony, na której użytkownik rozpoczął sesję, bez adresu domeny i bez parametrów z linku
  CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location')AS STRING) as strona_docelowa, -- zawiera pełną ścieżkę strony, na której użytkownik rozpoczął sesję
  CAST(traffic_source.source AS STRING) AS zrodlo, -- zawiera informację o źródle ruchu (np. Google Ads, Facebook, email)
  CAST (traffic_source.medium AS STRING) AS medium, -- zawiera informację o użytym medium (np. CPC, CPM, organic)
  CAST (traffic_source.name AS STRING) AS kampania, -- zawiera nazwę kampanii marketingowej, z której pochodzi ruch
  CAST (device.category AS STRING) AS kategoria_urzadzenia, -- zawiera informację o kategorii urządzenia użytkownika (np. smartfon, tablet, komputer)
  CAST (device.language AS STRING) AS jezyk_urzadzenia, -- zawiera informację o języku urządzenia użytkownika
  CAST (device.operating_system AS STRING) AS system_operacyjny -- zawiera informację o systemie operacyjnym urządzenia użytkownika
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 
  'add_shipping_info', 'add_payment_info', 'purchase') -- wskazanie zdarzeń
  AND event_date BETWEEN '2020-01-01' AND '2021-12-31' -- wskazanie przedziału dat: rok 2020 i 2021
GROUP BY -- grupowanie kolumn
  user_id,
  event_ts,
  event_name,
  sciezka_docelowa,
  strona_docelowa,
  zrodlo,
  medium,
  kampania,
  kategoria_urzadzenia,
  jezyk_urzadzenia,
  system_operacyjny
ORDER BY -- sortowanie wyniku po dacie zdarzenia
  event_ts DESC;
