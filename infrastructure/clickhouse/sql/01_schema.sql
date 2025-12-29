-- =============================================
-- ClickHouse: схема данных и материализованные представления
-- БД: ozon_analytics
-- =============================================

CREATE DATABASE IF NOT EXISTS ozon_analytics;

-- ---------------------------------------------
-- 1) Каталог товаров (offers)
-- Требование: ReplacingMergeTree
-- Источник: data/offers_clean.csv (UTF-8, без индекс-колонки)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.ecom_offers
(
    offer_id     UInt64,
    price        Float64,
    seller_id    UInt64,
    category_id  UInt32,
    vendor       String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY offer_id;

-- ---------------------------------------------
-- 2) Сырые пользовательские события (events)
-- Требование: MergeTree
-- Источник: data/raw_events.csv (генерируется скриптом)
-- Связь: ContentUnitID = offer_id
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.raw_events
(
    event_time        DateTime,
    DeviceTypeName    LowCardinality(String),
    ApplicationName   LowCardinality(String),
    OSName            LowCardinality(String),
    ProvinceName      LowCardinality(String),
    ContentUnitID     UInt64
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (event_time, ContentUnitID, DeviceTypeName);

-- ---------------------------------------------
-- 3) Агрегаты по категориям (ускорение топов/средних)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.catalog_by_category_agg
(
    category_id        UInt32,
    offers_cnt_state   AggregateFunction(count, UInt64),
    avg_price_state    AggregateFunction(avg, Float64),
    sellers_uniq_state AggregateFunction(uniqExact, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY category_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS ozon_analytics.catalog_by_category_mv
TO ozon_analytics.catalog_by_category_agg
AS
SELECT
    category_id,
    countState(offer_id)                      AS offers_cnt_state,
    avgState(price)                          AS avg_price_state,
    uniqExactState(seller_id)                AS sellers_uniq_state
FROM ozon_analytics.ecom_offers
GROUP BY category_id;

-- ---------------------------------------------
-- 4) Агрегаты по брендам (vendor)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.catalog_by_brand_agg
(
    vendor             String,
    offers_cnt_state   AggregateFunction(count, UInt64),
    avg_price_state    AggregateFunction(avg, Float64),
    categories_uniq_state AggregateFunction(uniqExact, UInt32)
)
ENGINE = AggregatingMergeTree
ORDER BY vendor;

CREATE MATERIALIZED VIEW IF NOT EXISTS ozon_analytics.catalog_by_brand_mv
TO ozon_analytics.catalog_by_brand_agg
AS
SELECT
    vendor,
    countState(offer_id)             AS offers_cnt_state,
    avgState(price)                 AS avg_price_state,
    uniqExactState(category_id)     AS categories_uniq_state
FROM ozon_analytics.ecom_offers
GROUP BY vendor;

-- ---------------------------------------------
-- 5) Покрытие каталога событиями (events by offer)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.events_by_offer_agg
(
    offer_id           UInt64,
    events_cnt_state   AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY offer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS ozon_analytics.events_by_offer_mv
TO ozon_analytics.events_by_offer_agg
AS
SELECT
    ContentUnitID AS offer_id,
    countState(ContentUnitID) AS events_cnt_state
FROM ozon_analytics.raw_events
GROUP BY ContentUnitID;

-- ---------------------------------------------
-- 6) Активность по устройствам (для бизнес-дашборда)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.events_by_device_agg
(
    DeviceTypeName     LowCardinality(String),
    events_cnt_state   AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY DeviceTypeName;

CREATE MATERIALIZED VIEW IF NOT EXISTS ozon_analytics.events_by_device_mv
TO ozon_analytics.events_by_device_agg
AS
SELECT
    DeviceTypeName,
    countState(ContentUnitID) AS events_cnt_state
FROM ozon_analytics.raw_events
GROUP BY DeviceTypeName;

-- ---------------------------------------------
-- 7) События по категориям (ускоряет "events per category" и покрытие в разрезе категорий)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS ozon_analytics.events_by_category_agg
(
    category_id          UInt32,
    events_cnt_state     AggregateFunction(count, UInt64),
    uniq_offers_state    AggregateFunction(uniqExact, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY category_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS ozon_analytics.events_by_category_mv
TO ozon_analytics.events_by_category_agg
AS
SELECT
    o.category_id AS category_id,
    countState(e.ContentUnitID) AS events_cnt_state,
    uniqExactState(e.ContentUnitID) AS uniq_offers_state
FROM ozon_analytics.raw_events AS e
ANY INNER JOIN ozon_analytics.ecom_offers AS o
    ON o.offer_id = e.ContentUnitID
GROUP BY o.category_id;


