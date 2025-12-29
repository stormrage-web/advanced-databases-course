-- =============================================
-- Аналитические запросы для практической работы
-- =============================================

-- 1) Топ-20 категорий по количеству товаров (RAW)
SELECT
    category_id,
    count() AS offers_cnt
FROM ozon_analytics.ecom_offers
GROUP BY category_id
ORDER BY offers_cnt DESC
LIMIT 20;

-- 1b) Топ-20 категорий по количеству товаров (MV/AGG)
SELECT
    category_id,
    countMerge(offers_cnt_state) AS offers_cnt
FROM ozon_analytics.catalog_by_category_agg
GROUP BY category_id
ORDER BY offers_cnt DESC
LIMIT 20;

-- 2) Топ-30 брендов по количеству товаров (RAW)
SELECT
    vendor,
    count() AS offers_cnt
FROM ozon_analytics.ecom_offers
GROUP BY vendor
ORDER BY offers_cnt DESC
LIMIT 30;

-- 2b) Топ-30 брендов по количеству товаров (MV/AGG)
SELECT
    vendor,
    countMerge(offers_cnt_state) AS offers_cnt
FROM ozon_analytics.catalog_by_brand_agg
GROUP BY vendor
ORDER BY offers_cnt DESC
LIMIT 30;

-- 3) Среднее количество товаров по брендам в категориях (RAW)
-- (для каждой категории считаем количество товаров по брендам, затем усредняем)
SELECT
    category_id,
    avg(offers_cnt) AS avg_offers_per_brand
FROM
(
  SELECT
    category_id,
    vendor,
    count() AS offers_cnt
  FROM ozon_analytics.ecom_offers
  GROUP BY category_id, vendor
)
GROUP BY category_id
ORDER BY avg_offers_per_brand DESC
LIMIT 50;

-- 4) Анализ товаров без пользовательских событий (LEFT JOIN)
-- Считаем количество событий по offer_id через AGG-таблицу
SELECT
    o.offer_id,
    o.category_id,
    o.vendor,
    o.price
FROM ozon_analytics.ecom_offers AS o
LEFT JOIN
(
  SELECT
    offer_id,
    countMerge(events_cnt_state) AS events_cnt
  FROM ozon_analytics.events_by_offer_agg
  GROUP BY offer_id
) AS ev ON ev.offer_id = o.offer_id
WHERE coalesce(ev.events_cnt, 0) = 0
ORDER BY o.offer_id
LIMIT 100;

-- 4b) Сводка покрытия каталога событиями
SELECT
    count() AS total_offers,
    countIf(coalesce(ev.events_cnt, 0) > 0) AS offers_with_events,
    countIf(coalesce(ev.events_cnt, 0) = 0) AS offers_without_events,
    round(offers_with_events / total_offers * 100, 2) AS coverage_percent
FROM ozon_analytics.ecom_offers AS o
LEFT JOIN
(
  SELECT
    offer_id,
    countMerge(events_cnt_state) AS events_cnt
  FROM ozon_analytics.events_by_offer_agg
  GROUP BY offer_id
) AS ev ON ev.offer_id = o.offer_id;


