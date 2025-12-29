-- Backfill для events_by_category_agg (MV не заполняет историю автоматически)

TRUNCATE TABLE IF EXISTS ozon_analytics.events_by_category_agg;

INSERT INTO ozon_analytics.events_by_category_agg
SELECT
  o.category_id AS category_id,
  countState(e.ContentUnitID) AS events_cnt_state,
  uniqExactState(e.ContentUnitID) AS uniq_offers_state
FROM ozon_analytics.raw_events AS e
ANY INNER JOIN ozon_analytics.ecom_offers AS o
  ON o.offer_id = e.ContentUnitID
GROUP BY o.category_id;

OPTIMIZE TABLE ozon_analytics.events_by_category_agg FINAL;


