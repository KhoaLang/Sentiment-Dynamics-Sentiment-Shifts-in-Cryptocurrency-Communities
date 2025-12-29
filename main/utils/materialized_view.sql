CREATE MATERIALIZED VIEW mv_sentiment_5m AS
SELECT
  window_start,
  asset,
  AVG(fear_index)     AS fear_index,
  AVG(anger)          AS anger,
  AVG(joy)            AS joy,
  AVG(sadness)        AS sadness,
  AVG(surprise)       AS surprise
FROM gold_sentiment_price
GROUP BY window_start, asset
HAVING asset IN (
  'CryptoCurrency',
  'CryptoMarkets',
  'Bitcoin',
  'Ethereum'
);

CREATE INDEX idx_mv_sentiment_5m_time
  ON mv_sentiment_5m (window_start);

CREATE INDEX idx_mv_sentiment_5m_asset
  ON mv_sentiment_5m (asset);


CREATE MATERIALIZED VIEW mv_price_return_5m AS
SELECT
  window_start,
  asset,
  AVG(price_return) AS price_return
FROM gold_sentiment_price
GROUP BY window_start, asset;

CREATE INDEX idx_mv_price_return_5m_time
  ON mv_price_return_5m (window_start);


-- REFRESH
-- REFRESH MATERIALIZED VIEW mv_sentiment_5m;
-- REFRESH MATERIALIZED VIEW mv_price_return_5m;
