-- train LTR weights in BigQuery ML from judged/clicked results.
-- Expects psearch_dataset.ltr_training(query STRING, product_id STRING, label FLOAT64,
--   rrf_score, vector_score, text_score, clicks, orders, discount_pct, popularity_score,
--   margin_pct, ctr, conversion_rate, recency_score, sla_hours  -- all normalized to [0,1]).
-- Export ML.WEIGHTS into the LTR_MODEL_JSON env var of psearch-serving:
--   {"version":"bqml-<date>","bias":<__INTERCEPT__>,"weights":[12 weights in feature order]}
CREATE OR REPLACE MODEL `psearch_dataset.ltr_linear`
OPTIONS (model_type = 'LINEAR_REG', input_label_cols = ['label'], l2_reg = 0.1) AS
SELECT label, rrf_score, vector_score, text_score, clicks, orders, discount_pct, popularity_score,
       margin_pct, ctr, conversion_rate, recency_score, sla_hours
FROM `psearch_dataset.ltr_training`;

SELECT processed_input AS feature, weight
FROM ML.WEIGHTS(MODEL `psearch_dataset.ltr_linear`)
ORDER BY feature;
