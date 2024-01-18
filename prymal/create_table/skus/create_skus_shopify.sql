CREATE EXTERNAL TABLE IF NOT EXISTS skus_shopify(
sku STRING
, product_category STRING
, product_type STRING
, sku_name STRING


)
PARTITIONED BY 
(
load_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/skus/shopify/'