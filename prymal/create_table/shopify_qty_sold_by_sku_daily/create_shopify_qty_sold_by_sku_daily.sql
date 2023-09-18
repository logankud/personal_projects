CREATE EXTERNAL TABLE IF NOT EXISTS shopify_qty_sold_by_sku_daily(
 order_date DATE
, sku STRING 
, title STRING
, qty_sold INT
, sku_name STRING

)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shopify/shopify_qty_sold_by_sku_daily/'