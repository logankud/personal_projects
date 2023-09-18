CREATE EXTERNAL TABLE IF NOT EXISTS "prymal-analytics"."shopify_qty_sold_by_sku_daily"
(
 order_date DATE "Date that the sku was sold"
, sku VARCHAR "SKU (from Shopify) being sold"
, title VARCHAR "Name of product from Shopify"
, qty_sold INT "Total quantity of sku sold on order_date"
, sku_name VARCHAR "Mapped name of the sku (from Prymal's skus_shopify table)"
)
 PARTITIONED BY 
(
partition_date DATE "Order date that the sku was sold" 
)
 STORED AS CSV
 LOCATION 's3://prymal-analytics/shopify/shopify_qty_sold_by_sku_daily/'
