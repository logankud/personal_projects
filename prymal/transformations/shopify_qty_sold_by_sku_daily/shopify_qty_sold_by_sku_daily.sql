with line_items as (

SELECT order_date
, sku
, title
, SUM(quantity) as qty_sold
FROM "prymal"."shopify_line_items"
WHERE year = {PARTITION_YEAR} 
AND month = {PARTITION_MONTH}
AND day = {PARTITION_DAY}
GROUP BY sku, title

)

, 

line_items_mapped as (

SELECT li.*
, sku.sku_name
FROM line_items li
LEFT JOIN "prymal"."skus_shopify" sku
ON li.sku = sku.sku
WHERE sku.load_date = (SELECT MAX(load_date) FROM "prymal"."skus_shopify")   -- Select latest sku table partition

)

INSERT INTO "prymal-analytics"."shopify_qty_sold_by_sku_daily"
PARTITION (year={PARTITION_YEAR}, month={PARTITION_MONTH}, day={PARTITION_DAY})

SELECT *
FROM line_items_mapped
