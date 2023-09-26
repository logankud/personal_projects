
INSERT INTO shopify_qty_sold_by_sku_daily

with line_items as (

SELECT CAST(order_date AS DATE) as order_date
, sku
, title
, SUM(quantity) as qty_sold
FROM "prymal"."shopify_line_items"
WHERE year = '2023' 
AND month = '09'
AND day = '17'
GROUP BY order_date
,sku
, title

)

, 

line_items_mapped as (

SELECT li.*
, sku.sku_name
, DATE(order_date) as partition_date
FROM line_items li
LEFT JOIN "prymal"."skus_shopify" sku
ON li.sku = sku.sku
WHERE sku.load_date = (SELECT MAX(load_date) FROM "prymal"."skus_shopify")   -- Select latest sku table partition

)


SELECT order_date
, sku_name
, SUM(qty_sold) as qty_sold
, order_date AS partition_date
FROM line_items_mapped
GROUP BY order_date
, sku_name
