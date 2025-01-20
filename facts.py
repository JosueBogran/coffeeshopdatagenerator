%sql
CREATE OR REPLACE TABLE sweetcoffeetree.facts_sales_3b_rows
SELECT
a.Order_ID
, a.order_line_id
, a.order_date
, a.time_Of_day
, a.season
, b.location_id
, c.name AS product_name
, a.quantity
, (c.standard_price * ((100-discount_rate)/100)) * a.Quantity AS sales_amount
, a.discount_rate AS discount_percentage
FROM
josuesunnydata.sweetcoffeetree.base AS a
LEFT JOIN sweetcoffeetree.dim_locations AS b ON (a.Location_ID = b.record_id)
LEFT JOIN sweetcoffeetree.dim_products AS c ON (a.Product_ID = c.product_id AND a.Order_Date BETWEEN c.from_date AND c.to_date)