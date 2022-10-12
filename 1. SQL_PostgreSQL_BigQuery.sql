-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL

SELECT left(date,6) as month, count(concat(fullVisitorId,visitId)) as visits, sum(totals.pageviews) as pageview , sum(totals.transactions) as transactions , sum(totals.totalTransactionRevenue)/(1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix between '20170101' and '20170331'
group by month
order by month;

-- Query 02: Bounce rate per traffic source in July 2017

#standardSQ
SELECT trafficSource.source AS source,count(concat(fullVisitorId,visitId)) as total_visits, sum(totals.bounces) as total_no_of_bounces, sum(totals.bounces)/count(concat(fullVisitorId,visitId)) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix  LIKE '201707%'
AND trafficSource.source IN ('google','(direct)','youtube.com','analytics.google.com')
group by SOURCE
order by total_visits DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017

#standardSQl
SELECT 
case when extract(month from (parse_date('%Y%m%d',date))) = 6 then 'month' end as time_type,
format_date('%Y%m',parse_date('%Y%m%d',date)) as time,
trafficSource.source AS source,  
sum(totals.totalTransactionRevenue)/(1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix LIKE '201706%'
AND trafficSource.source IN ('google','(direct)')
AND extract(month from (parse_date('%Y%m%d',date))) = 6 
group by time, time_type,  source
union all
SELECT case when extract(week from (parse_date('%Y%m%d',date))) > 20 then 'week' end as time_type,
format_date('%Y%W',parse_date('%Y%m%d',date)) as time,
trafficSource.source AS source,  sum(totals.totalTransactionRevenue)/(1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix LIKE '201706%'
AND trafficSource.source IN ('google','(direct)')
AND extract(week from (parse_date('%Y%m%d',date))) > 20
group by time, time_type , source
ORDER BY source, time

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

#standardSQL
SELECT table1.month,avg_pageviews_purchase,avg_pageviews_non_purchase
FROM (SELECT left(date,6) month, SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorID) as avg_pageviews_purchase, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix between '20170601' and '20170731'
AND totals.transactions >=1 
GROUP BY month) as table1
INNER JOIN
(SELECT left(date,6) month, SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorID) as avg_pageviews_non_purchase, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix between '20170601' and '20170731'
AND totals.transactions is null 
GROUP BY month) as table2
ON table1.month = table2.month 
ON table1.month = table2.month
ORDER BY table1.month

-- Query 05: Average number of transactions per user that made a purchase in July 2017

#standardSQL
SELECT left(date,6) month, SUM(totals.transactions)/COUNT(DISTINCT fullVisitorID) as avg_total_transactions_per_user, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix between '20170701' and '20170731'
AND totals.transactions >=1 
GROUP BY month

-- Query 06: Average amount of money spent per session

#standardSQL
SELECT left(date,6) month, avg(totals.totalTransactionRevenue)/(1000000) as avg_revenue_by_user_per_visit, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _table_suffix between '20170701' and '20170731'
GROUP BY month

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)

#standardSQL
with sub1 as (
    SELECT fullVisitorId, v2ProductName, 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    CROSS JOIN Unnest(hits)
    CROSS JOIN unnest(product)
    WHERE v2ProductName = "YouTube Men's Vintage Henley" and productRevenue is not null
)
SELECT 
    v2ProductName as other_purchased_products,
    sum(productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
CROSS JOIN Unnest(hits)
CROSS JOIN unnest(product)
WHERE fullVisitorId in (select fullVisitorId from sub1)
AND productRevenue is not null
AND v2ProductName != "YouTube Men's Vintage Henley"
GROUP BY v2ProductName
GROUP BY quantity desc;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

#standardSQL
with sub1 as (
 SELECT 
     format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
     COUNT(v2ProductName) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
WHERE ecommerceaction.action_type = '2'
AND (isImpression IS NULL OR isImpression = FALSE)
GROUP BY month),

sub2 as (
    SELECT 
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        COUNT(v2ProductName) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
WWHERE ecommerceaction.action_type = '3'
GROUP BY month),

sub3 as (
     SELECT
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        COUNT(v2ProductName) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
WWHERE ecommerceaction.action_type = '6'
GROUP BY month)

SELECT
    sub1.month,
    num_product_view,
    num_addtocart,
    num_purchase,
    round(Safe_divide(num_addtocart,num_product_view)*100,2) as add_to_cart_rate,
    round(Safe_divide(num_purchase,num_product_view)*100,2) as purchase_rate
FROM sub1
JOIN sub2 using(month) 
JOIN sub3 using(month) 
ORDER BY month
LIMIT 3;


