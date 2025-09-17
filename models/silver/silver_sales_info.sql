WITH sales as
(
    SELECT 
        sales_id,
        product_sk,
        customer_sk,
        {{ multiply('unit_price', 'quantity') }} as calculated_amount,
        gross_amount,
        payment_method

    FROM {{ ref('bronze_sales') }}
),

products as
(
    SELECT 
        product_sk,
        category

    FROM {{ ref('bronze_product') }}
), 

customers as
(
    SELECT 
        customer_sk,
        gender

    FROM {{ ref('bronze_customer') }}
),

joined_query as (
SELECT 
    sales.sales_id,
    sales.product_sk,
    sales.customer_sk,
    sales.gross_amount,
    sales.calculated_amount,
    sales.payment_method,
    products.product_sk,
    products.category,
    customers.customer_sk,
    customers.gender

FROM 
    sales

LEFT JOIN products
    ON sales.product_sk = products.product_sk
LEFT JOIN customers
    ON sales.customer_sk = customers.customer_sk
)

SELECT
    category,
    gender,
    ROUND(sum(gross_amount), 2) as total_gross_amount
FROM
    joined_query
GROUP BY
    category, gender
ORDER BY
    category, total_gross_amount DESC