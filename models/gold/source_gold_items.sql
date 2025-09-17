WITH  dedupe_ranked AS (

SELECT 
    *,
    row_number() OVER (PARTITION BY id ORDER BY updateDate DESC) AS dedupe_rank
FROM {{ source('source', 'items') }}
)

SELECT
    id,
    name,
    category,
    updateDate
FROM dedupe_ranked

WHERE 
    dedupe_rank = 1