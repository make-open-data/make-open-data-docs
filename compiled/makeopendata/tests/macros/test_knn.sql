-- Define expected values
WITH expected AS (
    SELECT 1 AS id, 250 AS expected_valeur UNION ALL
    SELECT 3,       200 UNION ALL
    SELECT 2,       150 UNION ALL
    SELECT 4,       300 UNION ALL
    SELECT 6,       250
),

-- Calculate knn for each row in the fake table
computed AS (
    
WITH knn AS (
    SELECT 
        a.id AS id,
        AVG(b.valeur) AS mean_knn_value
    FROM 
        "make_open_data"."prep"."fake_knn_data" a
        JOIN LATERAL (
            SELECT valeur
            FROM "make_open_data"."prep"."fake_knn_data"
            WHERE id != a.id
            ORDER BY ST_SetSRID(ST_MakePoint(a.longitude, a.latitude), 4326) <-> ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            LIMIT 2
        ) b ON TRUE
    GROUP BY a.id
)

SELECT 
    a.*,
    b.mean_knn_value
FROM 
    "make_open_data"."prep"."fake_knn_data" a
    JOIN knn b ON a.id = b.id

)

-- Compare computed and expected values
SELECT 
    computed.id, 
    computed.mean_knn_value AS computed_valeur, 
    expected.expected_valeur
FROM 
    computed
    JOIN expected ON computed.id = expected.id
WHERE
    computed.mean_knn_value != expected.expected_valeur