-- depends_on: "defaultdb"."prepare"."fake_knn_data"

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
        AVG(b.valeur) AS prix_m2_knn_2
    FROM 
        "defaultdb"."prepare"."fake_knn_data" a
        JOIN LATERAL (
            SELECT valeur
            FROM "defaultdb"."prepare"."fake_knn_data"
            WHERE (id != a.id)
            ORDER BY a.geopoint <-> geopoint
            LIMIT 2
        ) b ON TRUE
    GROUP BY a.id
)

SELECT * FROM knn



)

-- Compare computed and expected values
SELECT 
    computed.id, 
    computed.prix_m2_knn_2 AS computed_valeur, 
    expected.expected_valeur
FROM 
    computed
    JOIN expected ON computed.id = expected.id
WHERE
    computed.prix_m2_knn_2 != expected.expected_valeur