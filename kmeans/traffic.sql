EXPLAIN ANALYZE
WITH
summary AS (
SELECT report_id, k, c
  FROM (SELECT report_id, k, c,
               row_number() OVER (PARTITION BY report_id ORDER BY c DESC) rank
          FROM (SELECT report_id, k, count(*) c
                  FROM matrix_unnest(
                          (SELECT gpu_kmeans(array_matrix(int4_as_float4(report_id),
                                             avg_measured_time,
                                             avg_speed,
                                             vehicle_count),
                                             5)
                             FROM tr_rawdata
--                            WHERE extract('hour' from timestamp) between 7 and 17
--                            WHERE extract('hour' from timestamp) between 18 and 23 OR
--                                  extract('hour' from timestamp) 0 and 7
--                            WHERE extract(dow from timestamp) in (0,6)
--                            WHERE extract(dow from timestamp) in (1,2,3,4,5)
                          )
                       ) R(report_id int, k int)
                 GROUP BY report_id, k
               ) __summary_1
       ) __summary_2
   WHERE rank = 1
),
location AS (
SELECT point_1_lat, point_1_lng,
       point_2_lat, point_2_lng,
       CASE k WHEN 1 THEN 'red'
              WHEN 2 THEN 'blue'
              WHEN 3 THEN 'green'
              WHEN 4 THEN 'purple'
              ELSE 'orange'
       END col
  FROM summary s, tr_metadata m
 WHERE s.report_id = m.report_id
),
path_definition AS (
SELECT 'path=color:' || col || '|weight:3|' ||
       point_1_lat::text || ',' || point_1_lng::text || '|' ||
       point_2_lat::text || ',' || point_2_lng::text path_entry
  FROM location
 LIMIT 125 -- becuase of Goole Map API restriction
)
SELECT 'http://maps.google.com/maps/api/staticmap?' ||
       'zoom=11&' ||
       'size=640x480&' ||
       'scale=2&' ||
       string_agg(path_entry, '&') ||
       '&sensor=false'
  FROM path_definition;
