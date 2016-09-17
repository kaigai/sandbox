CREATE FUNCTION knn_cpu_similarity(int, int[], int[])
RETURNS real[]
AS 'MODULE_PATHNAME', 'knn_cpu_similarity'
LANGUAGE C STRICT;

