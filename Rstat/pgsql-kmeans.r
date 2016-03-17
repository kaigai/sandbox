require("RPostgreSQL")

#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "chembl"
PgParam_port <- 5432
# set "on" or "off" to turn on/off PG-Strom
PgParam_PGStrom_Enabled <- "default"
# set FALSE to turn off print SQL
PgParam_PrintSQL <- TRUE

#
# Utility routines
#

"||" <- function(a,b)
{
  if (is.character(c(a,b))) {
    base::paste(a,b,sep="")
  } else {
    base::"||"(a,b)
  }
}

# just for my convenience
vatts_full <- c("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10",
                "c11","c12","c13","c14","c15","c16","c17","c18","c19","c20",
                "c21","c22","c23","c24","c25","c26","c27","c28","c29","c30",
                "c31","c32","c33","c34","c35","c36","c37","c38","c39","c40",
                "c41","c42")
vatts_small <- c("c1","c2","c3","c4","c5","c6","c7","c8","c9","c10")

#
# pgsql_kmeans_init_random
#
# It makes initial pg_temp.centroid according to the random distribution 
# ------
# arguments:
# conn - PostgreSQL connection
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# n_clusters - number of clusters (integer)
#
pgsql_kmeans_init_random <- function(conn, relname, att_pk, att_val, n_clustrs)
{
  tv1 <- Sys.time()
  #
  # SQL1: construction of pg_temp.centroid according to the fully random
  #       manner
  #
  # TARGET QUERY
  # ------------------------------
  # SELECT floor(random() * N_CLUSTERS)::int + 1 cid,
  #        avg(c1) c1, avg(c2) c2, ..., avg(cN) cN
  #   INTO pg_temp.centroid
  #   FROM RELNAME
  #  GROUP BY cid;
  # VACUUM ANALYZE pg_temp.centroid
  #
  sql1 <- "SELECT floor(random() * " || n_clustrs || ")::int + 1 cid"
  for (att in att_val)
  {
    sql1 <- sql1 || ", AVG(" || att || ") " || att
  }
  sql1 <- sql1 || " INTO pg_temp.centroid" ||
                  " FROM " || relname ||
                 " GROUP BY cid"
  if (PgParam_PrintSQL)
  {
    print("Init SQL1: " || sql1, quote=FALSE)
  }
  dbGetQuery(conn, sql1);

  sql2 <- "VACUUM ANALYZE pg_temp.centroid"
  dbGetQuery(conn, sql2);

  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("k-means random init: %.3f sec", elapsed_sec, quote=FALSE))
}

#
# pgsql_kmeans_init_plus
#
# It makes initial pg_temp.centroid according to the k-means++ algorithm
# ------
# arguments:
# conn - PostgreSQL connection
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# n_clusters - number of clusters (integer)
#
pgsql_kmeans_init_plus <- function(conn, relname, att_pk, att_val, n_clustrs)
{
  tv1 <- Sys.time()
  #
  # SQL1 : choose an initial centroid item based on random manner
  #
  # TARGET QUERY
  # --------------------------------
  # SELECT 1 cid, c1, c2, ..., cN
  #   INTO pg_temp.centroid
  #   FROM RELNAME
  #  ORDER BY random()
  #  LIMIT 1
  #
  sql1a <- "DROP TABLE IF EXISTS pg_temp.centroid"
  sql1b <- "SELECT 1 cid"
  for (att in att_val)
  {
    sql1b <- sql1b || ", " || att
  }
  sql1b <- sql1b || " INTO pg_temp.centroid FROM " || relname ||
                    " ORDER BY random() LIMIT 1"
  sql1c <- "VACUUM ANALYZE pg_temp.centroid"
  if (PgParam_PrintSQL)
  {
    print("SQL1: " || sql1b, quote=FALSE)
  }
  dbGetQuery(conn, sql1a)
  dbGetQuery(conn, sql1b)
  dbGetQuery(conn, sql1c)

  #
  # SQL2: compute dist^2 from the closest centroid for each items,
  #       then choose an item randomly but far item will be chosen
  #       more frequently
  #
  # TARGET QUERY
  # --------------------------------
  # -- CTE dist2_next calculates representative distance from each
  # -- centroid and picks up the closest distance^2 for each items
  # WITH dist2_next AS (
  # SELECT row_number() OVER() rowid, did, dist2
  #   FROM (SELECT row_number() OVER w rank, cid, did, dist2
  #           FROM (SELECT c.cid, r.id did, (c.c1 - r.c1)^2 +
  #                                         (c.c2 - r.c2)^2 +
  #                                               :
  #                                         (c.cN - r.cN)^2 dist2
  #                   FROM pg_temp.centroid c, RELNAME r) dist2_all
  #         WINDOW w AS (PARTITION BY did ORDER BY dist2)
  #        ) dist2_rank
  #  WHERE rank = 1
  # )
  # -- The subquery dist2_cumulative makes ratio of cumulative sum
  # -- towards entire sum of the distance^2, then next_one picks up
  # -- the first item that exceeds an random value. Farther items
  # -- are more frequently chosen, due to probabilistic reason.
  # --
  # INSERT INTO pg_temp.centroid
  #   (SELECT (SELECT MAX(cid)+1 FROM pg_temp.centroid),
  #           c1, c2, ..., cN
  #      FROM (SELECT did
  #              FROM (SELECT did, SUM(dist2) OVER (ORDER BY rowid) /
  #                        (SELECT SUM(dist2) FROM dist2_next) cumulative
  #                      FROM dist2_next) dist2_cumulative
  #             WHERE cumulative > (SELECT random()) LIMIT 1) next_one
  #           , RELNAME r
  #     WHERE next_one.did = r.id)
  #
  sql2a <- "WITH dist2_next AS (\n" ||
  	   "SELECT row_number() OVER() rowid, did, dist2 " ||
             "FROM (SELECT row_number() OVER w rank, did, dist2 " ||
                     "FROM (r.id did"
  is_first <- TRUE
  for (att in att_val)
  {
    sql2a <- sql2a || ifelse(is_first, "", " + ") ||
             "(c." || att || " - r." || att || ")^2"
    is_first <- FALSE
  }
  sql2a <- sql2a || " dist2 FROM pg_temp.centroid c, mqn r) dist2_all " ||
       "WINDOW w AS (PARTITION BY did ORDER BY dist2)) dist2_rank" ||
       "WHERE rank = 1)\n"

  sql2a <- sql2a || "INSERT INTO pg_temp.centroid " ||
            "(SELECT (SELECT MAX(cid) + 1 FROM pg_temp.centroid)" ||
  for (att in att_val)
  {
    sql2a <- sql2a || ", " || att
  }
  sql2a <- sql2a || " FROM (SELECT did " ||
    "FROM (SELECT did, SUM(dist2) OVER (ORDER BY rowid) / " ||
              "(SELECT SUM(dist2) FROM dist2_next) cumulative " ||
            "FROM dist2_next) dist2_pickup " ||
           "WHERE cumulative > (SELECT random()) LIMIT 1) next_one, " ||
          relname || " r WHERE next_one.did = r.id)"

  sql2b <- "VACUUM ANALYZE pg_temp.centroid"

  if (PgParam_PrintSQL)
  {
    print("SQL2: " || sql2a, quote=FALSE)
  }

  for (loop in c(1:n_clusters))
  {
    dbGetQuery(conn, sql2a)
    dbGetQuery(conn, sql2b)
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("k-means plus init: %.3f sec", elapsed_sec, quote=FALSE))
}

#
# pgsql_kmeans_main - main iteration part of k-means algorithm
#
# ------
# arguments:
# conn - PostgreSQL connection
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# max_loops - max number of iteration, if threshold is not supplied
#             (integer, default: 20)
# threshold - number of different items to break k-means iteration
#             (integer, default: 0)
#
# NOTE: pg_temp.centroid has to be already built
#
pgsql_kmeans_main <- function(conn, relname, att_pk, att_val,
                              max_loops, threshold)
{
  #
  # SQL1: construction of pg_temp.cluster_map_new according to the current
  #       pg_temp.centroid. We try to compute distance for each combination
  #       of {centroid} X {RELNAME}, then picks up the closest cluster for
  #       each item in RELNAME table.
  #
  # TARGET QUERY
  # ----------------------------
  # SELECT did, cid INTO pg_temp.cluster_map_new
  #   FROM (SELECT row_number() OVER w rank, did, cid
  #           FROM (SELECT r.id did, c.cid, sqrt((c.c1 - r.c1)^2 +
  #                                              (c.c2 - r.c2)^2 +
  #                                                    :
  #                                              (c.cN - r.cN)^2) dist
  #                   FROM RELNAME r, pg_temp.centroid c) all_dist
  #         WINDOW w AS (PARTITION BY did ORDER BY dist)) all_dist_rank
  #  WHERE rank = 1
  #
  sql1a <- "SELECT did, cid INTO pg_temp.cluster_map_new " ||
             "FROM (SELECT row_number() OVER w rank, did, cid " ||
                     "FROM (SELECT r." || att_pk || " did, c.cid, sqrt("
  is_first <- TRUE
  for (att in att_val)
  {
    sql1a <- sql1a || ifelse(is_first, "", " + ") ||
               	   "(c." || att || " - r." || att || ")^2"
    is_first <- FALSE
  }
  sql1a <- sql1a || ") dist " ||
            "FROM " || relname || " r, pg_temp.centroid c) all_dist " ||
          "WINDOW w AS (PARTITION BY did ORDER BY dist)) all_dist_rank " ||
          "WHERE rank = 1"
  sql1b <- "VACUUM ANALYZE pg_temp.cluster_map_new"

  #
  # SQL2: update pg_temp.centroid according to the "cluster_map_new"
  #
  # TARGET QUERY
  # ----------------------------
  # SELECT cm.cid, AVG(r.c1) c1, AVG(r.c2) c2, ..., AVG(r.cN) cN
  #   INTO pg_temp.centroid
  #   FROM pg_temp.cluster_map_new cm, RELNAME r
  #  WHERE cm.did = r.ATT_PK
  #  GROUP BY cm.cid
  #
  sql2 <- "DROP TABLE IF EXISTS pg_temp.centroid; \n"
  sql2 <- sql2 || "SELECT cm.cid"
  for (att in att_val)
  {
    sql2 <- sql2 || ", AVG(r." || att || ") " || att
  }
  sql2 <- sql2 || " FROM pg_temp.centroid c, " || relname || " r" ||
                  " WHERE cm.did = r." || att_pk ||
                  " GROUP BY cm.cid; \n"
  sql2 <- sql2 || "VACUUM ANALYZE pg_temp.centroid"

  #
  # SQL3: rename cluster_map_new to cluster_map
  #
  # TARGET QUERY
  # ----------------------------
  # DROP TABLE IF EXISTS pg_temp.cluster_map
  # ALTER TABLE pg_temp.cluster_map_new RENAME TO cluster_map
  #
  sql3 <- "DROP TABLE IF EXISTS pg_temp.cluster_map;\n"
  sql3 <- "ALTER TABLE pg_temp.cluster_map_new RENAME TO cluster_map"

  #
  # SQL4: count difference between cluster_map and cluster_map_new
  #
  # TARGET QUERY
  # ----------------------------
  # SELECT count(*) count
  #   FROM pg_temp.cluster_map o, pg_temp.cluster_map_new n
  #  WHERE o.did = n.did AND o.cid != n.cid
  #
  sql4 <- "SELECT count(*) count " ||
          "  FROM pg_temp.cluster_map o, pg_temp.cluster_map_new n" ||
	  " WHERE o.did = n.did AND o.cid != n.cid"

  #
  # SQL5: dump the cluster_map + RELNAME
  #
  # TARGET QUERY
  # ----------------------------
  # SELECT c.cid, r.*
  #   FROM pg_temp.cluster_map c, RELNAME r
  #  WHERE c.did = r.ATT_PK
  #
  sql5 <- "SELECT c.cid, r.* " ||
            "FROM pg_temp.cluster_map_new c, " || relname || " r " ||
           "WHERE c.did = r." || att_pk;

  #
  # RUN Above Queries
  # -----------------
  loop <- 1

  tv2 <- Sys.time()
  # init: make pg_temp.cluster_map_new
  dbGetQuery(conn, sql1a)
  dbGetQuery(conn, sql1b)
  # init: make pg_temp.centroid based on the pg_temp.cluster_map_new
  dbGetQuery(conn, sql2)

  elapsed_sec = as.double(difftime(Sys.time(), tv2, units="secs"))
  print(sprintf("k-means %dth: %.3f sec", loop, elapsed_sec, quote=FALSE))

  loop <- loop + 1
  while (max_loops <= 0 || loop < max_loops)
  {
    tv1 <- tv2
    # loop: rename cluster_map_new -> cluster_map
    dbGetQuery(conn, sql3)
    # loop: make pg_temp.cluster_map_new
    dbGetQuery(conn, sql1a)
    dbGetQuery(conn, sql1b)

    tv2 <- Sys.time()
    elapsed_sec = as.double(difftime(tv2, tv1, units="secs"))
    print(sprintf("k-means %dth: %.3f sec", loop, elapsed_sec, quote=FALSE))

    if (threshold > 0)
    {
      # loop: compare cluster_map with cluster_map_new and count differences
      res = dbGetQuery(conn, sql4)
      count = res[[1, "count"]]

      if (count <= threshold) {break}
    }
    # loop: make pg_temp.centroid based on cluster_map_new
    dbGetQuery(conn, sql2)

    loop <- loop + 1
  }
  # end: dump the cluster_map + RELNAME
  res <- dbGetQuery(conn, sql5)
  elapsed_sec = as.double(difftime(Sys.time(), tv2, units="secs"))
  print(sprintf("k-means result dump: %.3f sec", elapsed_sec, quote=FALSE))

  return(res)
}

#
# pgsql_kmeans2
#
# arguments:
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# n_clusters - number of clusters (integer)
# max_loops - max number of k-means iteration (integer, default: 20)
# threshold - number of differences to break iteration (integer, default: 0)
#
pgsql_kmeans2 <- function(relname, att_pk, att_val, n_clusters,
                          max_loops=20, threshold=0)
{
  tv1 <- Sys.time()
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
		    port=PgParam_port)
  # turn on/off PG-Strom
  if (PgParam_PGStrom_Enabled == "on")
  {
    dbGetQuery(conn, "SET pg_strom.enabled=on")
  }
  else if (PgParam_PGStrom_Enabled == "off")
  {
    dbGetQuery(conn, "SET pg_strom.enabled=off")
  }
  # make initial centroid
  res <- try(pgsql_kmeans_init_random(conn, relname, att_pk, att_val,
                                      n_clusters))
  if (class(res) == "try-error")
  {
    dbDisconnect(conn)
    return(Null)
  }
  # iterate k-means steps
  res <- try(pgsql_kmeans_main(conn, relname, att_pk, att_val,
                               max_loops, threshold))
  dbDisconnect(conn)
  if (class(res) == "try-error")
  {
    return(Null)
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("k-means total: %.3f sec", elapsed_sec, quote=FALSE))

  return(res)
}







#
# pgsql_kmeans - returns pair of id and cluster for each item
#
# arguments:
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# n_clusters - number of clusters (integer)
# threshold - maximum distance to continue k-means repeat (float, default: 0.0)
#
pgsql_kmeans_tryblock <- function(conn, relname, att_pk, att_val,
                                  n_clusters, threshold)
{
  #
  # Init: construction of pg_temp.cluster_map based on random
  #
  sql1 <- "SELECT " || att_pk || " did, " ||
              "(random() * " || n_clusters || ")::int + 1 cid " ||
            "INTO pg_temp.cluster_map " ||
            "FROM " || relname
  if (PgParam_PrintSQL)
  {
    print("SQL1: " || sql1, quote=FALSE)
  }

  #
  # Repeat: construction of pg_temp.centroid according to the cluster_map
  #
  sql2a <- "DROP TABLE IF EXISTS pg_temp.centroid"
  sql2b <- "SELECT cid"
  for (att in att_val)
  {
    sql2b <- sql2b || ", avg(" || att || ") " || att
  }
  sql2b <- sql2b || " INTO pg_temp.centroid " ||
                  "FROM pg_temp.cluster_map c, " || relname || " r " ||
                  "WHERE c.did = r." || att_pk || " GROUP BY cid"
  sql2c <- "VACUUM ANALYZE pg_temp.centroid"
  if (PgParam_PrintSQL)
  {
    print("SQL2: " || sql2b, quote=FALSE)
  }

  #
  # Repeat: calculation of the distance between each item and centroid,
  #         then item shall belong to the closest cluster on the next
  #
  sql3a <- "SELECT did, cid INTO pg_temp.cluster_map_new " ||
             "FROM (SELECT row_number() OVER w rank, did, cid " ||
                     "FROM (SELECT r." || att_pk || " did, c.cid, sqrt("
  is_first <- 1
  for (att in att_val)
  {
    sql3a <- sql3a || ifelse(is_first, "", " + ") ||
            "(r." || att || " - c." || att || ")^2"
    is_first <- 0
  }
  sql3a <- sql3a || ") dist " ||
          "FROM " || relname || " r, pg_temp.centroid c) new_dist " ||
        "WINDOW w AS (PARTITION BY did ORDER BY dist)) new_dist_rank " ||
        "WHERE rank = 1"
  sql3b <- "VACUUM ANALYZE pg_temp.cluster_map_new";
  if (PgParam_PrintSQL)
  {
    print("SQL3: " || sql3a, quote=FALSE)
  }

  #
  # Repeat: check differences between cluster_map and cluster_map_new
  #
  sql4 <- "SELECT count(*) count " ||
            "FROM pg_temp.cluster_map o, pg_temp.cluster_map_new n " ||
           "WHERE o.did = n.did AND o.cid != n.cid"
  if (PgParam_PrintSQL)
  {
    print("SQL4: " || sql4, quote=FALSE)
  }

  #
  # Repeat: rename pg_temp.cluster_map_new to cluster_map
  #
  sql5a <- "DROP TABLE IF EXISTS pg_temp.cluster_map"
  sql5b <- "ALTER TABLE pg_temp.cluster_map_new RENAME TO cluster_map"
  if (PgParam_PrintSQL)
  {
    print("SQL5: " || sql5a, quote=FALSE)
  }

  #
  # Final: Dump the cluster_map
  #
  sql6 <- "SELECT c.cid, r.* " ||
            "FROM pg_temp.cluster_map c, " || relname || " r " ||
           "WHERE c.did = r." || att_pk;

  #-- execution and iteration --
  dbGetQuery(conn, sql1);

  count <- threshold + 1
  loop <- 1
  tv1 <- Sys.time()
  while (count > threshold)
  {
    # construction of pg_temp.centroid
    dbGetQuery(conn, sql2a)
    dbGetQuery(conn, sql2b)
    dbGetQuery(conn, sql2c)

    # construction of pg_temp.cluster_map_new
    dbGetQuery(conn, sql3a)
    dbGetQuery(conn, sql3b)

    # count difference
    res = dbGetQuery(conn, sql4)
    tv2 <- Sys.time();
    elapsed_sec = as.double(difftime(tv2, tv1, units="secs"))

    count = res[[1, "count"]]
    print(sprintf("%dth trial: count=%d, time=%.3fsec",
                  loop, count, elapsed_sec), quote=FALSE)

    # preparation of the next execution
    dbGetQuery(conn, sql5a)
    dbGetQuery(conn, sql5b)

    tv1 <- tv2
    loop <- loop + 1
  }
  # Dump the latest pg_temp.cluster_map
  return(dbGetQuery(conn, sql6))
}

pgsql_kmeans <- function(relname, att_pk, att_val,
                         n_clusters, threshold=1000)
{
  tv1 <- Sys.time()
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
		    port=PgParam_port)
  # turn on/off PG-Strom
  if (PgParam_PGStrom_Enabled == "on")
  {
    dbGetQuery(conn, "SET pg_strom.enabled=on")
  }
  else if (PgParam_PGStrom_Enabled == "off")
  {
    dbGetQuery(conn, "SET pg_strom.enabled=off")
  }
  res <- try(pgsql_kmeans_tryblock(conn,relname, att_pk, att_val,
                                   n_clusters, threshold))
  # clean up database session on error
  dbDisconnect(conn)

  if (class(res) == "try-error")
  {
    return(Null)
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("k-means total: %.3f sec", elapsed_sec, quote=FALSE))

  return(res)
}
