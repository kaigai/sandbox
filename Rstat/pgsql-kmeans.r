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
    return(null)
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("k-means total: %.3f sec", elapsed_sec, quote=FALSE))

  return(res)
}