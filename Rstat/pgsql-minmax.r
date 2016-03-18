require("RPostgreSQL")

#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "chembl"
PgParam_port   <- 5432
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
# pgsql_minmax - returns id and distance of the items
#
# arguments:
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_label - name of the human readable label column (string)
# att_val - vector of the property columns (string[])
# nloops  - number of iteration (integer)
#
pgsql_minmax_tryblock <- function(conn, relname,
                                  att_pk, att_label, att_val, nloops)
{
  tv1 <- Sys.time()
  # Init: construction of 'subset_table' based on random manner
  #
  # TARGET QUERY
  # ----------------------------------
  # SELECT * INTO pg_temp.subset_table
  #   FROM RELNAME
  #  ORDER BY random()
  #  LIMIT 1
  #
  sql1a <- "SELECT * INTO pg_temp.subset_table FROM " || relname ||
           " ORDER BY random() LIMIT 1"
  if (PgParam_PrintSQL)
  {
    print("init SQL1: " || sql1a)
  }
  dbGetQuery(conn, sql1a)
  sql1b <- "VACUUM ANALYZE pg_temp.subset_table"
  dbGetQuery(conn, sql1b)

  # Init: construction of 'dist_table'
  #
  # TARGET QUERY
  # ----------------------------------
  # SELECT r.ATT_PK, sqrt((r.c1 - c.c1) +
  #                       (r.c2 - c.c2) +
  #                             :
  #                       (r.cN - c.cN)) dist
  #   INTO pg_temp.dist_table
  #   FROM (SELECT * FROM pg_temp.subset_table LIMIT 1) s,
  #        RELNAME r
  #
  sql2a <- "SELECT r." || att_pk || ", sqrt("
  is_first <- 1
  for (att in att_val)
  {
    sql2a <- sql2a || ifelse(is_first, "", " + ") ||
            "(r." || att || " - s." || att || ")^2"
    is_first <- 0
  }
  sql2a <- sql2a || ") dist " ||
          "INTO pg_temp.dist_table " ||
          "FROM (SELECT * FROM pg_temp.subset_table LIMIT 1) s, " ||
          relname || " r"
  if (PgParam_PrintSQL)
  {
    print("init SQL2: " || sql2a)
  }
  dbGetQuery(conn, sql2a)
  sql2b <- "VACUUM ANALYZE pg_temp.dist_table"
  dbGetQuery(conn, sql2b)

  elapsed_sec <- as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("initialize: %.3f sec", elapsed_sec, quote=FALSE))

  #
  # Repeat: pick up a largest distance item, and insert it into
  #         the pg_temp.subset_table, then make a new distance
  #         table.
  #
  # TARGET QUERY
  # ------------
  # WITH next_item AS (
  #   INSERT INTO pg_temp.subset_table
  #       (SELECT * FROM RELNAME r,
  #                      (SELECT ATT_PK
  #                         FROM pg_temp.dist_table
  #                        ORDER BY dist
  #                        LIMIT 1) d
  #         WHERE r.ATT_PK = d.ATT_PK)
  #   RETURNING *
  # )
  # SELECT r.ATT_PK, LEAST(d.dist, sqrt((r.c1 - n.c1)^2 +
  #                                     (r.c2 - n.c2)^2 +
  #                                           :
  #                                     (r.cN - n.cN)^2)) dist
  #   INTO pg_temp.dist_table_new
  #   FROM pg_temp.dist_table d,
  #        next_item n,
  #        RELNAME r
  #  WHERE r.ATT_PK = d.ATT_PK
  #
  sql3a <- "WITH next_item AS (" ||
             "INSERT INTO pg_temp.subset_table" ||
                   "(SELECT r.* FROM " || relname || " r," ||
	                    "(SELECT " || att_pk ||
	                      " FROM pg_temp.dist_table" ||
                             " ORDER BY dist DESC" ||
                             " LIMIT 1) d" ||
                   " WHERE r." || att_pk || " = " || "d." || att_pk || ")" ||
             "RETURNING *" ||
	   ")"
  sql3a <- sql3a || "SELECT r." || att_pk || ", LEAST(d.dist, sqrt("
  is_first <- TRUE
  for (att in att_val)
  {
    sql3a <- sql3a || ifelse(is_first, "", " + ") ||
             "(r." || att || " - n." || att || ")^2"
    is_first <- FALSE
  }
  sql3a <- sql3a || ")) dist" ||
           "  INTO pg_temp.dist_table_new" ||
           "  FROM pg_temp.dist_table d," ||
	   "       next_item n, " || relname || " r" ||
	   " WHERE r." || att_pk || " = d." || att_pk
  sql3b <- "VACUUM ANALYZE pg_temp.dist_table_new"
  sql3c <- "DROP TABLE IF EXISTS pg_temp.dist_table"
  sql3d <- "ALTER TABLE pg_temp.dist_table_new RENAME TO dist_table"

  if (PgParam_PrintSQL)
  {
    print("repeat SQL3: " || sql3a)
  }

  for (i in 1:nloops)
  {
    tv1 <- Sys.time()
    dbGetQuery(conn, sql3a)
    dbGetQuery(conn, sql3b)
    dbGetQuery(conn, sql3c)
    dbGetQuery(conn, sql3d)
    elapsed_sec <- as.double(difftime(Sys.time(), tv1, units="secs"))
    print(sprintf("%dth repeat: %.3f sec", i, elapsed_sec, quote=FALSE))
  }

  # Final: obtain the final result from the dist_table
  # --------------------------------------------------
  sql4 <- "SELECT " || att_pk || ", " || att_label ||
           " FROM pg_temp.subset_table"
  return(dbGetQuery(conn, sql4))
}

pgsql_minmax <- function(relname, att_pk, att_label, att_val, nloops)
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

  res <- try(pgsql_minmax_tryblock(conn, relname,
                                   att_pk, att_label, att_val, nloops))
  # clean up database session on error
  dbDisconnect(conn)

  if (class(res) == "try-error")
  {
    return(null)
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("Min-Max total: %.3f sec", elapsed_sec, quote=FALSE))

  return(res)
}

#
# pgsql_minmax_bycpu - returns id and distance of the items
#       (*) It runs all the calculation stuff on the CPU side
#           once data is exported from the remote PostgreSQL
# arguments:
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# nloops  - number of iteration (integer)
#
pgsql_minmax_bycpu <- function(relname, att_pk, att_val, nloops)
{
  tv1 <- Sys.time()
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port)
  sql <- "SELECT " || att_pk
  for (att in att_val)
  {
    sql <- sql || ", " || att
  }
  sql <- sql || " FROM " || relname
  rel <- dbGetQuery(conn, sql)
  # Close the PostgreSQL session
  dbDisconnect(conn)

  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("Download : %.3f sec", elapsed_sec, quote=FALSE))

  t1 <- Sys.time()
  # extract 'att_val' portion and make a transportation form
  vrel <- t(as.matrix(rel[,c(1:length(att_val))+1]))

  # pick up an item randomly, then insert to subset
  index <- trunc(runif(1,1,ncol(vrel)))
  item <- vrel[,index]
  subset <- rbind(c(index, rel[index,1]))
  print("init pickup")

  # make a distance table
  dist <- sqrt(colSums((vrel - item)^2))

  for (count in c(1:nloops))
  {
    # pickup a new item that has largest distance
    index <- which.max(dist)
    item <- vrel[,index]
    subset <- rbind(subset, c(index, rel[index,1]))

    # comput new distance table
    dist_new <- sqrt(colSums((vrel - item)^2))
    dist <- ifelse(dist < dist_new, dist, dist_new)

    print(sprintf("%dth pickup", count))
  }
  elapsed_sec = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("Execution : %.3f sec", elapsed_sec, quote=FALSE))

  return(subset)
}
