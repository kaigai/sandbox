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
# att_val - vector of the property columns (string[])
# nloops  - number of iteration (integer)
#
pgsql_minmax_tryblock <- function(conn, relname, att_pk, att_val, nloops)
{
  tv1 <- Sys.time()
  # Init: construction of 'subset_table'
  # ------------------------------------
  sql1a <- "SELECT * INTO pg_temp.subset_table FROM " || relname ||
           " ORDER BY random() LIMIT 1"
  if (PgParam_PrintSQL)
  {
    print("Init SQL Run: " || sql1a)
  }
  dbGetQuery(conn, sql1a)
  sql1b <- "VACUUM ANALYZE pg_temp.subset_table"
  dbGetQuery(conn, sql1b)

  # Init: construction of 'dist_table'
  # ----------------------------------
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
    print("Init SQL Run: " || sql2a)
  }
  dbGetQuery(conn, sql2a)
  sql2b <- "VACUUM ANALYZE pg_temp.dist_table"
  dbGetQuery(conn, sql2b)

  elapsed_sec <- as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("initialize: %.3f sec", elapsed_sec, quote=FALSE))

  # Repeat: pick a largest distance item, compute new distance,
  #         then store the least one
  # -----------------------------------------------------------
  sql3a <- "SELECT r1." || att_pk || ", LEAST(d.dist, sqrt("
  is_first <- 1
  for (att in att_val)
  {
    sql3a <- sql3a || ifelse(is_first, "", " + ") ||
            "(r1." || att || " - r2." || att || ")^2"
    is_first <- 0
  }    
  sql3a <- sql3a || ")) dist " ||
          "INTO pg_temp.dist_table_new " ||
          "FROM " || relname || " r1, " ||
                "pg_temp.dist_table d, " ||
               "(SELECT main.* FROM " || relname || " main, " ||
                     "(SELECT id FROM pg_temp.dist_table " ||
                            "ORDER BY dist DESC LIMIT 1) largest " ||
         "WHERE main." || att_pk || " = largest." || att_pk || ") r2 " ||
         "WHERE r1." || att_pk || " = d." || att_pk
  sql3b <- "VACUUM ANALYZE pg_temp.dist_table_new"
  sql3c <- "DROP TABLE pg_temp.dist_table"
  sql3d <- "ALTER TABLE pg_temp.dist_table_new RENAME TO dist_table"

  if (PgParam_PrintSQL)
  {
    print("Repeat SQL is: " || sql3a)
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
  sql4 <- "SELECT * FROM pg_temp.dist_table"
  return(dbGetQuery(conn, sql4))
}

pgsql_minmax <- function(relname, att_pk, att_val, nloops)
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

  res <- try(pgsql_minmax_tryblock(conn, relname, att_pk, att_val, nloops))
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
  stop("under construction");
}
