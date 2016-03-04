require("RPostgreSQL")

#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "chembl"

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
  dbGetInfo(conn)

  # Init: construction of 'subset_table'
  # ------------------------------------
  sql1 <- "SELECT * INTO pg_temp.subset_table FROM " || relname ||
          " ORDER BY random() LIMIT 1"
  print("Init SQL Run: " || sql1)
  dbGetQuery(conn, sql1)

  # Init: construction of 'dist_table'
  # ----------------------------------
  sql2 <- "SELECT r." || att_pk || ", sqrt("
  is_first <- 1
  for (att in att_val)
  {
    sql2 <- sql2 || ifelse(is_first, "", " + ") ||
            "(r." || att || " - s." || att || ")^2"
    is_first <- 0
  }
  sql2 <- sql2 || ") dist " ||
          "INTO pg_temp.dist_table " ||
          "FROM (SELECT * FROM pg_temp.subset_table LIMIT 1) s, " ||
          relname || " r"
  print("Init SQL Run: " || sql2)
  dbGetQuery(conn, sql2)

  # Repeat: pick a largest distance item, compute new distance,
  #         then store the least one
  # -----------------------------------------------------------
  sql3 <- "SELECT r1." || att_pk || ", LEAST(d.dist, sqrt("
  is_first <- 1
  for (att in att_val)
  {
    sql3 <- sql3 || ifelse(is_first, "", " + ") ||
            "(r1." || att || " - r2." || att || ")^2"
    is_first <- 0
  }    
  sql3 <- sql3 || ")) dist " ||
          "INTO pg_temp.dist_table_new " ||
          "FROM " || relname || " r1, " ||
                "pg_temp.dist_table d, " ||
               "(SELECT main.* FROM " || relname || " main, " ||
                     "(SELECT id FROM pg_temp.dist_table " ||
                            "ORDER BY dist DESC LIMIT 1) largest " ||
         "WHERE main." || att_pk || " = largest." || att_pk || ") r2 " ||
         "WHERE r1." || att_pk || " = d." || att_pk
  sql3a <- "DROP TABLE pg_temp.dist_table"
  sql3b <- "ALTER TABLE pg_temp.dist_table_new RENAME TO dist_table"

  print("Repeat SQL is: " || sql3)

  for (i in 1:nloops)
  {
    print(as.character(i) || "th repeat")
    dbGetQuery(conn, sql3)
    dbGetQuery(conn, sql3a)
    dbGetQuery(conn, sql3b)
  }

  # Final: obtain the final result from the dist_table
  # --------------------------------------------------
  sql4 <- "SELECT * FROM pg_temp.dist_table"
  res <- dbGetQuery(conn, sql4)  

  # clean up the database session
  dbDisconnect(conn)

  return(res)
}

pgsql_minmax <- function(relname, att_pk, att_val, nloops)
{
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname)
  e <- try(pgsql_minmax_tryblock(conn, relname, att_pk, att_val, nloops))
  # clean up database session on error
  dbDisconnect(conn)
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
