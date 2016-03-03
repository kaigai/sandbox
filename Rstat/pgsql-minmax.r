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

#
# pgsql_minmax - returns id and distance of the items
#
# arguments:
# relname - name of the target table (string)
# att_pk  - name of the primary key column (string)
# att_val - vector of the property columns (string[])
# nloops  - number of iteration (integer)
#
pgsql_minmax <- function(relname, att_pk, att_val, nloops)
{
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname)
  tryCatch({
    # Init: construction of 'subset_table'
    # ------------------------------------
    sql1 <- "SELECT * INTO pg_temp.subset_table FROM " || relname ||
            " ORDER BY random() LIMIT 1"
    ## dbGetQuery(conn, sql1)
    sql1a <- "VACUUM ANALYZE pg_temp.subset_table"
    ## dbGetQuery(conn, sql1a)

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
            "FROM pg_temp.subset_table s, " || relname || " r"
    ## dbGetQuery(conn, sql2)

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

    for (i in 1:nloops)
    {
      print(as.character(i) || "th repeat")
      ## dbGetQuery(conn, sql3)
      ## dbGetQuery(conn, sql3a)
      ## dbGetQuery(conn, sql3b)
    }

    # Final: obtain the final result from the dist_table
    # --------------------------------------------------
    sql4 <- "SELECT * FROM pg_temp.dist_table"
    res <- dbGetQuery(conn, sql4)  

    # clean up the database session
    dbDisconnect(conn)

    return(res)  
  },
  finnaly = {
    # clean up the database session
    dbDisconnect(conn)
  })
}
