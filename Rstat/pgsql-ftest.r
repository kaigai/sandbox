require("RPostgreSQL")

#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "mobile"
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

#
# pgsql_ftest - run F-test using in-place computing manner
#
#
#
#
pgsql_ftest <- function(alpha)
{
  tv1 <- Sys.time()

  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port)
  res <- dbGetQuery(conn, "SET pg_strom.enabled=off")
  sql <- "SELECT model, count(*), variance(x+y+z) var
            FROM phones_accelerometer
           GROUP BY model
          HAVING model='nexus4' or model='s3mini'"
  res <- dbGetQuery(conn, sql)
  dbDisconnect(conn)

  elapsed_sec <- as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("query: %.3f sec", elapsed_sec, quote=FALSE))

  l_bound <- qf(alpha/2, res[1,'count']-1, res[2,'count']-1)
  u_bound <- qf(1-alpha/2, res[1,'count']-1, res[2,'count']-1)

  fv <- res[1, 'var'] / res[2, 'var']

  return (c(l_bound, fv, u_bound))

  print("F-test: " || l_bound || " ... " || fv || " ... " || u_bound, quote=FALSE)
  return (l_bound <= fv && fv <= u_bound)
}

#
# pgsql_ftest_bycpu - run F-test on the data pull from PostgreSQL
#
#
#
#
pgsql_ftest_bycpu <- function(alpha)
{
  tv1 <- Sys.time()
  # Open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port)
  sql <- "SELECT model, x, y, z
            FROM phones_accelerometer"
  res <- dbGetQuery(conn, sql)
  dbDisconnect(conn)

  tv2 <- Sys.time()
  elapsed_sec <- as.double(difftime(tv2, tv1, units="secs"))
  print(sprintf("query: %.3f sec", elapsed_sec, quote=FALSE))

  s3mini <- res[res[['model']] == 's3mini',]
  nexus4 <- res[res[['model']] == 'nexus4',]
  result <- var.test(s3mini[['x']]+s3mini[['y']]+s3mini[['z']],
                     nexus4[['x']]+nexus4[['y']]+nexus4[['z']])

  tv3 <- Sys.time()
  elapsed_sec <- as.double(difftime(tv3, tv2, units="secs"))
  print(sprintf("calculation: %.3f sec", elapsed_sec, quote=FALSE))

  return(result)
}
