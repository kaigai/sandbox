require("RPostgreSQL")
#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "postgres"
PgParam_port <- 5432
PgParam_username <- "kaigai"

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

pgsql_gpu_kmeans <- function(nitems, k_value, nloops)
{
  tv1 <- Sys.time()
  # open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port,
                    user=PgParam_username)
  # build SQL statement
  sql <- sprintf("SELECT cluster.*, c_test.x, c_test.y
                    FROM matrix_unnest(
                 (SELECT gpu_kmeans(array_matrix(int4_as_float4(id),x,y),
                                   %d,
                                   %d)
                    FROM c_test WHERE id <= %d))
                      AS cluster(id int, cluster int),
                 c_test
                   WHERE cluster.id = c_test.id",
          		   k_value,
                   nloops,
                   nitems);
  # send the query
  res <- try(dbGetQuery(conn, sql))
  dbDisconnect(conn)
  if (class(res) == "try-error")
  {
    return(NULL)
  }
  elapsed = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("Query total: %.3f sec", elapsed, quote=FALSE))

  return(res)
}

pgsql_cpu_kmeans <- function(nitems, k_value, nloops)
{
  tv1 <- Sys.time()
  # open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port,
                    user=PgParam_username)
  # build SQL statement
  sql <- sprintf("SELECT * FROM c_test WHERE id <= %d", nitems);
  # send the query
  data <- try(dbGetQuery(conn, sql))
  dbDisconnect(conn)
  if (class(data) == "try-error")
  {
    return(NULL);
  }
  tv2 <- Sys.time()

  # do local clustering
  res = kmeans(cbind(data$x, data$y), k_value, iter.max=nloops)

  elapsed1 = as.double(difftime(tv2, tv1, units="secs"))
  elapsed2 = as.double(difftime(Sys.time(), tv2, units="secs"))
  print(sprintf("DB Dump: %.3f sec, kmeans: %.3f sec",
                elapsed1, elapsed2, quote=FALSE))
  return(res)
}

hoge <- pgsql_gpu_kmeans(1000, 5, 10)
plot(x=hoge$x, y=hoge$y, col=hoge$cluster+1)

