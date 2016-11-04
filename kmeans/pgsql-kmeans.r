require("RPostgreSQL")
#
# PostgreSQL connection parameters
#
PgParam_hostname <- "localhost"
PgParam_dbname <- "postgres"
PgParam_port <- 5432
PgParam_query <- "SELECT cluster.*, c_test.x, c_test.y
 FROM matrix_unnest(
          (SELECT gpu_kmeans(array_matrix(int4_as_float4(id),x,y),
                             5,
                             20)
             FROM c_test WHERE id < 1000))
      AS cluster(id int, clustr int),
      c_test
WHERE cluster.id = c_test.id"

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

pgsql_gpu_kmeans <- function()
{
  tv1 <- Sys.time()
  # open the database connection
  conn <- dbConnect(PostgreSQL(),
                    host=PgParam_hostname,
                    dbname=PgParam_dbname,
                    port=PgParam_port)
  # send the query
  res <- try(dbGetQuery(conn, PgParam_query))
  dbDisconnect(conn)
  if (class(res) == "try-error")
  {
    return(NULL)
  }
  elapsed = as.double(difftime(Sys.time(), tv1, units="secs"))
  print(sprintf("Query total: %.3f sec", elapsed, quote=FALSE))

  return(res)
}
