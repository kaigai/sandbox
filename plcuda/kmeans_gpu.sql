-- ================================================================
-- helper function for sanity check of the arguments
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans__sanity_check(int[], float[], int, int)
RETURNS bool
AS $$
  SELECT array_matrix_validation($1) AND
         array_matrix_validation($2) AND
         array_matrix_width($1) = 1  AND
         array_matrix_height($1) > 0 AND
         array_matrix_height($2) > 0 AND
         array_matrix_height($1) = array_matrix_height($2) AND
         $3 > 0 AND     -- k-value > 0
         $4 > 0;        -- nloops > 0
$$ LANGUAGE 'sql';

-- ================================================================
-- helper function for working buffer estimation
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans__working_bufsz(int[], float[], int, int)
RETURNS bigint
AS $$
  -- C: kxm matrix to hold centroid
  SELECT array_matrix_rawsize('float'::regtype,
                              k, array_matrix_width($2))::bitgint;
$$ LANGUAGE 'sql';

-- ================================================================
-- helper function for result buffer estimation
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans__results_bufsz(int[], float[], int, int)
RETURNS bigint
AS $$
  -- R: Nx2 matrix to hold a pair of IDs
  SELECT array_matrix_rawsize('int'::regtype,
                              array_matrix_height($3), 2)::bigint;
$$ LANGUAGE 'sql';

-- ================================================================
--
-- Main part of the GPU version of k-means clustering
--
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans(int[],	-- Nx1 ID vector
           float[],     -- Nxm characteristics matrix
           int,         -- number of the cluster
           int = 20)    -- number of the iteration
RETURNS int[]           -- Nx2 (ID, Cluster) matrix
AS $$
#plcuda_decl
/*
 * assign_initial_cluster
 *
 * It distribute random cluster assignment on all the items.
 * TODO: We can improve the initial cluster using k-means+ method
 */
KERNEL_FUNCTION(void)
assign_initial_cluster(kern_arg_t __R,
                       kern_arg_t k_val,
                       kern_arg_t nloops)
{
  MatrixType *R = (MatrixType *)__R;
  cl_uint  n = ARRAY_MATRIX_HEIGHT(R);
  cl_int  *data = (cl_int *)ARRAY_MATRIX_DATAPTR(R);

  if ((nloops & 0x01) == 1)
    data += n;
  /* pseudo-random */
  if (get_global_id() < n)
    data[get_global_id()] = get_global_id() % k_val;
}

/*
 * update_centroid_matrix
 *
 * update the C matrix according to the current cluster assignment of
 * the items.
 */
KERNEL_FUNCTION(void)
update_centroid_matrix(kern_arg_t __D,
                       kern_arg_t __C,
                       kern_arg_t __R,
                       kern_arg_t k_value,
                       kern_arg_t nloops)
{
  MatrixType *D = (MatrixType *)__D;
  MatrixType *C = (MatrixType *)__C;
  MatrixType *R = (MatrixType *)__R;
  cl_uint     local_base;
  cl_uint     local_id;
  cl_uint     i, j;

  for (local_base = 0;
       local_base < k_value;
       local_base += get_local_size())
  {
    local_id = local_base + get_local_id();

    if (k_value < warpSize)
    {
      cl_int npacked = warpSize / k_value;
      cl_int nrows_per_block = (get_local_size() / warpSize) * npacked;



    }
    else
    {


    }











  }






}

KERNEL_FUNCTION(void)
update_cluster_assignment()
{}

KERNEL_FUNCTION(void)
move_data_ids()
{}

#plcuda_main
MatrixType *ID = (MatrixType *) arg1.value;
MatrixType *D  = (MatrixType *) arg2.value;
MatrixType *C  = (MatrixType *) workbuf;
MatrixType *R  = (Matrixtype *) results;
cl_int      n  = ARRAY_MATRIX_HEIGHT(D);
cl_int      m  = ARRAY_MATRIX_WIDTH(D);
cl_int      k_value = arg3.value;
cl_int      nloops = arg4.value;
cl_int      device;
cl_int      devMaxThreadsPerBlock;
dim3        grid_sz;
dim3        block_sz;
cl_int      unitsz;
cudaError_t status;

/*
 * construction of array matrix on working/results buffer
 */
INIT_ARRAY_MATRIX(C, PG_FLOAT4OID, sizeof(cl_float), k, m);
INIT_ARRAY_MATRIX(R, PG_INT4OID, sizeof(cl_int), n, 2);
retval->isnull = false;
retval->value  = (varlena *) R;

/* Get device property (maxThreadsPerBlock) */
/* Then, calculate a proper unit-size for this k-value */
status = cudaGetDevice(&device);
if (status != cudaSuccess)
  PLCUDA_RUNTIME_ERROR_RETURN(status);
status = cudaDeviceGetAttribute(&devMaxThreadsPerBlock,
                                cudaDevAttrMaxThreadsPerBlock,
                                device);
if (status != cudaSuccess)
  PLCUDA_RUNTIME_ERROR_RETURN(status);
unitsz = Min(k_value, devMaxThreadsPerBlock);

/*
 * 1. assignment of the initial cluster
 */
status = pgstromLaunchDynamicKernel3((void *)assign_initial_cluster,
                                     (kern_arg_t)R,
                                     (kern_arg_t)k_val,
                                     (kern_arg_t)nloops,
                                     n, 0, 0);
if (status != StromError_Success)
  PLCUDA_RUNTIME_ERROR_RETURN(status);

while (nloops > 0)
{
  /*
   * 2. update of the centroid matrix; according to the current cluster
   */


  /*
   * 3. update cluster assignment; according to the distance of nearest
   *    centroid.
   */  


  nloops--;
}

/*
 * 4. store the pair of data-id and cluster assignment
 */




#plcuda_end
#plcuda_sanity_check    gpu_kmeans__sanity_check
#plcuda_working_bufsz   gpu_kmeans__working_bufsz
#plcuda_results_bufsz   gpu_kmeans__results_bufsz
$$ LANGUAGE 'plcuda';
