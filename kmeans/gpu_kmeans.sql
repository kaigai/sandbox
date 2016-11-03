--
-- GPU version of k-means clustering
--


-- ================================================================
--
-- gpu_kmeans_sanity_check
--
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans_sanity_check(real[],
                        int,
                        int,
                        int)
RETURNS bool
AS $$
  SELECT array_matrix_validation($1)           -- D-matrix validation
     AND $2 > 1                                -- multiple clusters
     AND array_matrix_width($1) >= 2           -- ID + 1-attribute at least
     AND array_matrix_width($1) * $2 <= 10240  -- shared memory consumption
$$ LANGUAGE 'sql';

-- ================================================================
--
-- gpu_kmeans_working_bufsz
--
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans_working_bufsz(real[],
                         int,
                         int,
                         int)
RETURNS bigint
AS $$
  -- size of C-matrix (centroid)
  SELECT (4 * array_matrix_width($1) * $2)::bigint;
$$ LANGUAGE 'sql';

-- ================================================================
--
-- gpu_kmeans_results_bufsz
--
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans_results_bufsz(real[],
                         int,
                         int,
                         int)
RETURNS bigint
AS $$
  -- size of R-matrix (ID + cluster-Nr)
  SELECT 4 * 2 * array_matrix_height($1)::bigint;
$$ LANGUAGE 'sql';

-- ================================================================
--
-- main part of GPU k-means
--
-- ================================================================
CREATE OR REPLACE FUNCTION
gpu_kmeans(real[],    -- ID + Data Matrix
           int,       -- k-value (number of clusters)
           int = 10,  -- max number of iteration
           int = 1)   -- seed of initial randomness
RETURNS int[]
AS $$
#plcuda_decl
KERNEL_FUNCTION(void)
setup_initial_cluster(MatrixType *D,
                      MatrixType *R,
                      kern_arg_t k_value,
                      kern_arg_t r_seed)
{
    cl_int     *d_data = (cl_int *)ARRAY_MATRIX_DATAPTR(D);
    cl_int	   *r_data = (cl_int *)ARRAY_MATRIX_DATAPTR(R);
    cl_uint     n = ARRAY_MATRIX_HEIGHT(R);
    cl_uint		a, b, c;

    if (get_global_id() < n)
    {
        /* same logic of hash_uint32() */
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

        a = b = c = 0x9e3779b9 + (cl_uint) sizeof(uint32) + 3923095;
        a += (cl_uint)r_seed + get_global_id();
        /* final(a,b,c) */
        c ^= b; c -= rot(b,14);
        a ^= c; a -= rot(c,11);
        b ^= a; b -= rot(a,25);
        c ^= b; c -= rot(b,16);
        a ^= c; a -= rot(c, 4);
        b ^= a; b -= rot(a,14);
        c ^= b; c -= rot(b,24);
#undef rot

        r_data[get_global_id()] = d_data[get_global_id()];
        r_data[n + get_global_id()] = (c % k_value);
    }
}

KERNEL_FUNCTION(void)
clear_centroid(MatrixType *C)
{
    cl_float   *g_cent = (cl_float *)ARRAY_MATRIX_DATAPTR(C);
    cl_uint     i, n = (ARRAY_MATRIX_HEIGHT(C) *
                        ARRAY_MATRIX_WIDTH(C));

    /* clear the global centroid */
    for (i = get_global_id(); i < n; i += get_global_size())
        g_cent[i] = 0.0;
}

KERNEL_FUNCTION_MAXTHREADS(void)
update_centroid(MatrixType *D,
                MatrixType *R,
                MatrixType *C,
                kern_arg_t k_value)
{
    cl_float   *d_values = (cl_float *)ARRAY_MATRIX_DATAPTR(D);
    cl_int	   *r_values = (cl_int *)ARRAY_MATRIX_DATAPTR(R);
    cl_float   *c_values = (cl_float *)ARRAY_MATRIX_DATAPTR(C);
    cl_uint     nitems = ARRAY_MATRIX_HEIGHT(D);
    cl_uint     width = ARRAY_MATRIX_WIDTH(D);
    cl_float   *l_cent = SHARED_WORKMEM(cl_float);
    cl_uint     index;
    cl_int		i, j;

    /* init shared memory */
    for (index = get_local_id();
         index < width * k_value;
         index += get_local_size())
        l_cent[index] = 0.0;

    /* accumulate the local centroid */
    for (index = get_global_id();
         index < nitems;
         index += get_global_size())
    {
        /* pick up the target cluster (2nd column of R) */
        i = r_values[nitems + index];
        assert(i < k_value);

        /* increment the number of items */
        atomicAdd(&l_cent[i], 1.0);
        /* accumlate each attributes */
        for (j=1; j < width; j++)
            atomicAdd(&l_cent[j * k_value + i],
                      d_values[j * nitems + index]);
    }
    __sync_threads();

    /* write back to the global C-matrix */
    for (index = get_local_id();
         index < width * k_value;
         index += get_local_size())
        atomicAdd(&c_values[index], l_cent[index]);
}

KERNEL_FUNCTION_MAXTHREADS(void)
update_centroid_final(MatrixType *C)
{
    cl_uint		k_value = ARRAY_MATRIX_HEIGHT(C);
    cl_uint		width = ARRAY_MATRIX_WIDTH(C);
    cl_float   *c_values = (cl_float *)ARRAY_MATRIX_DATAPTR(C);

    if (get_global_id() < k_value * (width - 1))
    {
        cl_uint     i = get_global_id() % k_value;
        cl_float    count = c_values[i];

        if (count > 0.0)
            c_values[k_value + get_global_id()] /= count;
        else
            c_values[k_value + get_global_id()] = 0.0;
    }
}

KERNEL_FUNCTION(void)
kmeans_update_cluster(MatrixType *D,
                      MatrixType *R,
                      MatrixType *C
                      kern_arg_t k_value)
{
    cl_float   *dist_map = SHARED_WORKMEM(cl_float);
    cl_uint		nitems = ARRAY_MATRIX_HEIGHT(D);
    cl_uint		width = ARRAY_MATRIX_WIDTH(D);
    cl_uint		k_value = ARRAY_MATRIX_HEIGHT(C);
    cl_uint		did;
    cl_uint		cid;
    cl_uint		i;
    cl_float   *d_values = (cl_float *)ARRAY_MATRIX_DATAPTR(D);
    cl_float   *c_values = (cl_float *)ARRAY_MATRIX_DATAPTR(C);
    cl_int	   *r_values = (cl_int *)ARRAY_MATRIX_DATAPTR(R);
    cl_float	dist = 0.0;

    assert(width == ARRAY_MATRIX_WIDTH(C));
    assert(nitems == ARRAY_MATRIX_HEIGHT(R));

    did = get_global_id() / k_value;	/* index of D */
    cid = get_global_id() % k_value;	/* index of C */

    /* make distance from the target centroid */
    for (i=1, d_values += nitems, c_values += k_value;
         i < width;
         i++, d_values += nitems, c_values += k_value)
    {
        cl_float	x = d_values[did] - c_values[cid];

        dist += x * x;
    }
    dist_map[get_local_id()] = dist;
    __sync_threads();

    /* pick up the nearest one */
    for (loop)
    {




    }

    /* write back to the R-matrix */
    if (cid == 0)
        r_values[nitems + did] = cluster_index;
}

#plcuda_prep
MatrixType *D = (MatrixType *) arg1.value;
MatrixType *C = (MatrixType *) workbuf;
MatrixType *R = (MatrixType *) results;
cl_uint     k = arg2.value;
cl_uint     nitems = ARRAY_MATRIX_HEIGHT(D);
cl_uint     width = ARRAY_MATRIX_WIDTH(D);

INIT_ARRAY_MATRIX(C, PG_FLOAT4OID, sizeof(cl_float), k, width);
INIT_ARRAY_MATRIX(R, PG_FLOAT4OID, sizeof(cl_float), nitems, 2);
retval->isnull = false;
retval->value  = (varlena *) R;

#plcuda_main
MatrixType *D = (MatrixType *) arg1.value;
MatrixType *C = (MatrixType *) workbuf;
MatrixType *R = (MatrixType *) results;
cl_uint     nitems = ARRAY_MATRIX_HEIGHT(D);
cl_uint     width = ARRAY_MATRIX_WIDTH(D) - 1;
cl_int      k_value = arg2.value;
cl_int      loop, nloops = arg3.value;
cl_int      r_seed = arg4.value;
cl_int      device;
cl_int      sm_count;
cudaError_t status;

/*
 * Get device/function attributes
 */
status = cudaGetDevice(&device);
if (status != cudaSuccess)
    PLCUDA_RUNTIME_ERROR_RETURN(status);
status = cudaDeviceGetAttribute(&sm_count,
                                cudaDevAttrMultiProcessorCount,
                                device);
if (status != cudaSuccess)
    PLCUDA_RUNTIME_ERROR_RETURN(status);

/*
 * setup of R-matrix with random clustring
 *
 * NOTE: k-means+ will give more reasonable initial clustering
 */
status = pgstromLaunchDynamicKernel4((void *)setup_initial_cluster,
                                     (kern_arg_t)(D),
                                     (kern_arg_t)(R),
                                     (kern_arg_t)(k_value),
                                     (kern_arg_t)(r_seed),
                                     nitems, 0, 0);
if (status != cudaSuccess)
    PLCUDA_RUNTIME_ERROR_RETURN(status);

for (loop=0; loop < nloops; loop++)
{
    size_t      n_threads;
    cl_uint     required;

    /*
     * update of the C-matrix based on the current clustering
     */
    status = pgstromLaunchDynamicKernel1((void *)clear_centroid,
                                         (kern_arg_t)(C),
                                         k_value * width,
                                         0, 0);
    if (status != cudaSuccess)
        PLCUDA_RUNTIME_ERROR_RETURN(status);

    n_threads = Min(nitems, sm_count * 1024);
    required = sizeof(cl_uint) * width * k_value;
    status = pgstromLaunchDynamicKernelMaxThreads4((void *)update_centroid,
                                                   (kern_arg_t)(D),
                                                   (kern_arg_t)(R),
                                                   (kern_arg_t)(C),
                                                   (kern_arg_t)(k_value),
                                                   n_threads,
                                                   required,
                                                   0);
    if (status != cudaSuccess)
        PLCUDA_RUNTIME_ERROR_RETURN(status);

    status = pgstromLaunchDynamicKernel1((void *)update_centroid_final,
                                         (kern_arg_t)(C),
                                         k_value * (width - 1),
                                         0, 0);
    if (status != cudaSuccess)
        PLCUDA_RUNTIME_ERROR_RETURN(status);
    /*
     * calculation of the distance from the current centroid, then picks
     * up the nearest one and update R-matrix
     */
    status = pgstromLaunchDynamicKernelMaxThreads4();
    if (status != cudaSuccess)
        PLCUDA_RUNTIME_ERROR_RETURN(status);
}

#plcuda_sanity_check	gpu_kmeans_sanity_check
#plcuda_working_bufsz	gpu_kmeans_working_bufsz
#plcuda_results_bufsz	gpu_kmeans_results_bufsz

#plcuda_end
$$ LANGUAGE 'plcuda';
