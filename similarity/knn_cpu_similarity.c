#include "postgres.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/array.h"

Datum	knn_cpu_similarity(PG_FUNCTION_ARGS);

PG_MODULE_MAGIC;

#define VALIDATE_ARRAY_MATRIX(matrix,type_oid)	\
	(VARATT_IS_4B(matrix) &&					\
	 ARR_ELEMTYPE(matrix) == (type_oid) &&		\
	 !ARR_HASNULL(matrix) &&					\
	 ARR_NDIM(matrix) == 2 &&					\
	 ARR_DIMS(matrix)[0] > 0 &&					\
	 ARR_DIMS(matrix)[1] > 0 &&					\
	 ARR_LBOUND(matrix)[0] == 1 &&				\
	 ARR_LBOUND(matrix)[1] == 1)

#define ARRAY_MATRIX_HEIGHT(matrix)					\
	(ARR_NDIM(matrix) == 2 ? ARR_DIMS(matrix)[1] :	\
	 ARR_NDIM(matrix) == 1 ? ARR_DIMS(matrix)[0] : -1)
#define ARRAY_MATRIX_WIDTH(matrix)					\
	(ARR_NDIM(matrix) == 2 ? ARR_DIMS(matrix)[0] :	\
	 ARR_NDIM(matrix) == 1 ? 1 : -1)

/*
 * knn_cpu_similarity_sanitycheck - sanity check of the arguments
 */
static void
knn_cpu_similarity_sanitycheck(int32 kval, ArrayType *Q, ArrayType *D)
{
	if (!VALIDATE_ARRAY_MATRIX(Q, INT4OID))
		elog(ERROR, "Q-Matrix is not a valid array-matrix");
	if (!VALIDATE_ARRAY_MATRIX(D, INT4OID))
		elog(ERROR, "D-Matrix is not a valid array-matrix");
	if (ARRAY_MATRIX_WIDTH(Q) < 2 ||
		ARRAY_MATRIX_WIDTH(D) < 2 ||
		ARRAY_MATRIX_WIDTH(Q) != ARRAY_MATRIX_WIDTH(D))
		elog(ERROR, "Width of array-matrix mismatch");
	if (kval > ARRAY_MATRIX_HEIGHT(Q))
		elog(ERROR, "k-value must be smaller than height of Q-Matrix");
}

/*
 * knn_cpu_similarity_calculation - calculate a similarity score
 */
static double
knn_cpu_similarity_calculation(ArrayType *Q, int q_index, int q_nitems,
							   ArrayType *D, int d_index, int d_nitems)
{
	int	   *q_data = (int *)ARR_DATA_PTR(Q);
	int	   *d_data = (int *)ARR_DATA_PTR(D);
	int		i, width = ARRAY_MATRIX_WIDTH(Q);
	int		sum_and = 0;
	int		sum_or = 0;

	q_data += q_nitems + q_index;
	d_data += d_nitems + d_index;

	for (i=1; i < width; i++)
	{
		int		q_bits = *q_data;
		int		d_bits = *d_data;

		sum_and += __builtin_popcount(q_bits & d_bits);
		sum_or  += __builtin_popcount(q_bits | d_bits);

		q_data += q_nitems;
		d_data += d_nitems;
	}

	if (sum_or == 0)
		return 0.0;
	return (double)sum_and / (double)sum_or;
}

/*
 * knn_cpu_similarity_compare - callback of qsort
 */
static int
knn_cpu_similarity_compare(const void *__a, const void *__b)
{
	double	a = *((double *)__a);
	double	b = *((double *)__b);

	if (a < b)
		return 1;
	else if (a > b)
		return -1;
	return 0;
}

/*
 * knn_cpu_similarity
 *
 * CPU version of knn_gpu_similarity
 *
 * MatrixArray(float)  -- ID(int32) + similarity(fp32)
 * knn_cpu_similarity(int k,
 *                    int[],  -- ID+bitmap of query chemical components
 *                    int[])  -- ID+bitmap of database chemical components
 */
Datum
knn_cpu_similarity(PG_FUNCTION_ARGS)
{
	int32		kval = PG_GETARG_INT32(0);
	ArrayType  *D = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType  *Q = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *R;
	double	   *scores;
	double		similarity;
	int			i, d_nitems = ARRAY_MATRIX_HEIGHT(D);
	int			j, q_nitems = ARRAY_MATRIX_HEIGHT(Q);
	Size		nbytes;

	knn_cpu_similarity_sanitycheck(kval, Q, D);
	scores = palloc(sizeof(double) * d_nitems);
	nbytes = ARR_OVERHEAD_NONULLS(2) + sizeof(float) * 2 * d_nitems;
	R = palloc(nbytes);
	SET_VARSIZE(R, nbytes);
	R->ndim = 2;
	R->dataoffset = 0;
    R->elemtype = FLOAT4OID;
	ARR_DIMS(R)[0] = 2;
	ARR_DIMS(R)[1] = d_nitems;
	ARR_LBOUND(R)[0] = 1;
	ARR_LBOUND(R)[1] = 1;

	for (i=0; i < d_nitems; i++)
	{
		CHECK_FOR_INTERRUPTS();

		/* calculation of similarity for each Q items */
		for (j=0; j < q_nitems; j++)
		{
			scores[j] = knn_cpu_similarity_calculation(Q, j, q_nitems,
													   D, i, d_nitems);
		}
		/* sort the results */
		qsort(scores, q_nitems, sizeof(double),
			  knn_cpu_similarity_compare);

		/* make an average by the top-k items */
		similarity = 0.0;
		for (j=0; j < kval; j++)
			similarity += scores[j];
		similarity /= (double)kval;

		/* store the result */
		((int *)ARR_DATA_PTR(R))[i] = ((int *)ARR_DATA_PTR(D))[i];
		((float *)ARR_DATA_PTR(R))[d_nitems + i] = similarity;
	}
	PG_RETURN_ARRAYTYPE_P(R);
}
PG_FUNCTION_INFO_V1(knn_cpu_similarity);
