/*
 * bitcount.c 
 *    Several utility functions to count number of bits in bit(*) data.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/varbit.h"

/* declarations */
Datum popc(PG_FUNCTION_ARGS);
Datum popc_and(PG_FUNCTION_ARGS);
Datum popc_or(PG_FUNCTION_ARGS);
Datum popc_xor(PG_FUNCTION_ARGS);

PG_MODULE_MAGIC;

static int	bitcount_map[] =
{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

Datum
popc(PG_FUNCTION_ARGS)
{
	VarBit	   *arg = PG_GETARG_VARBIT_P(0);
	bits8	   *ptr = VARBITS(arg);
	int32		count = 0;
	int			offset = 0;
	int			bitlen;

#ifdef __GNUC__
	bitlen = LONGALIGN_DOWN(VARBITBYTES(arg));

	for (offset=0; offset < bitlen; offset += sizeof(long))
		count +=  __builtin_popcountl(*((long *)(ptr + offset)));
#endif
	bitlen = VARBITBYTES(arg);
	while (offset < bitlen)
		count += bitcount_map[ptr[offset++]];

	PG_RETURN_INT32(count);
}
PG_FUNCTION_INFO_V1(popc);

Datum
popc_and(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	VarBit	   *arg2 = PG_GETARG_VARBIT_P(1);
	bits8	   *ptr1 = VARBITS(arg1);
	bits8	   *ptr2 = VARBITS(arg2);
	int32		count = 0;
	int			offset = 0;
	int			bitlen;

	if (VARBITLEN(arg1) != VARBITLEN(arg2))
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("cannot AND bit strings of different sizes")));
#ifdef __GNUC__
	bitlen = LONGALIGN_DOWN(VARBITBYTES(arg1));

	for (offset=0; offset < bitlen; offset += sizeof(long))
		count += __builtin_popcountl(*((long *)(ptr1 + offset)) &
									 *((long *)(ptr2 + offset)));
#endif
	bitlen = VARBITBYTES(arg1);
	while (offset < bitlen)
	{
		count += bitcount_map[ptr1[offset] & ptr2[offset]];
		offset++;
	}
	PG_RETURN_INT32(count);
}
PG_FUNCTION_INFO_V1(popc_and);

Datum
popc_or(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	VarBit	   *arg2 = PG_GETARG_VARBIT_P(1);
	bits8	   *ptr1 = VARBITS(arg1);
	bits8	   *ptr2 = VARBITS(arg2);
	int32		count = 0;
	int			offset = 0;
	int			bitlen;

	if (VARBITLEN(arg1) != VARBITLEN(arg2))
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("cannot AND bit strings of different sizes")));
#ifdef __GNUC__
	bitlen = LONGALIGN_DOWN(VARBITBYTES(arg1));

	for (offset=0; offset < bitlen; offset += sizeof(long))
		count += __builtin_popcountl(*((long *)(ptr1 + offset)) |
									 *((long *)(ptr2 + offset)));
#endif
	bitlen = VARBITBYTES(arg1);
	while (offset < bitlen)
	{
		count += bitcount_map[ptr1[offset] & ptr2[offset]];
		offset++;
	}
	PG_RETURN_INT32(count);
}
PG_FUNCTION_INFO_V1(popc_or);

Datum
popc_xor(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	VarBit	   *arg2 = PG_GETARG_VARBIT_P(1);
	bits8	   *ptr1 = VARBITS(arg1);
	bits8	   *ptr2 = VARBITS(arg2);
	int32		count = 0;
	int			offset = 0;
	int			bitlen;

	if (VARBITLEN(arg1) != VARBITLEN(arg2))
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
				 errmsg("cannot AND bit strings of different sizes")));
#ifdef __GNUC__
	bitlen = LONGALIGN_DOWN(VARBITBYTES(arg1));

	for (offset=0; offset < bitlen; offset += sizeof(long))
		count += __builtin_popcountl(*((long *)(ptr1 + offset)) ^
									 *((long *)(ptr2 + offset)));
#endif
	bitlen = VARBITBYTES(arg1);
	while (offset < bitlen)
	{
		count += bitcount_map[ptr1[offset] & ptr2[offset]];
		offset++;
	}
	PG_RETURN_INT32(count);
}
PG_FUNCTION_INFO_V1(popc_xor);
