/*--------------------------------------------------------------------------
 *
 * test_pglz.c
 *		Test pglz performance.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/test_pglz.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "miscadmin.h"

#include <limits.h>
#include <time.h>

#include "common/pg_lzcompress.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pglz);

typedef int32 (*decompress_func)(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);

typedef int32 (*compress_func)(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);


int32
pglz_compress_vanilla(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);

int32
pglz_compress_hacked(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
int32
pglz_decompress_hacked_unrolled(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
int32
pglz_decompress_vanilla(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);
int32
pglz_decompress_hacked(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);

int32
pglz_decompress_hacked8(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);

int32
pglz_decompress_hacked16(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);


double do_test(int compressor, int decompressor, int payload, bool decompression_time);
double do_sliced_test(int compressor, int decompressor, int payload, int slice_size, bool decompression_time);

compress_func compressors[] = {pglz_compress_vanilla, pglz_compress_hacked};
char *compressor_name[] = {"pglz_compress_vanilla", "pglz_compress_hacked"};
int compressors_count = 2;

decompress_func decompressors[] =
{
	pglz_decompress_vanilla,
	pglz_decompress_hacked,
	pglz_decompress_hacked_unrolled,
	pglz_decompress_hacked8,
	pglz_decompress_hacked16,
	pglz_decompress_vanilla,
};
char *decompressor_name[] = 
{
	"pglz_decompress_vanilla - warmup", /* do vanilla test at the beginning and at the end */
	"pglz_decompress_hacked",
	"pglz_decompress_hacked_unrolled",
	"pglz_decompress_hacked8",
	"pglz_decompress_hacked16",
	"pglz_decompress_vanilla",
};

int decompressors_count = 6;

char *payload_names[] =
{
	//"adversary_rnd",
	"000000010000000000000001",
	"000000010000000000000006",
	"000000010000000000000008",
	"16398",
	"shakespeare.txt",
	//"adversary5",
	//"adversary7",
};
void **payloads;
long *payload_sizes;
int payload_count = 5;

/* benchmark returns ns per byte of payload to decompress */
double do_test(int compressor, int decompressor, int payload, bool decompression_time)
{
	ereport(LOG,
		(errmsg("Testing payload %s\tcompressor %s\tdecompressor %s",
			payload_names[payload], compressor_name[compressor], decompressor_name[decompressor]),
		errhidestmt(true)));
	void *data = payloads[payload];
	long size = payload_sizes[payload];
	void *extracted_data = palloc(size);
	void *compressed = palloc(size * 2);

	compressors[compressor](data, size, compressed, PGLZ_strategy_default);
	clock_t compression_begin = clock();
	int comp_size = compressors[compressor](data, size, compressed, PGLZ_strategy_default);
	clock_t copmression_end = clock();

	clock_t decompression_begin = clock();
	if (decompressors[decompressor](compressed, comp_size, extracted_data, size, true) != size)
		elog(ERROR, "decompressed wrong size %d instead of %d",decompressors[decompressor](compressed, comp_size, extracted_data, size, true),size);
	clock_t decopmression_end = clock();

	if (memcmp(extracted_data, data, size))
		elog(ERROR, "decompressed different data");
	
	ereport(LOG,
		(errmsg("Compression %ld\t(%f seconds)\tDecompression %ld\t(%f seconds)\tRatio %f",
			copmression_end - compression_begin, ((float)copmression_end - compression_begin) / CLOCKS_PER_SEC,
			decopmression_end - decompression_begin, ((float)decopmression_end - decompression_begin) / CLOCKS_PER_SEC,
			comp_size/(float)size),
		errhidestmt(true)));

	pfree(extracted_data);
	pfree(compressed);

	if (decompression_time)
		return ((double)decopmression_end - decompression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
	else
		return ((double)copmression_end - compression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
}

double do_sliced_test(int compressor, int decompressor, int payload, int slice_size, bool decompression_time)
{
	ereport(LOG,
		(errmsg("Testing %dKb slicing payload %s\tcompressor %s\tdecompressor %s", slice_size / 1024,
			payload_names[payload], compressor_name[compressor], decompressor_name[decompressor]),
		errhidestmt(true)));
	char *data = payloads[payload];
	long size = payload_sizes[payload];
	int slice_count = size / slice_size;
	void **extracted_data = palloc(slice_count * sizeof(void*));
	void **compressed = palloc(slice_count * sizeof(void*));
	int *comp_size = palloc(slice_count * sizeof(int));
	int i;
	for (i = 0; i < slice_count; i++)
	{
		extracted_data[i] = palloc(slice_size * 2);
		compressed[i] = palloc(slice_size * 2);
	}

	clock_t compression_begin = clock();
	for (i = 0; i < slice_count; i++)
		comp_size[i] = compressors[compressor](data + slice_size * i, slice_size, compressed[i], PGLZ_strategy_default);
	clock_t compression_end = clock();

	clock_t decompression_begin = clock();
	for (i = 0; i < slice_count; i++)
	{
		if (comp_size[i] == -1)
			continue; /* no decompression */
		int decompressed_size = decompressors[decompressor](compressed[i], comp_size[i], extracted_data[i], slice_size, false);
		if (decompressed_size != slice_size)
			elog(ERROR, "decompressed wrong size %d instead of %d, compressed size %d", decompressed_size, slice_size, comp_size[i]);
	}
	clock_t decopmression_end = clock();

	for (i = 0; i< slice_count; i++)
	{
		pfree(extracted_data[i]);
		pfree(compressed[i]);
	}
	pfree(extracted_data);
	pfree(comp_size);
	pfree(compressed);

	if (decompression_time)
		return ((double)decopmression_end - decompression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
	else
		return ((double)compression_end - compression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
}

static void prepare_payloads()
{
	payloads = palloc(sizeof(void*) * payload_count);
	payload_sizes = palloc(sizeof(long) * payload_count);

	char share_path[MAXPGPATH];
	char path[MAXPGPATH];
	FILE *f;
	long size;
	int i;

	for (i=0; i< payload_count; i++)
	{
		get_share_path(my_exec_path, share_path);
		snprintf(path, MAXPGPATH, "%s/extension/%s", share_path, payload_names[i]);
		f = fopen(path, "r");
		if (!f)
			elog(ERROR, "unable to open payload");

		fseek (f , 0 , SEEK_END);
		size = ftell (f);
		rewind (f);
		
		void* data = palloc(size);

		if (fread(data, size, 1, f) != 1)
			elog(ERROR, "unable to read payload");
		fclose(f);
		payloads[i] = data;
		payload_sizes[i] = size;
	}
}

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_pglz(PG_FUNCTION_ARGS)
{
	double decompression_results[10][10];
	double decompression_sliced_2kb_results[10][10];
	double decompression_sliced_8kb_results[10][10];

	double decompressor_results[10];

	double compression_results[10][10];
	double compression_sliced_2kb_results[10][10];
	double compression_sliced_8kb_results[10][10];

	double compressor_results[10];

	int iterations = 5;
	int iteration;
	int i,p;
	int old_verbosity = Log_error_verbosity;

	prepare_payloads();
	
	for (p = 0; p < payload_count; p++)
		for (i = 0; i < decompressors_count; i++)
		{
			decompressor_results[i] = 0;
			decompression_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				decompression_results[p][i] += do_test(0, i, p, true);
			decompression_results[p][i] /= iterations;

			decompression_sliced_2kb_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				decompression_sliced_2kb_results[p][i] += do_sliced_test(0, i, p, 2048, true);
			decompression_sliced_2kb_results[p][i] /= iterations;
			decompression_results[p][i] /= iterations;

			decompression_sliced_8kb_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				decompression_sliced_8kb_results[p][i] += do_sliced_test(0, i, p, 4096, true);
			decompression_sliced_8kb_results[p][i] /= iterations;
		}

	for (p = 0; p < payload_count; p++)
		for (i = 0; i < compressors_count; i++)
		{
			compressor_results[i] = 0;
			compression_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				compression_results[p][i] += do_test(i, 0, p, false);
			compression_results[p][i] /= iterations;

			compression_sliced_2kb_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				compression_sliced_2kb_results[p][i] += do_sliced_test(i, 0, p, 2048, false);
			compression_sliced_2kb_results[p][i] /= iterations;
			compression_results[p][i] /= iterations;

			compression_sliced_8kb_results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				compression_sliced_8kb_results[p][i] += do_sliced_test(i, 0, p, 4096, false);
			compression_sliced_8kb_results[p][i] /= iterations;
		}

	Log_error_verbosity = PGERROR_TERSE;
	ereport(NOTICE, (errmsg("Time to decompress one byte in ns:"), errhidestmt(true)));
	for (p = 0; p < payload_count; p++)
	{
		ereport(NOTICE, (errmsg("Payload %s", payload_names[p]), errhidestmt(true)));
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Decompressor %s result %f", decompressor_name[i], decompression_results[p][i]), errhidestmt(true)));
			decompressor_results[i] += decompression_results[p][i];
		}

		ereport(NOTICE, (errmsg("Payload %s sliced by 2Kb", payload_names[p]), errhidestmt(true)));
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Decompressor %s result %f", decompressor_name[i], decompression_results[p][i]), errhidestmt(true)));
			decompressor_results[i] += decompression_sliced_2kb_results[p][i];
		}

		ereport(NOTICE, (errmsg("Payload %s sliced by 8Kb", payload_names[p]), errhidestmt(true)));
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Decompressor %s result %f", decompressor_name[i], decompression_sliced_8kb_results[p][i]), errhidestmt(true)));
			decompressor_results[i] += decompression_sliced_8kb_results[p][i];
		}
	}

	ereport(NOTICE, (errmsg("\n\nDecompressor score (summ of all times):"), errhidestmt(true)));
	for (i = 1; i < decompressors_count; i++)
	{
		ereport(NOTICE, (errmsg("Decompressor %s result %f", decompressor_name[i], decompressor_results[i]), errhidestmt(true)));
	}

	Log_error_verbosity = PGERROR_TERSE;
	ereport(NOTICE, (errmsg("Time to compress one byte in ns:"), errhidestmt(true)));
	for (p = 0; p < payload_count; p++)
	{
		ereport(NOTICE, (errmsg("Payload %s", payload_names[p]), errhidestmt(true)));
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Compressor %s result %f", compressor_name[i], compression_results[p][i]), errhidestmt(true)));
			compressor_results[i] += compression_results[p][i];
		}

		ereport(NOTICE, (errmsg("Payload %s sliced by 2Kb", payload_names[p]), errhidestmt(true)));
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Compressor %s result %f", compressor_name[i], compression_results[p][i]), errhidestmt(true)));
			compressor_results[i] += compression_sliced_2kb_results[p][i];
		}

		ereport(NOTICE, (errmsg("Payload %s sliced by 8Kb", payload_names[p]), errhidestmt(true)));
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, (errmsg("Compressor %s result %f", compressor_name[i], compression_sliced_8kb_results[p][i]), errhidestmt(true)));
			compressor_results[i] += compression_sliced_8kb_results[p][i];
		}
	}

	ereport(NOTICE, (errmsg("\n\nCompressor score (summ of all times):"), errhidestmt(true)));
	for (i = 0; i < compressors_count; i++)
	{
		ereport(NOTICE, (errmsg("Compressor %s result %f", compressor_name[i], compressor_results[i]), errhidestmt(true)));
	}

	Log_error_verbosity = old_verbosity;

	PG_RETURN_VOID();
}
