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
pglz_decompress_vanilla(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);
int32
pglz_decompress_hacked(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);
int32
pglz_decompress_hacked2(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);


double do_test(int compressor, int decompressor, int payload);

compress_func compressors[] = {pglz_compress_vanilla};
char *compressor_name[] = {"pglz_compress_vanilla"};
int compressors_count = 1;

decompress_func decompressors[] =
{
	pglz_decompress_vanilla,
	pglz_decompress_hacked,
	pglz_decompress_hacked2,
	pglz_decompress_vanilla,
};
char *decompressor_name[] = 
{
	"pglz_decompress_vanilla - warmup", /* do vanilla test at the beginning and at the end */
	"pglz_decompress_hacked",
	"pglz_decompress_hacked2",
	"pglz_decompress_vanilla",
};

int decompressors_count = 4;

char *payload_names[] =
{
	"000000010000000000000001",
	"000000010000000000000006",
	"16398",
};
void **payloads;
long *payload_sizes;
int payload_count = 3;

/* benchmark returns ns per byte of payload to decompress */
double do_test(int compressor, int decompressor, int payload)
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
		elog(ERROR, "decompressed wrong size");
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

	return ((double)decopmression_end - decompression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
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
	double results[10][10];
	int iterations = 2;
	int iteration;
	int i,p;
	prepare_payloads();
	for (p = 0; p < payload_count; p++)
		for (i = 0; i < decompressors_count; i++)
		{
			results[p][i] = 0;
			for (iteration = 0; iteration < iterations; iteration++)
				results[p][i] += do_test(0, i, p);
			results[p][i] /= iterations;
		}

	ereport(NOTICE, (errmsg("Time to decompress one byte in ns:"), errhidestmt(true)));
	for (i = 1; i < decompressors_count; i++)
	{
		char msg[1024];
		char* msgx = msg;
		snprintf(msg, 1024, "%s", decompressor_name[i]);
		for (p = 0; p < payload_count; p++)
		{
			snprintf(msg, 1024, "%s\t%f", msg, results[p][i]);
		}
		ereport(NOTICE, (errmsg("%s",msg), errhidestmt(true)));
	}

	PG_RETURN_VOID();
}
