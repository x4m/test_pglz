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

compress_func compressors[] = {pglz_compress_vanilla};
char *compressor_name[] = {"pglz_compress_vanilla"};
int compressors_count = 1;

decompress_func decompressors[] = {pglz_decompress_vanilla, pglz_decompress_hacked};
char *decompressor_name[] = {"pglz_decompress_vanilla", "pglz_decompress_hacked"};
int decompressors_count = 2;

char* payloads[] = {"000000010000000000000006"};
int payload_count = 1;


void do_test(int compressor, int decompressor, int payload)
{
	char share_path[MAXPGPATH];
	char path[MAXPGPATH];
	FILE *f;
	long size;

	ereport(LOG,
		(errmsg("Testing payload %s\tcompressor %s\tdecompressor %s",
			payloads[payload], compressor_name[compressor], decompressor_name[decompressor]),
		errhidestmt(true)));

	get_share_path(my_exec_path, share_path);
	snprintf(path, MAXPGPATH, "%s/extension/%s", share_path, payloads[payload]);
	f = fopen(path, "r");
	if (!f)
		elog(ERROR, "unable to open payload");

	fseek (f , 0 , SEEK_END);
	size = ftell (f);
 	rewind (f);
	
	void* data = palloc(size);
	void* extracted_data = palloc(size);

	if (fread(data, size, 1, f) != 1)
		elog(ERROR, "unable to read payload");
	fclose(f);

	void *compressed = palloc(size * 2);

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
		(errmsg("Compression %d\t(%f seconds)\tDecompression %d\t(%f seconds)",
			copmression_end - compression_begin, ((float)copmression_end - compression_begin) / CLOCKS_PER_SEC,
			decopmression_end - decompression_begin, ((float)decopmression_end - decompression_begin) / CLOCKS_PER_SEC),
		errhidestmt(true)));

	pfree(data);
	pfree(extracted_data);
	pfree(compressed);
}

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_pglz(PG_FUNCTION_ARGS)
{
	for (int i = 0; i < decompressors_count; i++)
	{
		do_test(0, i, 0);
	}

	PG_RETURN_VOID();
}