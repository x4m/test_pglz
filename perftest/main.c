/* ----------
 * pg_lzcompress.c -
 *
 *		This is an implementation of LZ compression for PostgreSQL.
 *		It uses a simple history table and generates 2-3 byte tags
 *		capable of backward copy information for 3-273 bytes with
 *		a max offset of 4095.
 *
 *		Entry routines:
 *
 *			int32
 *			pglz_compress(const char *source, int32 slen, char *dest,
 *						  const PGLZ_Strategy *strategy);
 *
 *				source is the input data to be compressed.
 *
 *				slen is the length of the input data.
 *
 *				dest is the output area for the compressed result.
 *					It must be at least as big as PGLZ_MAX_OUTPUT(slen).
 *
 *				strategy is a pointer to some information controlling
 *					the compression algorithm. If NULL, the compiled
 *					in default strategy is used.
 *
 *				The return value is the number of bytes written in the
 *				buffer dest, or -1 if compression fails; in the latter
 *				case the contents of dest are undefined.
 *
 *			int32
 *			pglz_decompress(const char *source, int32 slen, char *dest,
 *							int32 rawsize, bool check_complete)
 *
 *				source is the compressed input.
 *
 *				slen is the length of the compressed input.
 *
 *				dest is the area where the uncompressed data will be
 *					written to. It is the callers responsibility to
 *					provide enough space.
 *
 *					The data is written to buff exactly as it was handed
 *					to pglz_compress(). No terminating zero byte is added.
 *
 *				rawsize is the length of the uncompressed data.
 *
 *				check_complete is a flag to let us know if -1 should be
 *					returned in cases where we don't reach the end of the
 *					source or dest buffers, or not.  This should be false
 *					if the caller is asking for only a partial result and
 *					true otherwise.
 *
 *				The return value is the number of bytes written in the
 *				buffer dest, or -1 if decompression fails.
 *
 *		The decompression algorithm and internal data format:
 *
 *			It is made with the compressed data itself.
 *
 *			The data representation is easiest explained by describing
 *			the process of decompression.
 *
 *			If compressed_size == rawsize, then the data
 *			is stored uncompressed as plain bytes. Thus, the decompressor
 *			simply copies rawsize bytes to the destination.
 *
 *			Otherwise the first byte tells what to do the next 8 times.
 *			We call this the control byte.
 *
 *			An unset bit in the control byte means, that one uncompressed
 *			byte follows, which is copied from input to output.
 *
 *			A set bit in the control byte means, that a tag of 2-3 bytes
 *			follows. A tag contains information to copy some bytes, that
 *			are already in the output buffer, to the current location in
 *			the output. Let's call the three tag bytes T1, T2 and T3. The
 *			position of the data to copy is coded as an offset from the
 *			actual output position.
 *
 *			The offset is in the upper nibble of T1 and in T2.
 *			The length is in the lower nibble of T1.
 *
 *			So the 16 bits of a 2 byte tag are coded as
 *
 *				7---T1--0  7---T2--0
 *				OOOO LLLL  OOOO OOOO
 *
 *			This limits the offset to 1-4095 (12 bits) and the length
 *			to 3-18 (4 bits) because 3 is always added to it. To emit
 *			a tag of 2 bytes with a length of 2 only saves one control
 *			bit. But we lose one byte in the possible length of a tag.
 *
 *			In the actual implementation, the 2 byte tag's length is
 *			limited to 3-17, because the value 0xF in the length nibble
 *			has special meaning. It means, that the next following
 *			byte (T3) has to be added to the length value of 18. That
 *			makes total limits of 1-4095 for offset and 3-273 for length.
 *
 *			Now that we have successfully decoded a tag. We simply copy
 *			the output that occurred <offset> bytes back to the current
 *			output location in the specified <length>. Thus, a
 *			sequence of 200 spaces (think about bpchar fields) could be
 *			coded in 4 bytes. One literal space and a three byte tag to
 *			copy 199 bytes with a -1 offset. Whow - that's a compression
 *			rate of 98%! Well, the implementation needs to save the
 *			original data size too, so we need another 4 bytes for it
 *			and end up with a total compression rate of 96%, what's still
 *			worth a Whow.
 *
 *		The compression algorithm
 *
 *			The following uses numbers used in the default strategy.
 *
 *			The compressor works best for attributes of a size between
 *			1K and 1M. For smaller items there's not that much chance of
 *			redundancy in the character sequence (except for large areas
 *			of identical bytes like trailing spaces) and for bigger ones
 *			our 4K maximum look-back distance is too small.
 *
 *			The compressor creates a table for lists of positions.
 *			For each input position (except the last 3), a hash key is
 *			built from the 4 next input bytes and the position remembered
 *			in the appropriate list. Thus, the table points to linked
 *			lists of likely to be at least in the first 4 characters
 *			matching strings. This is done on the fly while the input
 *			is compressed into the output area.  Table entries are only
 *			kept for the last 4096 input positions, since we cannot use
 *			back-pointers larger than that anyway.  The size of the hash
 *			table is chosen based on the size of the input - a larger table
 *			has a larger startup cost, as it needs to be initialized to
 *			zero, but reduces the number of hash collisions on long inputs.
 *
 *			For each byte in the input, its hash key (built from this
 *			byte and the next 3) is used to find the appropriate list
 *			in the table. The lists remember the positions of all bytes
 *			that had the same hash key in the past in increasing backward
 *			offset order. Now for all entries in the used lists, the
 *			match length is computed by comparing the characters from the
 *			entries position with the characters from the actual input
 *			position.
 *
 *			The compressor starts with a so called "good_match" of 128.
 *			It is a "prefer speed against compression ratio" optimizer.
 *			So if the first entry looked at already has 128 or more
 *			matching characters, the lookup stops and that position is
 *			used for the next tag in the output.
 *
 *			For each subsequent entry in the history list, the "good_match"
 *			is lowered by 10%. So the compressor will be more happy with
 *			short matches the farer it has to go back in the history.
 *			Another "speed against ratio" preference characteristic of
 *			the algorithm.
 *
 *			Thus there are 3 stop conditions for the lookup of matches:
 *
 *				- a match >= good_match is found
 *				- there are no more history entries to look at
 *				- the next history entry is already too far back
 *				  to be coded into a tag.
 *
 *			Finally the match algorithm checks that at least a match
 *			of 3 or more bytes has been found, because that is the smallest
 *			amount of copy information to code into a tag. If so, a tag
 *			is omitted and all the input bytes covered by that are just
 *			scanned for the history add's, otherwise a literal character
 *			is omitted and only his history entry added.
 *
 *		Acknowledgments:
 *
 *			Many thanks to Adisak Pochanayon, who's article about SLZ
 *			inspired me to write the PostgreSQL compression this way.
 *
 *			Jan Wieck
 *
 * Copyright (c) 1999-2019, PostgreSQL Global Development Group
 *
 * src/common/pg_lzcompress.c
 * ----------
 */

#include <limits.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#define MAXPGPATH 1024

#define int32 int32_t
#define int16 int16_t
#define int8 int8_t
#define Min(_a, _b) ((_a) < (_b) ? (_a) : (_b))
#define Max(_a, _b) ((_a) > (_b) ? (_a) : (_b))

#define ereport(__kek, ...) fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")
#define elog(__kek, ...) fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")


typedef struct PGLZ_Strategy
{
	int32		min_input_size;
	int32		max_input_size;
	int32		min_comp_rate;
	int32		first_success_by;
	int32		match_size_good;
	int32		match_size_drop;
} PGLZ_Strategy;

/* ----------
 * Local definitions
 * ----------
 */
#define PGLZ_MAX_HISTORY_LISTS	8192	/* must be power of 2 */
#define PGLZ_HISTORY_SIZE		4096
#define PGLZ_MAX_MATCH			273


/* ----------
 * PGLZ_HistEntry -
 *
 *		Linked list for the backward history lookup
 *
 * All the entries sharing a hash key are linked in a doubly linked list.
 * This makes it easy to remove an entry when it's time to recycle it
 * (because it's more than 4K positions old).
 * ----------
 */
typedef struct PGLZ_HistEntry
{
	struct PGLZ_HistEntry *next;	/* links for my hash key's list */
	struct PGLZ_HistEntry *prev;
	int			hindex;			/* my current hash key */
	const unsigned char *pos;			/* my input position */
} PGLZ_HistEntry;


/* ----------
 * The provided standard strategies
 * ----------
 */
static const PGLZ_Strategy strategy_default_data = {
	32,							/* Data chunks less than 32 bytes are not
								 * compressed */
	INT_MAX,					/* No upper limit on what we'll try to
								 * compress */
	25,							/* Require 25% compression rate, or not worth
								 * it */
	1024,						/* Give up if no compression in the first 1KB */
	128,						/* Stop history lookup if a match of 128 bytes
								 * is found */
	10							/* Lower good match size by 10% at every loop
								 * iteration */
};
const PGLZ_Strategy *const PGLZ_strategy_default = &strategy_default_data;


static const PGLZ_Strategy strategy_always_data = {
	0,							/* Chunks of any size are compressed */
	INT_MAX,
	0,							/* It's enough to save one single byte */
	INT_MAX,					/* Never give up early */
	128,						/* Stop history lookup if a match of 128 bytes
								 * is found */
	6							/* Look harder for a good match */
};
const PGLZ_Strategy *const PGLZ_strategy_always = &strategy_always_data;


/* ----------
 * Statically allocated work arrays for history
 * ----------
 */
static int16 hist_start[PGLZ_MAX_HISTORY_LISTS];
static PGLZ_HistEntry hist_entries[PGLZ_HISTORY_SIZE + 1];

/*
 * Element 0 in hist_entries is unused, and means 'invalid'. Likewise,
 * INVALID_ENTRY_PTR in next/prev pointers mean 'invalid'.
 */
#define INVALID_ENTRY			0
#define INVALID_ENTRY_PTR		(&hist_entries[INVALID_ENTRY])

/* ----------
 * pglz_hist_idx -
 *
 *		Computes the history table slot for the lookup by the next 4
 *		characters in the input.
 *
 * NB: because we use the next 4 characters, we are not guaranteed to
 * find 3-character matches; they very possibly will be in the wrong
 * hash list.  This seems an acceptable tradeoff for spreading out the
 * hash keys more.
 * ----------
 */
#define pglz_hist_idx(_s,_e, _mask) (										\
			((((_e) - (_s)) < 4) ? (int) (_s)[0] :							\
			 (((_s)[0] << 6) ^ ((_s)[1] << 4) ^								\
			  ((_s)[2] << 2) ^ (_s)[3])) & (_mask)				\
		)


/* ----------
 * pglz_hist_add -
 *
 *		Adds a new entry to the history table.
 *
 * If _recycle is true, then we are recycling a previously used entry,
 * and must first delink it from its old hashcode's linked list.
 *
 * NOTE: beware of multiple evaluations of macro's arguments, and note that
 * _hn and _recycle are modified in the macro.
 * ----------
 */
#define pglz_hist_add(_hs,_he,_hn,_recycle,_s,_e, _mask)	\
do {									\
			int __hindex = pglz_hist_idx((_s),(_e), (_mask));				\
			int16 *__myhsp = &(_hs)[__hindex];								\
			PGLZ_HistEntry *__myhe = &(_he)[_hn];							\
			if (_recycle) {													\
				if (__myhe->prev == NULL)									\
					(_hs)[__myhe->hindex] = __myhe->next - (_he);			\
				else														\
					__myhe->prev->next = __myhe->next;						\
				if (__myhe->next != NULL)									\
					__myhe->next->prev = __myhe->prev;						\
			}																\
			__myhe->next = &(_he)[*__myhsp];								\
			__myhe->prev = NULL;											\
			__myhe->hindex = __hindex;										\
			__myhe->pos  = (_s);											\
			/* If there was an existing entry in this hash slot, link */	\
			/* this new entry to it. However, the 0th entry in the */		\
			/* entries table is unused, so we can freely scribble on it. */ \
			/* So don't bother checking if the slot was used - we'll */		\
			/* scribble on the unused entry if it was not, but that's */	\
			/* harmless. Avoiding the branch in this critical path */		\
			/* speeds this up a little bit. */								\
			/* if (*__myhsp != INVALID_ENTRY) */							\
				(_he)[(*__myhsp)].prev = __myhe;							\
			*__myhsp = _hn;													\
			if (++(_hn) >= PGLZ_HISTORY_SIZE + 1) {							\
				(_hn) = 1;													\
				(_recycle) = true;											\
			}																\
} while (0)


/* ----------
 * pglz_out_ctrl -
 *
 *		Outputs the last and allocates a new control byte if needed.
 * ----------
 */
#define pglz_out_ctrl(__ctrlp,__ctrlb,__ctrl,__buf) \
do { \
	if ((__ctrl & 0xff) == 0)												\
	{																		\
		*(__ctrlp) = __ctrlb;												\
		__ctrlp = (__buf)++;												\
		__ctrlb = 0;														\
		__ctrl = 1;															\
	}																		\
} while (0)


/* ----------
 * pglz_out_literal -
 *
 *		Outputs a literal byte to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#define pglz_out_literal(_ctrlp,_ctrlb,_ctrl,_buf,_byte) \
do { \
	pglz_out_ctrl(_ctrlp,_ctrlb,_ctrl,_buf);								\
	*(_buf)++ = (unsigned char)(_byte);										\
	_ctrl <<= 1;															\
} while (0)
// void pglz_out_literal(unsigned char* _ctrlp, unsigned char _ctrlb, unsigned char _ctrl, unsigned char* _buf, const char _byte) {
// 	pglz_out_ctrl(_ctrlp,_ctrlb,_ctrl,_buf);								
// 	*(_buf)++ = (unsigned char)(_byte);										
// 	_ctrl <<= 1;															
// }


/* ----------
 * pglz_out_tag -
 *
 *		Outputs a backward reference tag of 2-4 bytes (depending on
 *		offset and length) to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#define pglz_out_tag(_ctrlp,_ctrlb,_ctrl,_buf,_len,_off) \
do { \
	pglz_out_ctrl(_ctrlp,_ctrlb,_ctrl,_buf);								\
	_ctrlb |= _ctrl;														\
	_ctrl <<= 1;															\
	if (_len > 17)															\
	{																		\
		(_buf)[0] = (unsigned char)((((_off) & 0xf00) >> 4) | 0x0f);		\
		(_buf)[1] = (unsigned char)(((_off) & 0xff));						\
		(_buf)[2] = (unsigned char)((_len) - 18);							\
		(_buf) += 3;														\
	} else {																\
		(_buf)[0] = (unsigned char)((((_off) & 0xf00) >> 4) | ((_len) - 3)); \
		(_buf)[1] = (unsigned char)((_off) & 0xff);							\
		(_buf) += 2;														\
	}																		\
} while (0)


inline unsigned char* pglz_put_tag(unsigned char* bp, int32 match_len, int32 match_off)
{
    if (match_len > 17)															
    {																		
        *(bp++) = (unsigned char)((((match_off) & 0xf00) >> 4) | 0x0f);		
        *(bp++) = (unsigned char)(((match_off) & 0xff));						
        *(bp++) = (unsigned char)((match_len) - 18);							
    } else {																
        *(bp++) = (unsigned char)((((match_off) & 0xf00) >> 4) | ((match_len) - 3));
        *(bp++) = (unsigned char)((match_off) & 0xff);							
    }																		
    return bp;
}


/* ----------
 * pglz_find_match -
 *
 *		Lookup the history table if the actual input stream matches
 *		another sequence of characters, starting somewhere earlier
 *		in the input buffer.
 * ----------
 */
static inline int
pglz_find_match(int16 *hstart, const unsigned char *input, const unsigned char *end,
				int *lenp, int *offp, int good_match, int good_drop, int mask)
{
	PGLZ_HistEntry *hent;
	int16		hentno;
	int32		len = 0;
	int32		off = 0;

	/*
	 * Traverse the linked history list until a good enough match is found.
	 */
	hentno = hstart[pglz_hist_idx(input, end, mask)];
	hent = &hist_entries[hentno];
	while (hent != INVALID_ENTRY_PTR)
	{
		const unsigned char *ip = input;
		const unsigned char *hp = hent->pos;
		int32		thisoff;
		int32		thislen;

		/*
		 * Stop if the offset does not fit into our tag anymore.
		 */
		thisoff = ip - hp;
		if (thisoff >= 0x0fff)
			break;

		/*
		 * Determine length of match. A better match must be larger than the
		 * best so far. And if we already have a match of 16 or more bytes,
		 * it's worth the call overhead to use memcmp() to check if this match
		 * is equal for the same size. After that we must fallback to
		 * character by character comparison to know the exact position where
		 * the diff occurred.
		 */
		thislen = 0;
		if (len >= 16)
		{
			if (memcmp(ip, hp, len) == 0)
			{
				thislen = len;
				ip += len;
				hp += len;
				while (ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH)
				{
					thislen++;
					ip++;
					hp++;
				}
			}
		}
		else
		{
			while (ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH)
			{
				thislen++;
				ip++;
				hp++;
			}
		}

		/*
		 * Remember this match as the best (if it is)
		 */
		if (thislen > len)
		{
			len = thislen;
			off = thisoff;
		}

		/*
		 * Advance to the next history entry
		 */
		hent = hent->next;

		/*
		 * Be happy with lesser good matches the more entries we visited. But
		 * no point in doing calculation if we're at end of list.
		 */
		if (hent != INVALID_ENTRY_PTR)
		{
			if (len >= good_match)
				break;
			good_match -= (good_match * good_drop) / 100;
		}
	}

	/*
	 * Return match information only if it results at least in one byte
	 * reduction.
	 */
	if (len > 2)
	{
		*lenp = len;
		*offp = off;
		return 1;
	}

	return 0;
}


/* ----------
 * pglz_compress -
 *
 *		Compresses source into dest using strategy. Returns the number of
 *		bytes written in buffer dest, or -1 if compression fails.
 * ----------
 */
int32
pglz_compress_vanilla(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	unsigned char *bp = (unsigned char *) dest;
	unsigned char *bstart = bp;
	int			hist_next = 1;
	bool		hist_recycle = false;
	const unsigned char *dp = (const unsigned char*)source;
	const unsigned char *dend = (const unsigned char*)source + slen;
	unsigned char ctrl_dummy = 0;
	unsigned char *ctrlp = &ctrl_dummy;
	unsigned char ctrlb = 0;
	unsigned char ctrl = 0;
	bool		found_match = false;
	int32		match_len;
	int32		match_off;
	int32		good_match;
	int32		good_drop;
	int32		result_size;
	int32		result_max;
	int32		need_rate;
	int			hashsz;
	int			mask;

	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
		strategy = PGLZ_strategy_default;

	/*
	 * If the strategy forbids compression (at all or if source chunk size out
	 * of range), fail.
	 */
	if (strategy->match_size_good <= 0 ||
		slen < strategy->min_input_size ||
		slen > strategy->max_input_size)
		return -1;

	/*
	 * Limit the match parameters to the supported range.
	 */
	good_match = strategy->match_size_good;
	if (good_match > PGLZ_MAX_MATCH)
		good_match = PGLZ_MAX_MATCH;
	else if (good_match < 17)
		good_match = 17;

	good_drop = strategy->match_size_drop;
	if (good_drop < 0)
		good_drop = 0;
	else if (good_drop > 100)
		good_drop = 100;

	need_rate = strategy->min_comp_rate;
	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	/*
	 * Compute the maximum result size allowed by the strategy, namely the
	 * input size minus the minimum wanted compression rate.  This had better
	 * be <= slen, else we might overrun the provided output buffer.
	 */
	if (slen > (INT_MAX / 100))
	{
		/* Approximate to avoid overflow */
		result_max = (slen / 100) * (100 - need_rate);
	}
	else
		result_max = (slen * (100 - need_rate)) / 100;

	/*
	 * Experiments suggest that these hash sizes work pretty well. A large
	 * hash table minimizes collision, but has a higher startup cost. For a
	 * small input, the startup cost dominates. The table size must be a power
	 * of two.
	 */
	if (slen < 128)
		hashsz = 512;
	else if (slen < 256)
		hashsz = 1024;
	else if (slen < 512)
		hashsz = 2048;
	else if (slen < 1024)
		hashsz = 4096;
	else
		hashsz = 8192;
	mask = hashsz - 1;

	/*
	 * Initialize the history lists to empty.  We do not need to zero the
	 * hist_entries[] array; its entries are initialized as they are used.
	 */
	memset(hist_start, 0, hashsz * sizeof(int16));

	/*
	 * Compress the source directly into the output buffer.
	 */
	while (dp < dend)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (bp - bstart >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && bp - bstart >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
        if ((ctrl & 0xff) == 0)												
        {																		
            *(ctrlp) = ctrlb;												
            ctrlp = (bp)++;												
            ctrlb = 0;														
            ctrl = 1;															
        }																		
		/*
		 * Try to find a match in the history
		 */
		if (pglz_find_match(hist_start, dp, dend, &match_len,
							&match_off, good_match, good_drop, mask))
		{
			/*
			 * Create the tag and add history entries for all matched
			 * characters.
			 */
            ctrlb |= ctrl;														
            bp = pglz_put_tag(bp, match_len, match_off);
			while (match_len--)
			{
				pglz_hist_add(hist_start, hist_entries,
							  hist_next, hist_recycle,
							  dp, dend, mask);
				dp++;			/* Do not do this ++ in the line above! */
				/* The macro would do it four times - Jan.  */
			}
			found_match = true;
		}
		else
		{
			/*
			 * No match found. Copy one literal byte.
			 */
            *(bp)++ = (unsigned char)(*dp);
			pglz_hist_add(hist_start, hist_entries,
						  hist_next, hist_recycle,
						  dp, dend, mask);
			dp++;				/* Do not do this ++ in the line above! */
			/* The macro would do it four times - Jan.  */
		}
        ctrl <<= 1;
	}

	/*
	 * Write out the last control byte and check that we haven't overrun the
	 * output size allowed by the strategy.
	 */
	*ctrlp = ctrlb;
	result_size = bp - bstart;
	if (result_size >= result_max)
		return -1;

	/* success */
	return result_size;
}


//----------------------------------------------------------------------------------------------------- 

typedef struct PGLZ_HistEntry1
{
    int16 next_id;
	int32			epoch;
	const unsigned char *pos;			/* my input position */
} PGLZ_HistEntry1;

static PGLZ_HistEntry1 hist_entries1[PGLZ_HISTORY_SIZE + 1];


static inline int pglz_hist_idx1(const unsigned char* s, int mask) {
    return ((s[0] << 6) ^ (s[1] << 4) ^ (s[2] << 2) ^ s[3]) & mask;
}

static inline int pglz_hist_add1(int hist_next, int *hindex, int32 epoch_counter, const unsigned char* s, int mask)
{
    int16* my_hist_start = &hist_start[*hindex];
    PGLZ_HistEntry1* entry = &(hist_entries1)[hist_next];

    entry->epoch = epoch_counter;
    entry->pos = s;
    entry->next_id = *my_hist_start;
    *my_hist_start = hist_next;

    *hindex = ((((*hindex) ^ (s[0] << 6)) << 2) ^ s[4]) & mask;
    // hist_next = (hist_next & (PGLZ_HISTORY_SIZE - 1)) + 1;
    hist_next++;
    if (hist_next == PGLZ_HISTORY_SIZE + 1) {
        hist_next = 1;
    }
    return hist_next;
}

static inline int
pglz_find_match1(int hindex, const unsigned char *input, const unsigned char *end,
				int *lenp, int *offp, int good_match, int good_drop)
{
	PGLZ_HistEntry1 *hent;
	int16		hentno;
	int32		len = 0;
	int32		off = 0;
	int32		thislen = 0;

    good_drop = good_drop * 128 / 100;
	/*
	 * Traverse the linked history list until a good enough match is found.
	 */
	hentno = hist_start[hindex];
    if (hentno == INVALID_ENTRY) {
        return 0;
    }
    hent = &hist_entries1[hentno];
    while(true)
	{
		const unsigned char *ip = input;
		const unsigned char *hp = hent->pos;
        const int32* ip1 = input;
        const int32* hp1 = hent->pos;
		int32		thisoff;
        int32 len_bound = Min(end - ip, PGLZ_MAX_MATCH);
        int32 my_epoch;

		/*
		 * Stop if the offset does not fit into our tag anymore.
		 */
		thisoff = ip - hp;
		if (thisoff >= 0x0fff)
			break;

		/*
		 * Determine length of match. A better match must be larger than the
		 * best so far. And if we already have a match of 16 or more bytes,
		 * it's worth the call overhead to use memcmp() to check if this match
		 * is equal for the same size. After that we must fallback to
		 * character by character comparison to know the exact position where
		 * the diff occurred.
		 */
		if (len >= 16)
		{
            if (memcmp(ip, hp, len) == 0)
            {
                off = thisoff;
                ip1 = ip + len;
                hp1 = hp + len;
                while(len <= len_bound - 4 && *ip1 == *hp1) {
                    len += 4;
                    ip1++;
                    hp1++;
                }
                ip = ip1;
                hp = hp1;
                while (len < len_bound && *ip == *hp)
                {
                    len++;
                    ip++;
                    hp++;
                }
            }
		}
		else
		{
            if (*ip1 == *hp1) {
                ip1++;
                hp1++;
                thislen = 4;
                while(thislen <= len_bound - 4 && *ip1 == *hp1) {
                    thislen += 4;
                    ip1++;
                    hp1++;
                }
                ip = ip1;
                hp = hp1;
                while (thislen < len_bound && *ip == *hp)
                {
                    thislen++;
                    ip++;
                    hp++;
                }
                if (thislen > len)
                {
                    len = thislen;
                    off = thisoff;
                }
            }
		}
        my_epoch = hent->epoch;
        hent = &hist_entries1[hent->next_id];
        if (len >= good_match || my_epoch <= hent->epoch)
            break;
        good_match -= (good_match * good_drop) >> 7;
	}

	if (len > 2)
	{
		*lenp = len;
		*offp = off;
		return 1;
	}

	return 0;
}





int32
pglz_compress_hacked(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	unsigned char *bp = (unsigned char *) dest;
	unsigned char *bstart = bp;
	int			hist_next = 1;
	const unsigned char *dp = (const unsigned char*)source;
	const unsigned char *dend = (const unsigned char*)source + slen;
    const unsigned char *compressing_dend = dend - 4;
	unsigned char ctrl_dummy = 0;
	unsigned char *ctrlp = &ctrl_dummy;
	unsigned char ctrlb = 0;
	unsigned char ctrl = 0;
	bool		found_match = false;
	int32		match_len;
	int32		match_off;
	int32		good_match;
	int32		good_drop;
	int32		result_size;
	int32		result_max;
	int32		need_rate;
	int			hashsz;
	int			mask;
    int hist_idx;
    int32 epoch_counter = 0;


	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
		strategy = PGLZ_strategy_default;

	/*
	 * If the strategy forbids compression (at all or if source chunk size out
	 * of range), fail.
	 */
	if (strategy->match_size_good <= 0 ||
		slen < strategy->min_input_size ||
		slen > strategy->max_input_size)
		return -1;

	/*
	 * Limit the match parameters to the supported range.
	 */
	good_match = strategy->match_size_good;
	if (good_match > PGLZ_MAX_MATCH)
		good_match = PGLZ_MAX_MATCH;
	else if (good_match < 17)
		good_match = 17;

	good_drop = strategy->match_size_drop;
	if (good_drop < 0)
		good_drop = 0;
	else if (good_drop > 100)
		good_drop = 100;

	need_rate = strategy->min_comp_rate;
	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	/*
	 * Compute the maximum result size allowed by the strategy, namely the
	 * input size minus the minimum wanted compression rate.  This had better
	 * be <= slen, else we might overrun the provided output buffer.
	 */
	if (slen > (INT_MAX / 100))
	{
		/* Approximate to avoid overflow */
		result_max = (slen / 100) * (100 - need_rate);
	}
	else
		result_max = (slen * (100 - need_rate)) / 100;

	/*
	 * Experiments suggest that these hash sizes work pretty well. A large
	 * hash table minimizes collision, but has a higher startup cost. For a
	 * small input, the startup cost dominates. The table size must be a power
	 * of two.
	 */
	if (slen < 128)
		hashsz = 512;
	else if (slen < 256)
		hashsz = 1024;
	else if (slen < 512)
		hashsz = 2048;
	else if (slen < 1024)
		hashsz = 4096;
	else
		hashsz = 8192;
	mask = hashsz - 1;

	/*
	 * Initialize the history lists to empty.  We do not need to zero the
	 * hist_entries[] array; its entries are initialized as they are used.
	 */
	memset(hist_start, 0, hashsz * sizeof(int16));
    hist_entries1[INVALID_ENTRY].epoch = INT32_MAX;
    hist_idx = pglz_hist_idx1(dp, mask);

	/*
	 * Compress the source directly into the output buffer.
	 */
	while (dp < compressing_dend)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (bp - bstart >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && bp - bstart >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
        if ((ctrl & 0xff) == 0)												
        {																		
            *(ctrlp) = ctrlb;												
            ctrlp = (bp)++;												
            ctrlb = 0;														
            ctrl = 1;															
        }																		
		/*
		 * Try to find a match in the history
		 */
		if (pglz_find_match1(hist_idx, dp, compressing_dend, &match_len,
							&match_off, good_match, good_drop))
		{
			/*
			 * Create the tag and add history entries for all matched
			 * characters.
			 */
            ctrlb |= ctrl;														
            bp = pglz_put_tag(bp, match_len, match_off);
			while (match_len--)
			{
				hist_next = pglz_hist_add1(hist_next, &hist_idx, epoch_counter, dp, mask);
				dp++;			/* Do not do this ++ in the line above! */
				/* The macro would do it four times - Jan.  */
			}
			found_match = true;
		}
		else
		{
			/*
			 * No match found. Copy one literal byte.
			 */
			hist_next = pglz_hist_add1(hist_next, &hist_idx, epoch_counter, dp, mask);
            *(bp)++ = (unsigned char)(*dp);
			dp++;				/* Do not do this ++ in the line above! */
			/* The macro would do it four times - Jan.  */
		}
        ctrl <<= 1;
        ++epoch_counter;
	}


	while (dp < dend)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (bp - bstart >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && bp - bstart >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
        if ((ctrl & 0xff) == 0)												
        {																		
            *(ctrlp) = ctrlb;												
            ctrlp = (bp)++;												
            ctrlb = 0;														
            ctrl = 1;															
        }																		
        *(bp)++ = (unsigned char)(*dp);
        dp++;
        ctrl <<= 1;
	}


	/*
	 * Write out the last control byte and check that we haven't overrun the
	 * output size allowed by the strategy.
	 */
	*ctrlp = ctrlb;
	result_size = bp - bstart;
	if (result_size >= result_max)
		return -1;

	/* success */
	return result_size;
}


//----------------------------------------------------------------------------------------------------- 


#define MAX_SA (8192 + PGLZ_MAX_MATCH)
int16 cl[MAX_SA], cl_n[MAX_SA];
int16 pos[MAX_SA], pos_n[MAX_SA];
int16 cnt[MAX_SA];
int8 lcp[MAX_SA];
int16 cl_repr[MAX_SA];
int16 rpos[MAX_SA];
int16 cl_cnt;

inline int16 sum(int16 a, int16 b, int16 module) {
    a += b;
    if (a >= module) {
        return a - module;
    }
    return a;
}

inline int16 sub(int16 a, int16 b, int16 module) {
    a -= b;
    if (a < 0) {
        return a + module;
    }
    return a;
}

void sort_pos(int16 cl_cnt, int16 offset, int16 len) {
    int16 i;

    for(i = 0; i < len; ++i) {
        pos_n[i] = sub(pos[i], offset, len);
    }
    memset(cnt, 0, cl_cnt * sizeof(int16));
    for(i = 0; i < len; ++i) {
        ++cnt[cl[i]];
    }
    for(i = 1; i < cl_cnt; ++i) {
        cnt[i] += cnt[i - 1];
    }
    for(i = len - 1; i >= 0; --i) {
        pos[--cnt[cl[pos_n[i]]]] = pos_n[i];
    }
}

int16 calc_cl(int16 step, int16 len) {
    int16 i;
    int16 cl_cnt = 1;

    cl_n[pos[0]] = 0;
    for(i = 1; i < len; ++i) {
        if (cl[pos[i]] != cl[pos[i - 1]] || cl[sum(pos[i], step, len)] != cl[sum(pos[i - 1], step, len)]) { // TODO : can optimize sum
            ++cl_cnt;
        }
        cl_n[pos[i]] = cl_cnt - 1;
    }
    memcpy(cl, cl_n, len * sizeof(int16));
    return cl_cnt;
}

#define MX_STEP (1 << 4)

void calc_lcp(const char* source, int16 len, int16 cl_cnt) {
    int16 cur_lcp = 0;
    int16 i, j;

    for(i = 0; i < len; ++i) {
        if (cur_lcp > 0) {
            --cur_lcp;
        }
        if (cl_repr[cl[i]] != i || cl[i] == cl_cnt - 1) {
            continue;
        }
        j = cl_repr[cl[i] + 1];
        while(source[sum(i, cur_lcp, len)] == source[sum(j, cur_lcp, len)] && cur_lcp < MX_STEP) { // TODO : can optimize
            ++cur_lcp;
        }
        lcp[rpos[i]] = cur_lcp;
    }
    lcp[len - 1] = 0;
}

void build_sa(const char* source, int16 len) {
    int16 i, h, step;

    for(i = 0; i < len; ++i) {
        pos[i] = i;
        cl[i] = (unsigned char)source[i];
    }
    for(h = 1, cl_cnt = 256; h <= MX_STEP; h *= 2) {
        step = h / 2;
        sort_pos(cl_cnt, step, len);
        cl_cnt = calc_cl(step, len);
    }
    for(i = 0; i < len - 1; ++i) {
        if (cl[pos[i]] != cl[pos[i + 1]]) {
            cl_repr[cl[pos[i]]] = pos[i];
        }
    }
    cl_repr[cl[pos[len - 1]]] = pos[len - 1];
    for(i = 0; i < len; ++i) {
        rpos[pos[i]] = i;
    }
    memset(lcp, MX_STEP, len);
    calc_lcp(source, len, cl_cnt);
    
    /*
    printf("%d\n", len);
    puts("Pos:");
    for(i = 0; i < len; ++i) {
        printf("%02d ", pos[i]);
    }
    puts("");
    puts("Cl:");
    for(i = 0; i < len; ++i) {
        printf("%02d ", cl[i]);
    }
    puts("");
    puts("Lcp:");
    for(i = 0; i < len; ++i) {
        printf("%02d ", lcp[i]);
    }
    puts(""); */
}

inline void add_long_match(const char* dp, const char* sa_start, const char* sa_end, int32 str_pos, int32 upd_sa_pos, int32* len, int32* off) {
    int32 cur_len;
    int32 cur_pos = pos[upd_sa_pos];

    const char* curp;
    const char* thosep;

    if (cur_pos < str_pos && str_pos < cur_pos + 0x0fff) {
        // TODO : memcmp optimize
        curp = dp + MX_STEP;
        thosep = sa_start + cur_pos + MX_STEP;
        while (curp < sa_end && *curp == *thosep) {
            ++curp;
            ++thosep;
        }
        cur_len = curp - dp;
        if (cur_len > *len) {
            *len = cur_len;
            *off = str_pos  - cur_pos;
        }
    }
}

int32 pglz_find_sa_match(const char* dp, const char* sa_start, const char* sa_end, int32 sa_len, int32* match_len, int32* match_off) {
    int32 str_pos = dp - sa_start;
    int32 left = rpos[str_pos];
    int32 right = rpos[str_pos];
    int32 cur_pos;

    int32 len = 0;
    int32 off = 0;

    int32 left_lcp = MX_STEP;
    int32 right_lcp = MX_STEP;
    int32 best_lcp;

    while(left > 0 && lcp[left - 1] == MX_STEP && len < PGLZ_MAX_MATCH) {
        --left;
        add_long_match(dp, sa_start, sa_end, str_pos, left, &len, &off);
    }
    while(right + 1 < sa_len && lcp[right] == MX_STEP && len < PGLZ_MAX_MATCH) {
        ++right;
        add_long_match(dp, sa_start, sa_end, str_pos, right, &len, &off);
    }
    if (len > 0) {
        *match_len = Min(len, PGLZ_MAX_MATCH);
        *match_off = off;
        // elog(NOTICE, "Found 16+ match!\n");
        return 1;
    }
    while(true) {
        if (left != 0) {
            left_lcp = Min(left_lcp, lcp[left - 1]);
        } else {
            left_lcp = 0;
        }
        if (right + 1 != sa_len) {
            right_lcp = Min(right_lcp, lcp[right]);
        } else {
            right_lcp = 0;
        }
        best_lcp = Max(left_lcp, right_lcp);
        if (best_lcp < 3) {
            return 0;
        }
        if (left_lcp > right_lcp) {
            --left;
            cur_pos = pos[left];
        } else {
            ++right;
            cur_pos = pos[right];
        }
        if (cur_pos < str_pos && str_pos < cur_pos + 0x0fff) {
            *match_len = best_lcp;
            *match_off = str_pos - cur_pos;
            return 1;
        }
    }
}

int32
pglz_compress_suff_arr(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	unsigned char *bp = (unsigned char *) dest;
	unsigned char *bstart = bp;

	const char *dp = source;
	const char *dend = source + slen;
	unsigned char ctrl_dummy = 0;
	unsigned char *ctrlp = &ctrl_dummy;
	unsigned char ctrlb = 0;
	unsigned char ctrl = 0;
	bool		found_match = false;
	int32		match_len;
	int32		match_off;

	int32		need_rate;
	int32		result_size;
	int32		result_max;

    int32 sa_len;
    const char* sa_end;
    const char* sa_start;

	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
		strategy = PGLZ_strategy_default;

	/*
	 * If the strategy forbids compression (at all or if source chunk size out
	 * of range), fail.
	 */
	if (strategy->match_size_good <= 0 ||
		slen < strategy->min_input_size ||
		slen > strategy->max_input_size)
		return -1;

	need_rate = strategy->min_comp_rate;
	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	/*
	 * Compute the maximum result size allowed by the strategy, namely the
	 * input size minus the minimum wanted compression rate.  This had better
	 * be <= slen, else we might overrun the provided output buffer.
	 */
	if (slen > (INT_MAX / 100))
	{
		/* Approximate to avoid overflow */
		result_max = (slen / 100) * (100 - need_rate);
	}
	else
		result_max = (slen * (100 - need_rate)) / 100;

	/*
	 * Compress the source directly into the output buffer.
	 */
    sa_len = Min(PGLZ_HISTORY_SIZE + PGLZ_MAX_MATCH, dend - dp);
    sa_start = dp;
    build_sa(sa_start, sa_len);
    sa_end = dp + Min(PGLZ_HISTORY_SIZE, dend - dp);
	while (dp < dend)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (bp - bstart >= result_max) {
            // elog(ERROR, "bp - bstart >= result_max");
			return -1;
        }

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && bp - bstart >= strategy->first_success_by) {
            // elog(ERROR, "!found_match && bp - bstart >= strategy->first_success_by");
			return -1;
        }

        if (sa_end < dp)
        {
            sa_len = Min(PGLZ_HISTORY_SIZE + PGLZ_MAX_MATCH, dend - dp) + PGLZ_HISTORY_SIZE;
            sa_start = dp - PGLZ_HISTORY_SIZE;
            build_sa(sa_start, sa_len);
            sa_end = dp + Min(PGLZ_HISTORY_SIZE, dend - dp);
        }

		/*
		 * Refresh control byte if needed.
		 */
        if ((ctrl & 0xff) == 0)												
        {																		
            *(ctrlp) = ctrlb;												
            ctrlp = (bp)++;												
            ctrlb = 0;														
            ctrl = 1;															
        }																		
		/*
		 * Try to find a match in the history
		 */
        if (pglz_find_sa_match(dp, sa_start, sa_end, sa_len, &match_len, &match_off))
		{
            // elog(NOTICE, "Match: %d", match_len);
			/*
			 * Create the tag.
			 */
            ctrlb |= ctrl;														
            bp = pglz_put_tag(bp, match_len, match_off);
            dp += match_len;
			found_match = true;
		}
		else
		{
            // elog(NOTICE, "Symbol: %d", (int)(*dp));
			/*
			 * No match found. Copy one literal byte.
			 */
            *(bp)++ = (unsigned char)*(dp++);
		}
        ctrl <<= 1;
	}

	/*
	 * Write out the last control byte and check that we haven't overrun the
	 * output size allowed by the strategy.
	 */
	*ctrlp = ctrlb;
	result_size = bp - bstart;
	if (result_size >= result_max) {
        // elog(ERROR, "result_size >= result_max");
		return -1;
    }

	/* success */
	return result_size;
}

//----------------------------------------------------------------------------------------------------- 


/* ----------
 * pglz_decompress -
 *
 *		Decompresses source into dest. Returns the number of bytes
 *		decompressed in the destination buffer, and *optionally*
 *		checks that both the source and dest buffers have been
 *		fully read and written to, respectively.
 * ----------
 */
int32
pglz_decompress_vanilla(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete)
{
	const unsigned char *sp;
	const unsigned char *srcend;
	unsigned char *dp;
	unsigned char *destend;

	sp = (const unsigned char *) source;
	srcend = ((const unsigned char *) source) + slen;
	dp = (unsigned char *) dest;
	destend = dp + rawsize;

	while (sp < srcend && dp < destend)
	{
		/*
		 * Read one control byte and process the next 8 items (or as many as
		 * remain in the compressed input).
		 */
		unsigned char ctrl = *sp++;
		int			ctrlc;

		for (ctrlc = 0; ctrlc < 8 && sp < srcend && dp < destend; ctrlc++)
		{

			if (ctrl & 1)
			{
				/*
				 * Otherwise it contains the match length minus 3 and the
				 * upper 4 bits of the offset. The next following byte
				 * contains the lower 8 bits of the offset. If the length is
				 * coded as 18, another extension tag byte tells how much
				 * longer the match really was (0-255).
				 */
				int32		len;
				int32		off;

				len = (sp[0] & 0x0f) + 3;
				off = ((sp[0] & 0xf0) << 4) | sp[1];
				sp += 2;
				if (len == 18)
					len += *sp++;

				/*
				 * Now we copy the bytes specified by the tag from OUTPUT to
				 * OUTPUT. It is dangerous and platform dependent to use
				 * memcpy() here, because the copied areas could overlap
				 * extremely!
				 */
				len = Min(len, destend - dp);
				while (len--)
				{
					*dp = dp[-off];
					dp++;
				}
			}
			else
			{
				/*
				 * An unset control bit means LITERAL BYTE. So we just copy
				 * one from INPUT to OUTPUT.
				 */
				*dp++ = *sp++;
			}

			/*
			 * Advance the control bit
			 */
			ctrl >>= 1;
		}
	}

	/*
	 * Check we decompressed the right amount.
	 * If we are slicing, then we won't necessarily
	 * be at the end of the source or dest buffers
	 * when we hit a stop, so we don't test them.
	 */
	if (check_complete && (dp != destend || sp != srcend))
		return -1;

	/*
	 * That's it.
	 */
	return (char*)dp - dest;
}



//----------------------------------------------------------------------------------------------------- 



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


typedef int32 (*decompress_func)(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete);

typedef int32 (*compress_func)(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);


int32
pglz_compress_vanilla(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);

int32
pglz_compress_suff_arr(const char *source, int32 slen, char *dest,
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
	pglz_decompress_vanilla,
};
char *decompressor_name[] = 
{
	"pglz_decompress_vanilla - warmup", /* do vanilla test at the beginning and at the end */
};

int decompressors_count = 1;

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
    int32 i;
	ereport(LOG,
		"Testing payload %s\tcompressor %s\tdecompressor %s",
			payload_names[payload], compressor_name[compressor], decompressor_name[decompressor]);
	void *data = payloads[payload];
	long size = payload_sizes[payload];
	void *extracted_data = malloc(size);
    void *compressed = malloc(size * 2);

	compressors[compressor](data, size, compressed, PGLZ_strategy_default);
	clock_t compression_begin = clock();
	int comp_size = compressors[compressor](data, size, compressed, PGLZ_strategy_default);
	clock_t copmression_end = clock();

	clock_t decompression_begin = clock();
	if (decompressors[decompressor](compressed, comp_size, extracted_data, size, true) != size)
		elog(ERROR, "decompressed wrong size %d instead of %d",decompressors[decompressor](compressed, comp_size, extracted_data, size, true),(int)size);
	clock_t decopmression_end = clock();

	if (memcmp(extracted_data, data, size)) {
		elog(ERROR, "decompressed different data");
        for(i = 0; i < size; ++i) {
            if (((char*)extracted_data)[i] != ((char*)data)[i]) {
                elog(ERROR, "Differs at: %d", i);
                break;
            }
        }
    }
	
	ereport(LOG,
		"Compression %ld\t(%f seconds)\tDecompression %ld\t(%f seconds)\tRatio %f",
			copmression_end - compression_begin, ((float)copmression_end - compression_begin) / CLOCKS_PER_SEC,
			decopmression_end - decompression_begin, ((float)decopmression_end - decompression_begin) / CLOCKS_PER_SEC,
			comp_size/(float)size);

	free(extracted_data);
	free(compressed);

	if (decompression_time)
		return ((double)decopmression_end - decompression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
	else
		return ((double)copmression_end - compression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
}

double do_sliced_test(int compressor, int decompressor, int payload, int slice_size, bool decompression_time)
{
	ereport(LOG,
		"Testing %dKb slicing payload %s\tcompressor %s\tdecompressor %s", slice_size / 1024,
			payload_names[payload], compressor_name[compressor], decompressor_name[decompressor]);
	char *data = payloads[payload];
	long size = payload_sizes[payload];
	int slice_count = size / slice_size;
	void **extracted_data = malloc(slice_count * sizeof(void*));
	void **compressed = malloc(slice_count * sizeof(void*));
	int *comp_size = malloc(slice_count * sizeof(int));
	int i;
	for (i = 0; i < slice_count; i++)
	{
		extracted_data[i] = malloc(slice_size * 2);
		compressed[i] = malloc(slice_size * 2);
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
		free(extracted_data[i]);
		free(compressed[i]);
	}
	free(extracted_data);
	free(comp_size);
	free(compressed);

	if (decompression_time)
		return ((double)decopmression_end - decompression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
	else
		return ((double)compression_end - compression_begin) * (1000000000.0L / size) / CLOCKS_PER_SEC;
}

static void prepare_payloads()
{
	payloads = malloc(sizeof(void*) * payload_count);
	payload_sizes = malloc(sizeof(long) * payload_count);

	char share_path[MAXPGPATH] = "/home/vladimirlesk/pg_src/postgres/contrib/test_pglz";
	char path[MAXPGPATH];
	FILE *f;
	long size;
	int i;

	for (i=0; i< payload_count; i++)
	{
		snprintf(path, MAXPGPATH, "%s/%s", share_path, payload_names[i]);
        puts(path);
		f = fopen(path, "r");
		if (!f)
			elog(ERROR, "unable to open payload");

		fseek (f , 0 , SEEK_END);
		size = ftell (f);
		rewind (f);
		
		void* data = malloc(size);

		if (fread(data, size, 1, f) != 1)
			elog(ERROR, "unable to read payload");
		fclose(f);
		payloads[i] = data;
		payload_sizes[i] = size;
	}
}

void test_pglz()
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

	ereport(NOTICE, "Time to decompress one byte in ns:");
	for (p = 0; p < payload_count; p++)
	{
		ereport(NOTICE, "Payload %s", payload_names[p]);
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, "Decompressor %s result %f", decompressor_name[i], decompression_results[p][i]);
			decompressor_results[i] += decompression_results[p][i];
		}

		ereport(NOTICE, "Payload %s sliced by 2Kb", payload_names[p]);
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, "Decompressor %s result %f", decompressor_name[i], decompression_results[p][i]);
			decompressor_results[i] += decompression_sliced_2kb_results[p][i];
		}

		ereport(NOTICE, "Payload %s sliced by 8Kb", payload_names[p]);
		for (i = 1; i < decompressors_count; i++)
		{
			ereport(NOTICE, "Decompressor %s result %f", decompressor_name[i], decompression_sliced_8kb_results[p][i]);
			decompressor_results[i] += decompression_sliced_8kb_results[p][i];
		}
	}

	ereport(NOTICE, "\n\nDecompressor score (summ of all times):");
	for (i = 1; i < decompressors_count; i++)
	{
		ereport(NOTICE, "Decompressor %s result %f", decompressor_name[i], decompressor_results[i]);
	}

	ereport(NOTICE, "Time to compress one byte in ns:");
	for (p = 0; p < payload_count; p++)
	{
		ereport(NOTICE, "Payload %s", payload_names[p]);
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, "Compressor %s result %f", compressor_name[i], compression_results[p][i]);
			compressor_results[i] += compression_results[p][i];
		}

		ereport(NOTICE, "Payload %s sliced by 2Kb", payload_names[p]);
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, "Compressor %s result %f", compressor_name[i], compression_results[p][i]);
			compressor_results[i] += compression_sliced_2kb_results[p][i];
		}

		ereport(NOTICE, "Payload %s sliced by 8Kb", payload_names[p]);
		for (i = 0; i < compressors_count; i++)
		{
			ereport(NOTICE, "Compressor %s result %f", compressor_name[i], compression_sliced_8kb_results[p][i]);
			compressor_results[i] += compression_sliced_8kb_results[p][i];
		}
	}

	ereport(NOTICE, "\n\nCompressor score (summ of all times):");
	for (i = 0; i < compressors_count; i++)
	{
		ereport(NOTICE, "Compressor %s result %f", compressor_name[i], compressor_results[i]);
	}
}

#define SSZ 400
#define DSZ 400
unsigned char src_data[SSZ];
unsigned char compressed_data[DSZ + 1];
unsigned char dst_data[DSZ + 1];

int do_compression_test(int32 src_len) {
    int32 res;
    int32 comp_len;
    int32 i;
    comp_len = pglz_compress_suff_arr((char*) src_data, src_len, (char*)compressed_data, NULL);
    if (comp_len == -1) {
        return 0;
    }
    //printf("%d\n", res);
    /*if (res > 0) {
        for(i = 0; i < res; ++i) {
            printf("%d ", (int)compressed_data[i]);
        }
    }*/
    res = pglz_decompress_vanilla((char*)compressed_data, comp_len, (char*)dst_data, src_len, true);
    printf("\nDST: %d\n", res);
    if (res == -1) {
        puts("Decompression failed");
        puts("Compressed:");
        for(i = 0; i < comp_len; ++i) {
            printf("%d ", (int)compressed_data[i]);
        }
        puts("");
        puts("Decompressed:");
        for(i = 0; i < res; ++i) {
            printf("%d ", (int)dst_data[i]);
        }
        puts("");
        puts("SRC:");
        for(i = 0; i < src_len; ++i) {
            printf("%d ", (int)src_data[i]);
        }
        return 1;
    }
    for(i = 0; i < res; ++i) {
        if (src_data[i] != dst_data[i]) {
            printf("Fail at: %d, symbol: %d\n", i, dst_data[i]);
            puts("Compressed:");
            for(i = 0; i < comp_len; ++i) {
                printf("%d ", (int)compressed_data[i]);
            }
            puts("");
            puts("Decompressed:");
            for(i = 0; i < res; ++i) {
                printf("%d ", (int)dst_data[i]);
            }
            puts("");
            puts("SRC:");
            for(i = 0; i < src_len; ++i) {
                printf("%d ", (int)src_data[i]);
            }
            return 1;
        }
    }
    if (res < src_len) {
        puts("Fail, too short!");
        for(i = 0; i < res; ++i) {
            printf("%d ", (int)dst_data[i]);
        }
        puts("");
        puts("SRC:");
        for(i = 0; i < res; ++i) {
            printf("%d ", (int)src_data[i]);
        }
        return 1;
    }
    return 0;
}

int main(int argc, char** argv) {
    test_pglz();
    return 0;
    int32 src_len;
    int32 i;
    srand(0);
    while(true) {
        src_len = 200 + rand() % 10;
        for(i = 0; i < src_len; ++i) {
            src_data[i] = rand() % 4;
        }
        if (do_compression_test(src_len)) {
            return 0;
        }
    }
    return 0;
}
