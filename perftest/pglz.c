#include <limits.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#define MAXPGPATH 1024

#define uint64 uint64_t
#define int32 int32_t
#define uint32 uint32_t
#define int16 int16_t
#define uint16 uint16_t
#define int8 int8_t
#define Min(_a, _b) ((_a) < (_b) ? (_a) : (_b))
#define Max(_a, _b) ((_a) > (_b) ? (_a) : (_b))

#define ereport(__kek, ...) fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")
#define elog(__kek, ...) fprintf(stderr, __VA_ARGS__), fprintf(stderr, "\n")


/*-************************************
*  CPU Feature Detection
**************************************/
/* PGLZ_FORCE_MEMORY_ACCESS
 * By default, access to unaligned memory is controlled by `memcpy()`, which is safe and portable.
 * Unfortunately, on some target/compiler combinations, the generated assembly is sub-optimal.
 * The below switch allow to select different access method for improved performance.
 * Method 0 (default) : use `memcpy()`. Safe and portable.
 * Method 1 : direct access. This method is portable but violate C standard.
 *            It can generate buggy code on targets which assembly generation depends on alignment.
 *            But in some circumstances, it's the only known way to get the most performance (ie GCC + ARMv6)
 * See https://fastcompression.blogspot.fr/2015/08/accessing-unaligned-memory.html for details.
 * Prefer these methods in priority order (0 > 1)
 */
#ifndef PGLZ_FORCE_MEMORY_ACCESS   /* can be defined externally */
#  if defined(__GNUC__) && \
  ( defined(__ARM_ARCH_6__) || defined(__ARM_ARCH_6J__) || defined(__ARM_ARCH_6K__) \
  || defined(__ARM_ARCH_6Z__) || defined(__ARM_ARCH_6ZK__) || defined(__ARM_ARCH_6T2__) )
#    define PGLZ_FORCE_MEMORY_ACCESS 1
#  endif
#endif


#if defined(__x86_64__)
  typedef uint64 reg_t;   /* 64-bits in x32 mode */
#else
  typedef size_t reg_t;   /* 32-bits in x32 mode */
#endif


#if defined(PGLZ_FORCE_MEMORY_ACCESS) && (PGLZ_FORCE_MEMORY_ACCESS==1)
/* lie to the compiler about data alignment; use with caution */

static uint32 pglz_read32(const void* ptr) { return *(const uint32*) ptr; }

#else  /* safe and portable access using memcpy() */

static uint32 pglz_read32(const void* ptr)
{
    uint32 val; memcpy(&val, ptr, sizeof(val)); return val;
}

#endif /* PGLZ_FORCE_MEMORY_ACCESS */


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
#define PGLZ_HISTORY_SIZE	    0x0fff - 1    /* to avoid compare in iteration */
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
    int16 next_id;                      /* links for my hash key's list */
    uint16 hist_idx;                      /* my current hash key */
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
static inline uint16 pglz_hist_idx(const unsigned char* s, uint16 mask)
{
    return ((s[0] << 6) ^ (s[1] << 4) ^ (s[2] << 2) ^ s[3]) & mask;
}


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
static inline int16 pglz_hist_add(int16 hist_next, uint16 *hist_idx, const unsigned char* s, uint16 mask)
{
    int16* my_hist_start = &hist_start[*hist_idx];
    PGLZ_HistEntry* entry = &(hist_entries)[hist_next];

    entry->next_id = *my_hist_start;
    entry->hist_idx = *hist_idx;
    entry->pos = s;
    *my_hist_start = hist_next;

    *hist_idx = ((((*hist_idx) ^ (s[0] << 6)) << 2) ^ s[4]) & mask;
    hist_next++;
    if (hist_next == PGLZ_HISTORY_SIZE + 1)
    {
        hist_next = 1;
    }
    return hist_next;
}


/* ----------
 * pglz_out_tag -
 *
 *		Outputs a backward reference tag of 2-4 bytes (depending on
 *		offset and length) to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
inline unsigned char* pglz_out_tag(unsigned char* dest_ptr, int32 match_len, int32 match_offset)
{
    if (match_len > 17)															
    {																		
        *(dest_ptr++) = (unsigned char)((((match_offset) & 0xf00) >> 4) | 0x0f);		
        *(dest_ptr++) = (unsigned char)(((match_offset) & 0xff));						
        *(dest_ptr++) = (unsigned char)((match_len) - 18);							
    } else {																
        *(dest_ptr++) = (unsigned char)((((match_offset) & 0xf00) >> 4) | ((match_len) - 3));
        *(dest_ptr++) = (unsigned char)((match_offset) & 0xff);							
    }																		
    return dest_ptr;
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
pglz_find_match(uint16 hist_idx, const unsigned char *input, const unsigned char *end,
				int *len_ptr, int *offset_ptr, int good_match, int good_drop)
{
	PGLZ_HistEntry *hist_entry;
	int16		*hist_entry_number;
	int32		len = 0;
	int32		offset = 0;
	int32		cur_len = 0;
    int32 len_bound = Min(end - input, PGLZ_MAX_MATCH);

	/*
	 * Traverse the linked history list until a good enough match is found.
	 */
	hist_entry_number = &hist_start[hist_idx];
    if (*hist_entry_number == INVALID_ENTRY)
        return 0;

    hist_entry = &hist_entries[*hist_entry_number];
    if (hist_idx != hist_entry->hist_idx)
    {
        *hist_entry_number = INVALID_ENTRY;
        return 0;
    }

    while(true)
	{
		const unsigned char *input_pos = input;
		const unsigned char *hist_pos = hist_entry->pos;
        const unsigned char *my_pos;
		int32		cur_offset = input_pos - hist_pos;

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
            if (memcmp(input_pos, hist_pos, len) == 0)
            {
                offset = cur_offset;
                input_pos += len;
                hist_pos += len;
                while(len <= len_bound - 4 && pglz_read32(input_pos) == pglz_read32(hist_pos))
                {
                    len += 4;
                    input_pos += 4;
                    hist_pos += 4;
                }
                while (len < len_bound && *input_pos == *hist_pos)
                {
                    len++;
                    input_pos++;
                    hist_pos++;
                }
            }
		}
		else
		{
            if (pglz_read32(input_pos) == pglz_read32(hist_pos))
            {
                input_pos += 4;
                hist_pos += 4;
                cur_len = 4;
                while(cur_len <= len_bound - 4 && pglz_read32(input_pos) == pglz_read32(hist_pos))
                {
                    cur_len += 4;
                    input_pos += 4;
                    hist_pos += 4;
                }
                while (cur_len < len_bound && *input_pos == *hist_pos)
                {
                    cur_len++;
                    input_pos++;
                    hist_pos++;
                }
                if (cur_len > len)
                {
                    len = cur_len;
                    offset = cur_offset;
                }
            }
		}

		/*
		 * Advance to the next history entry
		 */
        my_pos = hist_entry->pos;
        hist_entry = &hist_entries[hist_entry->next_id];

        if (len >= good_match || my_pos <= hist_entry->pos || hist_idx != hist_entry->hist_idx)
        {
            break;
        }
		/*
		 * Be happy with lesser good matches the more entries we visited.
		 */
        good_match -= (good_match * good_drop) >> 7;
	}

    len = Min(len, len_bound);

	/*
	 * Return match information only if it results at least in one byte
	 * reduction.
	 */
	if (len > 2)
	{
		*len_ptr = len;
		*offset_ptr = offset;
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
pglz_compress(const char *source, int32 src_len, char *dest,
			  const PGLZ_Strategy *strategy)
{
	unsigned char *dest_ptr = (unsigned char *) dest;
	unsigned char *dest_start = dest_ptr;
	uint16			hist_next = 1;
    uint16          hist_idx;
	const unsigned char *src_ptr = (const unsigned char*)source;
	const unsigned char *src_end = (const unsigned char*)source + src_len;
    const unsigned char *compress_src_end = src_end - 4;
	unsigned char control_dummy = 0;
	unsigned char *control_ptr = &control_dummy;
	unsigned char control_byte = 0;
	unsigned char control_pos = 0;
	bool		found_match = false;
	int32		match_len;
	int32		match_offset;
	int32		good_match;
	int32		good_drop;
	int32		result_size;
	int32		result_max;
	int32		need_rate;
	int			hash_size;
	uint16		mask;


	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
    {
		strategy = PGLZ_strategy_default;
    }

	/*
	 * If the strategy forbids compression (at all or if source chunk size out
	 * of range), fail.
	 */
	if (strategy->match_size_good <= 0 ||
		src_len < strategy->min_input_size ||
		src_len > strategy->max_input_size)
		return -1;

	/*
	 * Limit the match parameters to the supported range.
	 */
	good_match = strategy->match_size_good;
	if (good_match > PGLZ_MAX_MATCH)
    {
		good_match = PGLZ_MAX_MATCH;
    }
	else if (good_match < 17)
    {
		good_match = 17;
    }

	good_drop = strategy->match_size_drop;
	if (good_drop < 0)
    {
		good_drop = 0;
    }
	else if (good_drop > 100)
    {
		good_drop = 100;
    }
    good_drop = good_drop * 128 / 100;

	need_rate = strategy->min_comp_rate;
	if (need_rate < 0)
    {
		need_rate = 0;
    }
	else if (need_rate > 99)
    {
		need_rate = 99;
    }

	/*
	 * Compute the maximum result size allowed by the strategy, namely the
	 * input size minus the minimum wanted compression rate.  This had better
	 * be <= src_len, else we might overrun the provided output buffer.
	 */
	if (src_len > (INT_MAX / 100))
	{
		/* Approximate to avoid overflow */
		result_max = (src_len / 100) * (100 - need_rate);
	}
	else
    {
		result_max = (src_len * (100 - need_rate)) / 100;
    }

	/*
	 * Experiments suggest that these hash sizes work pretty well. A large
	 * hash table minimizes collision, but has a higher startup cost. For a
	 * small input, the startup cost dominates. The table size must be a power
	 * of two.
	 */
	if (src_len < 128)
		hash_size = 512;
	else if (src_len < 256)
		hash_size = 1024;
	else if (src_len < 512)
		hash_size = 2048;
	else if (src_len < 1024)
		hash_size = 4096;
	else
		hash_size = 8192;
	mask = hash_size - 1;

	/*
	 * Initialize the history lists to empty.  We do not need to zero the
	 * hist_entries[] array; its entries are initialized as they are used.
	 */
	memset(hist_start, 0, hash_size * sizeof(int16));
    hist_entries[INVALID_ENTRY].pos = src_end;
    hist_idx = pglz_hist_idx(src_ptr, mask);

	/*
	 * Compress the source directly into the output buffer.
	 */
	while (src_ptr < compress_src_end)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (dest_ptr - dest_start >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && dest_ptr - dest_start >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
        if ((control_pos & 0xff) == 0)												
        {																		
            *(control_ptr) = control_byte;												
            control_ptr = (dest_ptr)++;												
            control_byte = 0;														
            control_pos = 1;															
        }																		
		/*
		 * Try to find a match in the history
		 */
		if (pglz_find_match(hist_idx, src_ptr, compress_src_end, &match_len,
							&match_offset, good_match, good_drop))
		{
			/*
			 * Create the tag and add history entries for all matched
			 * characters.
			 */
            control_byte |= control_pos;														
            dest_ptr = pglz_out_tag(dest_ptr, match_len, match_offset);
			while (match_len--)
			{
				hist_next = pglz_hist_add(hist_next, &hist_idx, src_ptr, mask);
				src_ptr++;			/* Do not do this ++ in the line above! */
				/* The macro would do it four times - Jan.  */
			}
			found_match = true;
		}
		else
		{
			/*
			 * No match found. Copy one literal byte.
			 */
			hist_next = pglz_hist_add(hist_next, &hist_idx, src_ptr, mask);
            *(dest_ptr)++ = (unsigned char)(*src_ptr);
			src_ptr++;				/* Do not do this ++ in the line above! */
			/* The macro would do it four times - Jan.  */
		}
        control_pos <<= 1;
	}


	while (src_ptr < src_end)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (dest_ptr - dest_start >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && dest_ptr - dest_start >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
        if ((control_pos & 0xff) == 0)												
        {																		
            *(control_ptr) = control_byte;												
            control_ptr = (dest_ptr)++;												
            control_byte = 0;														
            control_pos = 1;															
        }																		
        *(dest_ptr)++ = (unsigned char)(*src_ptr);
        src_ptr++;
        control_pos <<= 1;
	}


	/*
	 * Write out the last control byte and check that we haven't overrun the
	 * output size allowed by the strategy.
	 */
	*control_ptr = control_byte;
	result_size = dest_ptr - dest_start;
	if (result_size >= result_max)
		return -1;

	/* success */
	return result_size;
}

