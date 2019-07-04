#include "postgres.h"

#include <limits.h>

#include "common/pg_lzcompress.h"

int32
pglz_decompress_hacked(const char *source, int32 slen, char *dest,
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
				while (off <= len)
				{
					memcpy(dp, dp - off, off);
					len -= off;
					dp+=off;
					off *= 2;
				}
				memcpy(dp, dp - off, len);
				dp+=len;
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

static unsigned char * pglz_decompress_emmit_match(unsigned char **sp, unsigned char *dp,
						unsigned char *destend)
{
	/*
	 * Output should contain the match length minus 3 and the
	 * upper 4 bits of the offset. The next following byte
	 * contains the lower 8 bits of the offset. If the length is
	 * coded as 18, another extension tag byte tells how much
	 * longer the match really was (0-255).
	 */
	int32		len;
	int32		off;

	len = (**sp & 0x0f) + 3;
	off = ((**sp & 0xf0) << 4) | (*sp)[1];
	*sp += 2;
	if (len == 18)
	{
		len += **sp;
		*sp += 1;
	}

	/*
	 * Now we copy the bytes specified by the tag from OUTPUT to
	 * OUTPUT. It is dangerous and platform dependent to use
	 * memcpy() here, because the copied areas could overlap
	 * extremely!
	 */
	len = Min(len, destend - dp);
	while (off <= len)
	{
		memcpy(dp, dp - off, off);
		len -= off;
		dp+=off;
		off *= 2;
	}
	memcpy(dp, dp - off, len);
	dp+=len;
	return dp;
}

int32
pglz_decompress_hacked_unrolled(const char *source, int32 slen, char *dest,
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


		/*
		 * An unset control bit means LITERAL BYTE. So we just copy
		 * one from INPUT to OUTPUT.
		 */
		unsigned char ctrl = *sp++;
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x1)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x2)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x4)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x8)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x10)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x20)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x40)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
		}
		if (sp < srcend && dp < destend)
		{
			if (ctrl & 0x80)
				dp = pglz_decompress_emmit_match(&sp, dp, destend);
			else
				*dp++ = *sp++;
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


int32
pglz_decompress_hacked4(const char *source, int32 slen, char *dest,
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
				if (len > 4)
				{
					while (off <= len)
					{
						memcpy(dp, dp - off, off);
						len -= off;
						dp+=off;
						off *= 2;
					}
					memcpy(dp, dp - off, len);
					dp+=len;
				}
				else
				{
					while (len--)
					{
						*dp = dp[-off];
						dp++;
					}
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

int32
pglz_decompress_hacked8(const char *source, int32 slen, char *dest,
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
				if (len > 8)
				{
					while (off <= len)
					{
						memcpy(dp, dp - off, off);
						len -= off;
						dp+=off;
						off *= 2;
					}
					memcpy(dp, dp - off, len);
					dp+=len;
				}
				else
				{
					while (len--)
					{
						*dp = dp[-off];
						dp++;
					}
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


int32
pglz_decompress_hacked16(const char *source, int32 slen, char *dest,
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
				if (len > 16)
				{
					while (off <= len)
					{
						memcpy(dp, dp - off, off);
						len -= off;
						dp+=off;
						off *= 2;
					}
					memcpy(dp, dp - off, len);
					dp+=len;
				}
				else
				{
					while (len--)
					{
						*dp = dp[-off];
						dp++;
					}
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


int32
pglz_decompress_hacked32(const char *source, int32 slen, char *dest,
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
				if (len > 32)
				{
					while (off <= len)
					{
						memcpy(dp, dp - off, off);
						len -= off;
						dp+=off;
						off *= 2;
					}
					memcpy(dp, dp - off, len);
					dp+=len;
				}
				else
				{
					while (len--)
					{
						*dp = dp[-off];
						dp++;
					}
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
