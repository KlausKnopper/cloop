#ifndef _COMPRESSED_LOOP_H
#define _COMPRESSED_LOOP_H

#define CLOOP_HEADROOM 128

/* The cloop header usually looks like this:          */
/* #!/bin/sh                                          */
/* #V2.00 Format                                      */
/* ...padding up to CLOOP_HEADROOM...                 */
/* block_size (32bit number, network order)           */
/* num_blocks (32bit number, network order)           */

struct cloop_head
{
	char preamble[CLOOP_HEADROOM];
	u_int32_t block_size;
	u_int32_t num_blocks;
};

/* data_index (num_blocks 64bit pointers, network order)...      */
/* compressed data (gzip block compressed format)...             */

/* Cloop suspend IOCTL */
#define CLOOP_SUSPEND 0x4C07

#endif /*_COMPRESSED_LOOP_H*/
