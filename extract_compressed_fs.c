/* Extracts a filesystem back from a compressed cloop file */
/* Extended to support stdin 31.5.2008 Klaus Knopper       */
/* License: GPL V2                                         */

#define _LARGEFILE64_SOURCE
#define _XOPEN_SOURCE 600

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <endian.h>
#include <errno.h>
#include <string.h>
#include <zlib.h>
#include <netinet/in.h>
#include <inttypes.h>

#ifdef __CYGWIN__
typedef uint64_t loff_t;
#endif
#ifndef be64toh
static __inline __uint64_t
__bswap64(__uint64_t _x)
{

	return ((_x >> 56) | ((_x >> 40) & 0xff00) | ((_x >> 24) & 0xff0000) |
	    ((_x >> 8) & 0xff000000) | ((_x << 8) & ((__uint64_t)0xff << 32)) |
	    ((_x << 24) & ((__uint64_t)0xff << 40)) |
	    ((_x << 40) & ((__uint64_t)0xff << 48)) | ((_x << 56)));
}
#if BYTE_ORDER == LITTLE_ENDIAN
#define be64toh(x)	__bswap64(x)
#else
#define be64toh(x) x
#endif
#endif /* !be64toh */
#define __be64_to_cpu be64toh
#include "cloop.h"

struct compressed_block
{
	size_t size;
	void *data;
};

int main(int argc, char *argv[])
{
	int handle, output;
	unsigned int i, total_blocks, total_offsets, offsets_size,
	    compressed_buffer_size, uncompressed_buffer_size;
	struct cloop_head head;
	unsigned char *compressed_buffer, *uncompressed_buffer;
	loff_t *offsets;
	/* For statistics */
	loff_t compressed_bytes, uncompressed_bytes, block_modulo;

	if (argc != 3) {
		fprintf(stderr, "Syntax: %s infile outfile, use \"-\" for stdin/stdout.\n", argv[0]);
		exit(1);
	}

	if(!strcmp(argv[1],"-")) handle = STDIN_FILENO;
	else {
		handle = open(argv[1], O_RDONLY|O_LARGEFILE);
		if (handle < 0) {
			perror("Opening compressed input file\n");
			exit(1);
		}
		/* Never ever attempt to cache file content in
		 * filesystem cache, since we really need it just ONCE. */
		fdatasync(handle);
		posix_fadvise(handle, 0, 0, POSIX_FADV_DONTNEED|POSIX_FADV_SEQUENTIAL);
	}

	if(!strcmp(argv[2],"-")) output = STDOUT_FILENO;
	else {
		output = open(argv[2], O_CREAT|O_WRONLY|O_LARGEFILE,
		                       S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
		if (output < 0) {
			perror("Opening uncompressed output file\n");
			exit(1);
		}
		/* Never ever attempt to cache file content in
		 * filesystem cache, since we really need it just ONCE. */
		fdatasync(output);
		posix_fadvise(output, 0, 0, POSIX_FADV_DONTNEED|POSIX_FADV_SEQUENTIAL);
	}


	if (read(handle, &head, sizeof(head)) != sizeof(head)) {
		perror("Reading compressed file header\n");
		exit(1);
	}

	total_blocks = ntohl(head.num_blocks);
	uncompressed_buffer_size = ntohl(head.block_size);

	fprintf(stderr, "%s: compressed input has %u blocks of size %u.\n",
		argv[0], total_blocks, uncompressed_buffer_size);


	/* The maximum size of a compressed block, due to the
	 * specification of uncompress() */
	compressed_buffer_size = uncompressed_buffer_size + uncompressed_buffer_size/1000 + 12 + 4;
	compressed_buffer = malloc(compressed_buffer_size);
	if (compressed_buffer == NULL) {
		perror("Out of memory for compressed buffer");
		fprintf(stderr," (%d bytes).\n", compressed_buffer_size);
		exit(1);
	}

	uncompressed_buffer = malloc(uncompressed_buffer_size);
	if (uncompressed_buffer == NULL) {
		perror("Out of memory for uncompressed buffer");
		fprintf(stderr," (%d bytes).\n", uncompressed_buffer_size);
		exit(1);
	}


	/* Store block index in memory to avoid seek()ing a lot */
	total_offsets  = total_blocks + 1;
	offsets_size = total_offsets * sizeof(loff_t);
	offsets = (loff_t *)malloc(offsets_size);
	if (offsets == NULL) {
		perror("Out of memory");
		fprintf(stderr, " for %d offsets.\n", total_offsets);
		exit(1);
	}

	if (read(handle, offsets, offsets_size) != offsets_size) {
		perror("Reading offsets");
		fprintf(stderr, " (%d bytes).\n", offsets_size);
		exit(1);
	}
	
	for (i = 0, compressed_bytes=0, uncompressed_bytes=0, block_modulo = total_blocks / 10;
	     i < total_blocks;
	     i++) {
                int size = __be64_to_cpu(offsets[i+1]) - __be64_to_cpu(offsets[i]);
		uLongf destlen = uncompressed_buffer_size;
		if (size < 0 || size > compressed_buffer_size) {
			fprintf(stderr, 
				"%s: Size %d for block %u (offset %" PRIu64 ") wrong, corrupt data!\n",
				argv[0], size, i, (uint64_t) __be64_to_cpu(offsets[i]));
			exit(1);
		}
		if(read(handle, compressed_buffer, size) != size) {
			perror("Reading block");
			fprintf(stderr, " %u (offset %" PRIu64 ") of size %d.\n", i,
			     (uint64_t) __be64_to_cpu(offsets[i]), size);
			exit(1);
		}

#if 0 /* DEBUG */
		if (i == 3) {
			fprintf(stderr,
				"Block head:%02X%02X%02X%02X%02X%02X%02X%02X\n",
				buffer[0],
				buffer[1],
				buffer[2],
				buffer[3],
				buffer[4],
				buffer[5],
				buffer[6],
				buffer[7]);
			fprintf(stderr,
				"Block tail:%02X%02X%02X%02X%02X%02X%02X%02X\n",
				buffer[3063],
				buffer[3064],
				buffer[3065],
				buffer[3066],
				buffer[3067],
				buffer[3068],
				buffer[3069],
				buffer[3070]);
		}
#endif
		switch (uncompress(uncompressed_buffer, &destlen,
				   compressed_buffer, size)) {
			case Z_OK: break;

			case Z_MEM_ERROR:
				fprintf(stderr, "Uncomp: oom block %u\n", i);
				exit(1);
				break;

			case Z_BUF_ERROR:
				fprintf(stderr, "Uncomp: not enough out room %u\n", i);
				exit(1);
				break;

			case Z_DATA_ERROR:
				fprintf(stderr, "Uncomp: input corrupt %u\n", i);
				exit(1);
				break;

			default:
				fprintf(stderr, "Uncomp: unknown error %u\n", i);
				exit(1);
		}
		compressed_bytes += size; uncompressed_bytes += destlen;
		if(((i % block_modulo) == 0) || (i == (total_blocks - 1))) {
			fprintf(stderr, "[Current block: %6u, In: %" PRIu64 "kB, Out: %" PRIu64 "kB, ratio %d%%, complete %3d%%]\n",
			        i, 
              (uint64_t) compressed_bytes / 1024L,
              (uint64_t) uncompressed_bytes / 1024L,
				(int)((uncompressed_bytes * 100L) / compressed_bytes),
				(int)(i * 100 / (total_blocks - 1)));
		}
		write(output, uncompressed_buffer, destlen);
		fdatasync(output);
	}
	return 0;
}
