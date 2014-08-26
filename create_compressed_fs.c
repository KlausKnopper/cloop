/* Creates a compressed image, given a file as an argument.
 * (c)1999 Paul `Rusty' Russell.  GPL.
 *
 * CHANGELOG:
 * * Sun Okt 26 01:05:29 CEST 2003 Klaus Knopper
 * - Changed format of index pointers to network byte order
 * * Sat Sep 29 2001 Klaus Knopper <knopper@knopper.net>
 * - changed compression to Z_BEST_COMPRESSION,
 * * Sat Jun 17 2000 Klaus Knopper <knopper@knopper.net>
 * - Support for reading file from stdin,
 * - Changed Preamble.
 * * Sat Jul 28 2001 Klaus Knopper <knopper@knopper.net>
 * - cleanup and gcc 2.96 / glibc checking
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#if defined(__FreeBSD__)
#include <sys/endian.h>
#include <netinet/in.h>
typedef uint64_t loff_t;
#ifndef htobe64
static __inline __uint64_t
__bswap64(__uint64_t _x)
{

	return ((_x >> 56) | ((_x >> 40) & 0xff00) | ((_x >> 24) & 0xff0000) |
	    ((_x >> 8) & 0xff000000) | ((_x << 8) & ((__uint64_t)0xff << 32)) |
	    ((_x << 24) & ((__uint64_t)0xff << 40)) |
	    ((_x << 40) & ((__uint64_t)0xff << 48)) | ((_x << 56)));
}
#if BYTE_ORDER == LITTLE_ENDIAN
#define htobe64(x)	__bswap64(x)
#else
#define htobe64(x)
#endif
#endif
#define __cpu_to_be64 htobe64
#else
#include <asm/byteorder.h>
#endif
#include <fcntl.h>
#include <zlib.h>
#include "compressed_loop.h"

#define MAX_KMALLOC_SIZE 2L<<17

#define CLOOP_PREAMBLE "#!/bin/sh\n" "#V2.0 Format\n" "insmod cloop.o file=$0 && mount -r -t iso9660 /dev/cloop $1\n" "exit $?\n"

struct cb_list
{
	struct cb_list *next;
	size_t size;
	char data[0];
};

void free_cb_list(struct cb_list *cbl)
{
 if(cbl->next) free_cb_list(cbl->next);
 free(cbl);
}

/* Now using the goto style because it is quicker to read */
static struct cb_list *create_compressed_blocks(int handle, unsigned long
                          blocksize, unsigned long *numblocks)
{
 struct cb_list *cbl,**cbp=&cbl;
 unsigned long i=0;
 unsigned int last;
 unsigned long long total_uncompressed=0,total_compressed=0;
 unsigned long maxlen = blocksize + blocksize/1000 + 12;
 char *compressed, *uncompressed;
 if((uncompressed=malloc(blocksize))==NULL)
  {
   fprintf(stderr, "*** Can't malloc(%ld).\n",blocksize);
   return NULL;
  }
 if((compressed=malloc(maxlen))==NULL)
  {
   fprintf(stderr, "*** Can't malloc(%ld).\n",blocksize);
   goto free_uncompressed;
  }
 for(i=0,last=0; !last; i++)
  {
   int z_error;
   unsigned long total=0, len = maxlen;
   memset(compressed,0,len); memset(uncompressed,0,blocksize);
   while(total<blocksize) /* Read a complete block */
    {
     ssize_t r=read(handle, uncompressed+total, blocksize-total);
     if(r<=0) { last=1; break; }
     total+=r;
    }
   total_uncompressed += total;
   if (total != blocksize)
    {
     last=1;
     fprintf(stderr, "Partial read (%lu bytes of %lu), padding with zeros.\n",
					total, blocksize);
    }
   if((z_error=compress2(compressed, &len, uncompressed, blocksize, Z_BEST_COMPRESSION)) != Z_OK)
    {
     fprintf(stderr, "*** Error %d compressing block %lu! (compressed=%p, len=%lu, uncompressed=%p, blocksize=%lu)\n", z_error, i, compressed,len,uncompressed,blocksize);
     goto error_free_cb_list;
    }
   if((*cbp = malloc(sizeof(struct cb_list)+len))==NULL) /* get another block */
    {
     fprintf(stderr, "*** Out of memory allocating block ptrs (virtual memory exhausted).\n");
     goto error_free_cb_list;
    }
   total_compressed+=len;
   /* Print status */
   fprintf(stderr, "Block# %5lu size %6lu -> %6lu [compression ratio %3lu%%, overall: %3Lu%%]\n", i, total, len, total>0?((len*100)/total):100,total_uncompressed>0?((total_compressed*100)/total_uncompressed):100);
   (*cbp)->size = len;
   memcpy((*cbp)->data, compressed, len);
   (*cbp)->next=NULL;
   cbp=&((*cbp)->next);
  } /* for */
 goto free_compressed;

 error_free_cb_list:
    if(cbl) { free_cb_list(cbl); cbl=NULL; i=0; }

 free_compressed:
    free(compressed);
 free_uncompressed:
    free(uncompressed);
 
 *numblocks=i;
 return cbl;
}

int main(int argc, char **argv)
{
 int in;
 unsigned long blocksize;
 struct cloop_head head;
 unsigned long numblocks;
 unsigned long long bytes_so_far;
 unsigned long i;
 struct cb_list *compressed_blocks,*cbp;

 if (argc != 3)
  {
   fprintf(stderr, "Usage: %s filename blocksize(bytes).\n",argv[0]);
   fprintf(stderr, "Use '-' as filename for stdin.\n");
   return 1;
  }

 blocksize = atoi(argv[2]);
 if (blocksize == 0 || blocksize % 512 != 0)
  {
   fprintf(stderr, "*** Blocksize must be a multiple of 512.\n");
   return 1;
  }

 if (blocksize > MAX_KMALLOC_SIZE)
  {
   fprintf(stderr, "WARNING: Blocksize %lu may be too big for a kmalloc() (%lu max).\n",blocksize,MAX_KMALLOC_SIZE);
   sleep(2);
  }

 if (sizeof(CLOOP_PREAMBLE) > CLOOP_HEADROOM)
  {
   fprintf(stderr, "*** Preamble (%u chars) > headroom (%u)\n",
			sizeof(CLOOP_PREAMBLE), CLOOP_HEADROOM);
   return 1;
  }
		
 in=strcmp(argv[1],"-")==0?dup(fileno(stdin)):open(argv[1], O_RDONLY);

 if (in < 0)
  {
   perror("Opening input");
   return 1;
  }

 compressed_blocks = create_compressed_blocks(in, blocksize, &numblocks);

 close(in);

 memset(head.preamble, 0, sizeof(head.preamble));
 memcpy(head.preamble, CLOOP_PREAMBLE, sizeof(CLOOP_PREAMBLE));
 head.block_size = htonl(blocksize);
 head.num_blocks = htonl(numblocks);

 fprintf(stderr, "Block size %lu, number of blocks %lu.\n",
         blocksize, numblocks);

 bytes_so_far = sizeof(head) + sizeof(loff_t) * (numblocks + 1);

 /* Write out head... */
 write(STDOUT_FILENO, &head, sizeof(head));

 if (!compressed_blocks) return 1;

 /* Write offsets */
 for (i=0,cbp=compressed_blocks; i < numblocks+1; i++)
  {
   loff_t tmp;
   tmp = __cpu_to_be64(bytes_so_far);
   write(STDOUT_FILENO, &tmp, sizeof(tmp));
   if(cbp) { bytes_so_far += cbp->size; cbp=cbp->next; }
  }

 /* Now write blocks and free them. */
 for (i = 0, cbp=compressed_blocks; cbp && i < numblocks; i++)
  {
   if (write(STDOUT_FILENO, cbp->data, cbp->size) != cbp->size)
    {
     perror("writing block");
     free_cb_list(compressed_blocks);
     return 1;
    }
   cbp=cbp->next;
   free(compressed_blocks); compressed_blocks=cbp;
  }
#if defined(__FreeBSD__)
 /*
  * FreeBSD requires padding to 512 byte boundary
  */
 bytes_so_far = lseek(STDOUT_FILENO, 0, SEEK_END);
 if (bytes_so_far % 512)
  {
   static char padding[512];
   off_t len = 512 - bytes_so_far % 512;

   if (write(STDOUT_FILENO, padding, len) != len)
    {
     perror("writing padding block");
     return 1;
    }
  }
#endif
 fprintf(stderr,"Done.\n");
 return 0;
}
