/* 
 * cloop_suspend - Suspend a cloop device until losetup /dev/cloop <file> is 
 *                 run again.
 * 
 * Copyright (c) 2007 by Fabian Franz.
 *
 * License: GPL, v2.
 * 
 */

#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

/* We don't use the structure, so that define does not hurt */
#define dev_t int
#include <linux/loop.h>
#include "cloop.h"

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		fprintf(stderr, "syntax: %s <device>\n", argv[0]);
		fprintf(stderr, "        suspends and unlocks cloop <device>\n");
		fprintf(stderr, "        until a new file is loaded via losetup\n");
		return 1;
	}
	int fd = open(argv[1], O_RDONLY);

	if (fd < 0)
	{
		perror(argv[1]);
		return 1;
	}

	if (ioctl(fd, CLOOP_SUSPEND) < 0)
	{
		perror("ioctl: CLOOP_SUSPEND");
		return 1;
	}

	close(fd);

	return 0;
}
