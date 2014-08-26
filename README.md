                  COMPRESSED LOOPBACK BLOCK DEVICE (cloop.ko)
=============================================================================

DISCLAIMER: THE AUTHORS HEREBY DENY ANY WARRANTY FOR ANY DATA LOSS AND
DAMAGE WHATSOEVER THAT MAY BE CAUSED DIRECTLY OR INDIRECTLY BY USING THIS
SOFTWARE. THIS IS EXPERIMENTAL SOFTWARE. USE AT YOUR OWN RISK.

This module is licensed under the GNU GENERAL PUBLIC LICENSE Version 2, as
stated in the source.

-----------------------------------------------------------------------------

This is cloop, a Kernel module to add support for filesystem-independent,
transparently decompressed, read-only block devices.

Original Author: Paul 'Rusty' Russel
Extensions, Bugfixes & newer Kernels: Klaus Knopper (http://knopper.net/knoppix/)

INSTALLATION:
-------------

make KERNEL_DIR=/path/to/linux-kernel/sources
(as root) mkdir -p /lib/modules/misc && cp cloop.o /lib/modules/misc/ 
(as root) depmod -a
(as root) mknod /dev/cloop b 240 0
(as root) mknod /dev/cloop1 b 240 1
...

USAGE:
------

Creating a compressed image:
 create_compressed_fs image blocksize > image.cloop_compressed

blocksize must be a multiple of 512  bytes. Make sure you have enough
swap to hold the entire compressed image in virtual memory! Use "-"
as filename to read data from stdin, as in:

 mkisofs -r datadir | create_compressed_fs - 65536 > datadir.iso.compressed

Mounting a compressed image (see above for device creation):
 insmod cloop.o file=/path/to/compressed/image
 mount -o ro -t whatever /dev/cloop /mnt/compressed

Starting from cloop version 1.0, setting of cloop compressed files via
losetup /dev/cloop1 /path/to/file
is possible. cloop 1.0 uses 64bit pointers instead of 32bit, so cloop files
are no longer limited to <= 4GB (uncompressed size).

cloop supports the losetup ioctls for adding and removing files via /dev/cloop*

For more information, please refer to the sources. If you don't understand
what all this is about, please DON'T EVEN ATTEMPT TO INSTALL OR USE THIS
SOFTWARE.

A download location for the currentmost version of this module and utilities
has been provided at: https://github.com/KlausKnopper/cloop

		-Klaus Knopper 26.08.2014 <cloop@knopper.net>
