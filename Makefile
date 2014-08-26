#!/usr/bin/make

MACHINE=$(shell uname -m)
ifndef KERNEL_DIR
KERNEL_DIR:=/lib/modules/`uname -r`/build
endif

file_exist=$(shell test -f $(1) && echo yes || echo no)

# test for 2.6 or 2.4 kernel
ifeq ($(call file_exist,$(KERNEL_DIR)/Rules.make), yes)
PATCHLEVEL:=4
else
PATCHLEVEL:=6
endif

ifdef APPSONLY
CFLAGS:=-Wall -Wstrict-prototypes -Wno-trigraphs -O2 -s -I. -fno-strict-aliasing -fno-common -fomit-frame-pointer 
endif

KERNOBJ:=cloop.o

# Name of module
ifeq ($(PATCHLEVEL),6)
MODULE:=cloop.ko
else
MODULE:=cloop.o
endif

ALL_TARGETS = create_compressed_fs extract_compressed_fs
ifndef APPSONLY
ALL_TARGETS += $(MODULE)
endif

all: $(ALL_TARGETS)

module: $(MODULE)

utils: create_compressed_fs extract_compressed_fs

# For Kernel >= 2.6, we now use the "recommended" way to build kernel modules
obj-m := cloop.o
# cloop-objs := cloop.o

$(MODULE): cloop.c cloop.h
	@echo "Building for Kernel Patchlevel $(PATCHLEVEL)"
	$(MAKE) modules -C $(KERNEL_DIR) M=$(CURDIR)

create_compressed_fs: advancecomp-1.15/advfs
	ln -f $< $@

advancecomp-1.15/advfs:
	( cd advancecomp-1.15 ; ./configure && $(MAKE) advfs )

extract_compressed_fs: extract_compressed_fs.c
	$(CC) -Wall -O2 -s -o $@ $< -lz

cloop_suspend: cloop_suspend.o
	$(CC) -Wall -O2 -s -o $@ $<

clean:
	rm -rf create_compressed_fs extract_compressed_fs zoom *.o *.ko Module.symvers .cloop* .compressed_loop.* .tmp*
	[ -f advancecomp-1.15/Makefile ] && $(MAKE) -C advancecomp-1.15 distclean || true

dist: clean
	cd .. ; \
	tar -cf - cloop/{Makefile,*.[ch],CHANGELOG,README} | \
	bzip2 -9 > $(HOME)/redhat/SOURCES/cloop.tar.bz2
