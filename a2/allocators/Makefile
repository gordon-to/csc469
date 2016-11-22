CC = gcc

CC_FLAGS = -std=gnu99 -c -Wall -fmessage-length=0 -pipe -O3 -finline-limit=65000 -fkeep-inline-functions -finline-functions -ffast-math -fomit-frame-pointer -DNDEBUG -I. -I$(TOPDIR)/include -D_REENTRANT=1

CC_DBG_FLAGS = -c -Wall -fmessage-length=0 -pipe -g -finline-limit=65000 -fkeep-inline-functions -finline-functions -ffast-math -fomit-frame-pointer -I. -I$(TOPDIR)/include -D_REENTRANT=1

all: libkheap libmmlibc liba2alloc

debug: libkheap_dbg liba2alloc_dbg

alloclibs:
	mkdir alloclibs

# Library containing mm_malloc and mm_free for OS/161 kheap-based allocator

libkheap: alloclibs
	cd kheap; $(CC) $(CC_FLAGS) kheap.c; ar rs ../alloclibs/libkheap.a kheap.o

libkheap_dbg: alloclibs
	cd kheap; $(CC) $(CC_DBG_FLAGS) kheap.c; ar rs ../alloclibs/libkheap_dbg.a kheap.o

# Library containing mm_malloc and mm_free for student a2 solution

liba2alloc: alloclibs
	cd a2alloc; $(CC) $(CC_FLAGS) a2alloc.c; ar rs ../alloclibs/liba2alloc.a a2alloc.o

liba2alloc_dbg: alloclibs
	cd a2alloc; $(CC) $(CC_DBG_FLAGS) a2alloc.c; ar rs ../alloclibs/liba2alloc_dbg.a a2alloc.o


# Library containing mm_malloc and mm_free wrappers for libc allocator
libmmlibc:	alloclibs
	cd libc; $(CC) $(CC_FLAGS) libc_wrapper.c; ar rs ../alloclibs/libmmlibc.a libc_wrapper.o

clean:
	rm -rf alloclibs; rm -f */*.o; rm -f *~
