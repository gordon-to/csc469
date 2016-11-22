#define _GNU_SOURCE

#include <sched.h>
#include <stdlib.h>
#include <strings.h>
#include "malloc.h"
#include "memlib.h"
#include "mm_thread.h"

name_t myname = {
  /* team name to be displayed on webpage */
  "nodejs is the only real dev language",
  /* Full name of first team member */
  "Eugene Yue-Hin Cheung",
  /* Email address of first team member */
  "ey.cheung@mail.utoronto.ca",
  /* Full name of second team member */
  "Eric Snyder",
  /* Email address of second team member */
  "eric.snyder@mail.utoronto.ca"
};

/* CONSTANTS */

#define NSIZES 9 // Size clases
#define NBINS 4  // Fullness groups (~ 0-24%, 25-49%, 50-74%, 75-100%)
#define K 8      // Threshold (TODO: use threshold per size class?)

// Lower threshold leads to a larger number of superblocks being transferred to the
// global heap (which affects scalability), but higher threshold increases blowup
// (fragmentation)

/* STRUCTS */

typedef struct superblock
{
  pthread_mutex_t lock;
  int owner;        // Owner heap ID
  int sz_class_idx; // Index of corresponding size class in SIZES
  int used;         // In bytes (TODO: consider using something smaller since upper bound is 4096)
  int bitmap;       // Bitmap to track used blocks (TODO: consider using char bitmap[64])
  struct superblock *prev;
  struct superblock *next;
} superblock_t;

typedef struct heap
{
  pthread_mutex_t lock;
  int allocated;                     // In bytes
  int used;                          // In bytes
  superblock_t *bins[NSIZES][NBINS]; // For every size class, have bins for the fullness groups
} heap_t;

/* GLOBALS (should be < 1K) */

// Size classes
static const size_t SIZES[NSIZES] = {8, 16, 32, 64, 128, 256, 512, 1024, 2048};

// Page size
static int page_size;

// Heaps
static heap_t **heaps;

// Global lock
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;

// ========================================================================= //
//                             HELPER FUNCTIONS                              //
// ========================================================================= //

/**
 * "Hashes the current thread", for determining which heap to use.
 *
 * We're actually just checking which CPU it's running on:
 * https://piazza.com/class/isb6pxw9i2n73q?cid=109
 */
int hash()
{
  return sched_getcpu();
}

void lock_heap(int heap)
{
  pthread_mutex_lock(&(heaps[heap]->lock));
}

void unlock_heap(int heap)
{
  pthread_mutex_unlock(&(heaps[heap]->lock));
}

void lock_superblock(superblock_t *sb)
{
  pthread_mutex_lock(&(sb->lock));
}

int trylock_superblock(superblock_t *sb)
{
  return pthread_mutex_trylock(&(sb->lock));
}

void unlock_superblock(superblock_t *sb)
{
  pthread_mutex_unlock(&(sb->lock));
}

/**
 * Returns the index of the first empty block in a superblock,
 * or -1 if none exist.
 */
int find_block(superblock_t *sb)
{
  int blocks = page_size / SIZES[sb->sz_class_idx];

  int i;
  for (i = 0; i < blocks; i++)
  {
    if ((sb->bitmap & (1 << i)) == 0)
    {
      return i;
    }
  }

  return -1;
}

/**
 * Determines which fullness bin (index) should be used.
 */
int fullness_bin(int used)
{
  float fullness = (float)used / page_size;
  int bin = (int)(fullness * NBINS);
  return bin >= NBINS ? NBINS - 1 : bin;
}

/**
 * Returns the appropriate index in SIZES for the given size, or -1 if it's too
 * large for any of the possible sizes.
 */
int get_sz_class(size_t sz)
{
  int sz_class_idx = -1;

  int i;
  for (i = 0; i < NSIZES; i++)
  {
    if (sz <= SIZES[i])
    {
      sz_class_idx = i;
      break;
    }
  }

  return sz_class_idx;
}

/**
 * Moves a superblock to the beginning of the appropriate bin if needed and/or
 * a different heap.
 */
void transfer_superblock(superblock_t *sb, int new_heap, int old_used)
{
  pthread_mutex_lock(&(global_lock));

  int old_heap = sb->owner;

  int old_bin = fullness_bin(old_used);
  int new_bin = fullness_bin(sb->used);

  if (old_bin != new_bin || new_heap != old_heap)
  {
    // Detach from linked list
    if (sb->next)
    {
      sb->next->prev = sb->prev;
    }

    if (sb->prev)
    {
      sb->prev->next = sb->next;
    }
    else
    {
      // If prev is NULL, then it's the head of the bin
      heaps[old_heap]->bins[sb->sz_class_idx][old_bin] = sb->next;
    }

    // Insert as head of new bin
    superblock_t *old_head = heaps[new_heap]->bins[sb->sz_class_idx][new_bin];
    if (old_head)
    {
      old_head->prev = sb;
    }
    sb->prev = NULL;
    sb->next = old_head;
    heaps[new_heap]->bins[sb->sz_class_idx][new_bin] = sb;

    // Update usage counts if transferring between heaps
    if (old_heap != new_heap)
    {
      sb->owner = new_heap;
      heaps[old_heap]->used -= sb->used;
      heaps[old_heap]->allocated -= page_size;
      heaps[new_heap]->used += sb->used;
      heaps[new_heap]->allocated += page_size;
    }
  }

  pthread_mutex_unlock(&(global_lock));
}

/**
 * Initializes and returns the pointer to a new superblock.
 */
superblock_t *new_superblock(int heap, int sz_class_idx)
{
  // Allocate S bytes as superblock
  pthread_mutex_lock(&(global_lock));
  superblock_t *sb = (superblock_t *)mem_sbrk(page_size);
  pthread_mutex_unlock(&(global_lock));

  if (!sb)
  {
    fprintf(stderr, "[ERROR] new_superblock: failed to mem_sbrk\n");
    return NULL;
  }

  pthread_mutex_init(&(sb->lock), NULL);
  sb->owner = heap;
  sb->sz_class_idx = sz_class_idx;
  sb->bitmap = 0;

  // Mark beginning bits for this superblock metadata
  int metadata_size = sizeof(superblock_t);
  int sz_class = SIZES[sz_class_idx];
  int blocks_used = metadata_size / sz_class + (metadata_size % sz_class != 0); // ceil

  int i;
  for (i = 0; i < blocks_used; i++)
  {
    sb->bitmap |= 1 << i;
  }

  heaps[heap]->allocated += page_size;
  heaps[heap]->used += blocks_used * sz_class;
  sb->used = blocks_used * sz_class;

  // Add to beginning of fullness bin
  int bin = fullness_bin(sb->used);

  superblock_t *old_head = heaps[heap]->bins[sz_class_idx][bin];
  if (old_head)
  {
    old_head->prev = sb;
  }
  sb->prev = NULL;
  sb->next = old_head;
  heaps[heap]->bins[sz_class_idx][bin] = sb;

  return sb;
}

// ========================================================================= //
//                           MALLOC / FREE / INIT                            //
// ========================================================================= //

void *mm_malloc(size_t sz)
{
#ifndef NDEBUG
  fprintf(stderr, "> malloc: %zu\n", sz);
#endif

  if (sz == 0)
  {
    return NULL;
  }

  // Just malloc for "large" allocations.
  // https://piazza.com/class/isb6pxw9i2n73q?cid=105
  if (sz > SIZES[NSIZES - 1])
  {
    return malloc(sz);
  }

  // Add 1 to skip over global heap
  int i = hash() + 1;

  lock_heap(i);

  superblock_t *sb = NULL;
  superblock_t *existing_sb = NULL;
  int j;

  // Determine size class
  int sz_class_idx = get_sz_class(sz);
  size_t sz_class = SIZES[sz_class_idx];

  // Scan heap i's list of (free-ish) superblocks from most full to least
  for (j = NBINS - 2; j >= 0; j--)
  {
    existing_sb = heaps[i]->bins[sz_class_idx][j];
    while (existing_sb)
    {
      if (find_block(existing_sb) != -1)
      {
        // Found block with enough free space
        if (trylock_superblock(existing_sb) == 0)
        {
          sb = existing_sb;
          break;
        }
      }

      existing_sb = existing_sb->next;
    }

    if (sb)
    {
      break;
    }
  }

  // No superblock in heap i with free space
  if (!sb)
  {
    lock_heap(0);

    // Check global heap for an empty-ish superblock
    for (j = 0; j < NBINS - 1; j++)
    {
      existing_sb = heaps[0]->bins[sz_class_idx][j];
      if (existing_sb && find_block(existing_sb) != -1)
      {
        if (trylock_superblock(existing_sb) == 0)
        {
          // Found an empty superblock: transfer to heap i
          sb = existing_sb;
          transfer_superblock(sb, i, sb->used);
          break;
        }
      }
    }

    unlock_heap(0);

    // No superblock found in global heap (or somehow got a superblock with no space)
    if (!sb)
    {
#ifndef NDEBUG
      fprintf(stderr, "> malloc: new superblock\n");
#endif

      sb = new_superblock(i, sz_class_idx);
      if (!sb)
      {
        unlock_heap(i);
        return NULL;
      }

      lock_superblock(sb);
    }
  }

  // Find a free block and mark it as used
  int block_idx = find_block(sb);
  if (block_idx == -1)
  {
    fprintf(stderr, "[ERROR] malloc: chose superblock with no free block\n");
    unlock_superblock(sb);
    unlock_heap(i);
    return NULL;
  }

  sb->bitmap |= 1 << block_idx;

  int old_used = sb->used;

  heaps[i]->used += sz_class;
  sb->used += sz_class;

  // Move to appropriate fullness bin
  transfer_superblock(sb, i, old_used);

  unlock_superblock(sb);
  unlock_heap(i);

  // Return pointer to specific block
  return (void *)sb + (block_idx * sz_class);
}

void mm_free(void *ptr)
{
#ifndef NDEBUG
  fprintf(stderr, "< free\n");
#endif

  if (!ptr)
  {
    return;
  }

  // If block is "large", then the pointer won't be within our originally
  // allocated range
  // https://piazza.com/class/isb6pxw9i2n73q?cid=115
  if (ptr < (void *)dseg_lo || ptr > (void *)dseg_hi)
  {
    free(ptr);
    return;
  }

  // Move up to closest page border for superblock header
  unsigned long offset = (unsigned long)ptr % page_size;
  superblock_t *sb = (superblock_t *)((unsigned long)ptr - offset);

  lock_superblock(sb);

  // Lock superblock's owner
  int i = sb->owner;
  lock_heap(i);

  size_t sz_class = SIZES[sb->sz_class_idx];

  // Deallocate block from superblock
  memset(ptr, 0, sz_class);

  // Mark block as free in bitmap
  int block_idx = offset / sz_class;
  sb->bitmap &= ~(1 << block_idx);

  int old_used = sb->used;

  heaps[i]->used -= sz_class;
  sb->used -= sz_class;

  // Move to appropriate fullness bin
  transfer_superblock(sb, sb->owner, old_used);

  if (i == 0)
  {
    unlock_heap(i);
    unlock_superblock(sb);
    return;
  }

  // TODO: experiment with value of f
  float f = 1 / (float)NBINS;
  if (heaps[i]->used < heaps[i]->allocated - K * page_size && heaps[i]->used < (1 - f) * heaps[i]->allocated)
  {
    // Find and transfer a mostly-empty superblock to heap 0
    int j;
    for (j = 0; j < NSIZES; j++)
    {
      // Just check the top of the fullness bins for an empty-ish one
      if (heaps[i]->bins[j][0] && trylock_superblock(heaps[i]->bins[j][0]) == 0)
      {
        superblock_t *s1 = heaps[i]->bins[j][0];

#ifndef NDEBUG
        fprintf(stderr, "< free: Transfer s1 to heap 0 (from %d)\n", s1->owner);
#endif

        // Transfer to heap 0
        lock_heap(0);

        transfer_superblock(s1, 0, s1->used);

        unlock_superblock(s1);
        unlock_heap(0);
        break;
      }
    }
  }

  unlock_heap(i);
  unlock_superblock(sb);
}

int mm_init(void)
{
  if (mem_init() == -1)
  {
    fprintf(stderr, "[ERROR] init: Failed to mem_init\n");
    return -1;
  }

  int cpus = getNumProcessors();
  page_size = mem_pagesize();

#ifndef NDEBUG
  fprintf(stderr, "init: %d CPUs, %d page size\n", cpus, page_size);
#endif

  // Initialize heaps
  heaps = (heap_t **)mem_sbrk(page_size);

  if (!heaps)
  {
    fprintf(stderr, "[ERROR] init: Failed to mem_sbrk heaps\n");
    return -1;
  }

  // 0 -> cpus (0 is global, rest are for each CPU)
  int i;
  for (i = 0; i <= cpus; i++)
  {
    heaps[i] = (heap_t *)mem_sbrk(page_size);

    pthread_mutex_init(&(heaps[i]->lock), NULL);
    heaps[i]->allocated = 0;
    heaps[i]->used = 0;
    memset(heaps[i]->bins, 0, sizeof(superblock_t *) * NSIZES * NBINS);
  }

  return 0;
}
