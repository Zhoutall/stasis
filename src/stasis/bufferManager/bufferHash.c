#include <stasis/bufferManager/bufferHash.h>

#include <stasis/bufferPool.h>
#include <stasis/doubleLinkedList.h>
#include <stasis/lhtable.h>

#include <stasis/pageHandle.h>

#include <stasis/replacementPolicy.h>
#include <stasis/bufferManager.h>
#include <stasis/page.h>
#include <assert.h>
#include <stdio.h>

//#define LATCH_SANITY_CHECKING

static struct LH_ENTRY(table) * cachedPages;

static pthread_t worker;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t readComplete = PTHREAD_COND_INITIALIZER;
static pthread_cond_t needFree     = PTHREAD_COND_INITIALIZER;

static pageid_t freeLowWater;
static pageid_t freeListLength;
static pageid_t freeCount;
static pageid_t pageCount;

static Page ** freeList;

// A page is in LRU iff !pending, !pinned
static replacementPolicy * lru;

static stasis_buffer_pool_t * stasis_buffer_pool;

static stasis_page_handle_t * page_handle;

static int running;

typedef struct LL_ENTRY(node_t) node_t;

static node_t * pageGetNode(void * page, void * ignore) {
  Page * p = page;
  return (node_t*)p->prev;
}
static void pageSetNode(void * page, node_t * n, void * ignore) {
  Page * p = page;
  p->prev = (Page *) n;
}

static inline struct Page_s ** pagePendingPtr(Page * p) {
  return ((struct Page_s **)(&((p)->next)));
}
static inline intptr_t* pagePinCountPtr(Page * p) {
  return ((intptr_t*)(&((p)->queue)));
}

#ifdef LONG_RUN

inline static void checkPageState(Page * p) {
  Page * check = LH_ENTRY(find)(cachedPages, &(p->id), sizeof(p->id));
  if(check) {
    int pending = *pagePendingPtr(p);
    int pinned  = *pagePinCountPtr(p);
    if((!pinned) && (!pending)) {
      assert(pageGetNode(p, 0));
    } else {
      assert(!pageGetNode(p,0));
    }
    int notfound = 1;
    for(pageid_t i = 0; i < freeCount; i++) {
      if(freeList[i] == p) { notfound = 0; }
    }
    assert(notfound);
  } else {
    assert(!pageGetNode(p,0));
    assert(!*pagePendingPtr(p));
    assert(!*pagePinCountPtr(p));
    int found = 0;
    for(pageid_t i = 0; i < freeCount; i++) {
      if(freeList[i] == p) { found = 1; }
    }
    assert(found);
  }
}

#else

inline static void checkPageState(Page * p) { }

#endif

inline static int tryToWriteBackPage(pageid_t page) {

  Page * p = LH_ENTRY(find)(cachedPages, &page, sizeof(page));

  if(!p) { return ENOENT; }

  assert(p->id == page);

  if(*pagePendingPtr(p) || *pagePinCountPtr(p)) {
    return EBUSY;
  }

  DEBUG("Write(%ld)\n", (long)victim->id);
  page_handle->write(page_handle, p);  /// XXX pageCleanup and pageFlushed might be heavyweight.

  assert(!stasis_dirty_page_table_is_dirty(stasis_runtime_dirty_page_table(), p));

  return 0;
}
/** You need to hold mut before calling this.

    @return the page that was just written back.  It will not be in
    lru or cachedPages after the call returns.
*/
inline static Page * writeBackOnePage() {
  Page * victim = lru->getStale(lru);
  // Make sure we have an exclusive lock on victim.
  if(!victim) return 0;

  assert(! *pagePendingPtr(victim));
  assert(! *pagePinCountPtr(victim));
#ifdef LATCH_SANITY_CHECKING
  int latched = trywritelock(victim->loadlatch,0);
  assert(latched);
#endif

  checkPageState(victim);

  lru->remove(lru, victim);

  int err= tryToWriteBackPage(victim->id);
  assert(!err);

  Page * old = LH_ENTRY(remove)(cachedPages, &(victim->id), sizeof(victim->id));
  assert(old == victim);

  stasis_page_cleanup(victim);
  // Make sure that no one mistakenly thinks this is still a live copy.
  victim->id = -1;


#ifdef LATCH_SANITY_CHECKING
  // We can release the lock since we just grabbed it to see if
  // anyone else has pinned the page...  the caller holds mut, so
  // no-one will touch the page for now.
  unlock(victim->loadlatch);
#endif

  return victim;
}

/** Returns a free page.  The page will not be in freeList,
    cachedPages or lru. */
inline static Page * getFreePage() {
  Page * ret;
  if(pageCount < MAX_BUFFER_SIZE) {
    ret = stasis_buffer_pool_malloc_page(stasis_buffer_pool);
    stasis_buffer_pool_free_page(stasis_buffer_pool, ret,-1);
    (*pagePinCountPtr(ret)) = 0;
    (*pagePendingPtr(ret)) = 0;
    pageSetNode(ret,0,0);
    pageCount++;
 } else {
    if(!freeCount) {
      ret = writeBackOnePage();
      if(!ret) {
        printf("bufferHash.c: Cannot find free page for application request.\nbufferHash.c: This should not happen unless all pages have been pinned.\nbufferHash.c: Crashing.");
        abort();
      }
    } else {
      ret = freeList[freeCount-1];
      freeList[freeCount-1] = 0;
      freeCount--;
    }
    if(freeCount < freeLowWater) {
      pthread_cond_signal(&needFree);
    }
  }
  assert(!*pagePinCountPtr(ret));
  assert(!*pagePendingPtr(ret));
  assert(!pageGetNode(ret,0));
  assert(!ret->dirty);
  return ret;
}

static void * writeBackWorker(void * ignored) {
  pthread_mutex_lock(&mut);
  while(1) {
    while(running && (freeCount == freeListLength || pageCount < MAX_BUFFER_SIZE)) {
      pthread_cond_wait(&needFree, &mut);
    }
    if(!running) { break; }
    Page * victim = writeBackOnePage();

    if(victim) {
      assert(freeCount < freeListLength);
      freeList[freeCount] = victim;
      freeCount++;
      assert(!pageGetNode(victim, 0));
      checkPageState(victim);
    } else {
      static int warned = 0;
      if(!warned) {
        printf("bufferHash.c: writeBackWorker() could not find a page to write back.\nbufferHash.c:\tThis means a significant fraction of the buffer pool is pinned.\n");
        warned = 1;
      }
      pthread_cond_wait(&needFree, &mut);
    }
  }
  pthread_mutex_unlock(&mut);
  return 0;
}

static Page * bhGetCachedPage(int xid, const pageid_t pageid) {
  pthread_mutex_lock(&mut);
  // Is the page in cache?
  Page * ret = LH_ENTRY(find)(cachedPages, &pageid, sizeof(pageid));
  if(ret) {
    checkPageState(ret);
#ifdef LATCH_SANITY_CHECKING
    int locked = tryreadlock(ret->loadlatch,0);
    assert(locked);
#endif
    if(!*pagePendingPtr(ret)) {
      if(!*pagePinCountPtr(ret) ) {
        // Then ret is in lru (otherwise it would be pending, or not cached); remove it.
        lru->remove(lru, ret);
      }
      (*pagePinCountPtr(ret))++;
      checkPageState(ret);
      assert(ret->id == pageid);
    } else {
      ret = 0;
    }
  }
  pthread_mutex_unlock(&mut);
  return ret;
}

static Page * bhLoadPageImpl_helper(int xid, const pageid_t pageid, int uninitialized, pagetype_t type) {

  // Note:  Calls to loadlatch in this function violate lock order, but
  // should be safe, since we make sure no one can have a writelock
  // before we grab the readlock.

  void* check;

  pthread_mutex_lock(&mut);

  // Is the page in cache?
  Page * ret = LH_ENTRY(find)(cachedPages, &pageid,sizeof(pageid));

  do {

    // Is the page already in memory or being read from disk?
    // (If ret == 0, then no...)
    while(ret) {
      checkPageState(ret);
      if(*pagePendingPtr(ret)) {
        pthread_cond_wait(&readComplete, &mut);
        if(ret->id != pageid) {
          ret = LH_ENTRY(find)(cachedPages, &pageid, sizeof(pageid));
        }
      } else {
#ifdef LATCH_SANITY_CHECKING
        int locked = tryreadlock(ret->loadlatch,0);
        assert(locked);
#endif
        if(! *pagePinCountPtr(ret) ) {
          // Then ret is in lru (otherwise it would be pending, or not cached); remove it.
          lru->remove(lru, ret);
        }
        (*pagePinCountPtr(ret))++;
        checkPageState(ret);
        pthread_mutex_unlock(&mut);
        assert(ret->id == pageid);
        return ret;
      }
    }

    // The page is not in cache, and is not (no longer is) pending.
    assert(!ret);

    // Remove a page from the freelist.  This may cause writeback, and release our latch.
    Page * ret2 = getFreePage();

    // Did some other thread put the page in cache for us?
    ret = LH_ENTRY(find)(cachedPages, &pageid,sizeof(pageid));

    if(!ret) {
      // No, so we're ready to add it.
      ret = ret2;
      // Esacpe from this loop.
      break;
    } else {
      // Put the page back on the free list

      // It's possible that we wrote this page back even though the
      // freeList didn't have any free space; extend free list if necessary.
      if(freeListLength == freeCount) {
        freeList = realloc(freeList, freeListLength+1);
        freeListLength++;
      }

      freeList[freeCount] = ret2;
      assert(!pageGetNode(ret2, 0));
      freeCount++;
    }
    // try again.
  } while(1);

  // Add a pending entry to cachedPages to block like-minded threads and writeback
  (*pagePendingPtr(ret)) = (void*)1;

  check = LH_ENTRY(insert)(cachedPages,&pageid,sizeof(pageid), ret);
  assert(!check);

  ret->id = pageid;

  if(!uninitialized) {

    // Now, it is safe to release the mutex; other threads won't
    // try to read this page from disk.
    pthread_mutex_unlock(&mut);

    page_handle->read(page_handle, ret, type);

    pthread_mutex_lock(&mut);

  } else {
    memset(ret->memAddr,0,PAGE_SIZE);
    *stasis_page_lsn_ptr(ret) = ret->LSN;
    assert(!ret->dirty);
//    ret->dirty = 0;
    stasis_page_loaded(ret, type);
  }
  *pagePendingPtr(ret) = 0;
  // Would remove from lru, but getFreePage() guarantees that it isn't
  // there.
  assert(!pageGetNode(ret, 0));

  assert(!(*pagePinCountPtr(ret)));
  (*pagePinCountPtr(ret))++;

#ifdef LATCH_SANITY_CHECKING
  int locked = tryreadlock(ret->loadlatch, 0);
  assert(locked);
#endif

  pthread_mutex_unlock(&mut);
  pthread_cond_broadcast(&readComplete);

  assert(ret->id == pageid);
  checkPageState (ret);
  return ret;
}

static Page * bhLoadPageImpl(int xid, const pageid_t pageid, pagetype_t type) {
  return bhLoadPageImpl_helper(xid,pageid,0, type);
}
static Page * bhLoadUninitPageImpl(int xid, const pageid_t pageid) {
  return bhLoadPageImpl_helper(xid,pageid,1,UNKNOWN_TYPE_PAGE); // 1 means dont care about preimage of page.
}


static void bhReleasePage(Page * p) {
  pthread_mutex_lock(&mut);
  checkPageState(p);
  (*pagePinCountPtr(p))--;
  if(!(*pagePinCountPtr(p))) {
    assert(!pageGetNode(p, 0));
    lru->insert(lru,p);
  }
#ifdef LATCH_SANITY_CHECKING
  unlock(p->loadlatch);
#endif
  pthread_mutex_unlock(&mut);
}
static int bhWriteBackPage(pageid_t pageid) {
  pthread_mutex_lock(&mut);
  int ret = tryToWriteBackPage(pageid);
  pthread_mutex_unlock(&mut);
  return ret;
}
static void bhForcePages() {
  page_handle->force_file(page_handle);
}
static void bhForcePageRange(pageid_t start, pageid_t stop) {
  page_handle->force_range(page_handle, start, stop);
}
static void bhBufDeinit() {
  running = 0;

  pthread_cond_signal(&needFree); // Wake up the writeback thread so it will exit.
  pthread_join(worker, 0);

  struct LH_ENTRY(list) iter;
  const struct LH_ENTRY(pair_t) * next;
  LH_ENTRY(openlist)(cachedPages, &iter);
  while((next = LH_ENTRY(readlist)(&iter))) {
    Page * p = next->value;
    assertunlocked(p->rwlatch);
    assert(0 == *pagePinCountPtr(p));
    page_handle->write(page_handle, p);
    stasis_page_cleanup(p); // normally called by writeBackOnePage()
  }
  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(cachedPages);

  free(freeList);

  lru->deinit(lru);
  stasis_buffer_pool_deinit(stasis_buffer_pool);
  page_handle->close(page_handle);
}
static void bhSimulateBufferManagerCrash() {
  running = 0;

  pthread_cond_signal(&needFree);
  pthread_join(worker, 0);

  struct LH_ENTRY(list) iter;
  const struct LH_ENTRY(pair_t) * next;
  LH_ENTRY(openlist)(cachedPages, &iter);
  while((next = LH_ENTRY(readlist)(&iter))) {
    Page * p = next->value;
    writelock(p->rwlatch,0);
    stasis_page_flushed(p); // normally, pageWrite() would call this...
    stasis_page_cleanup(p); // normally called by writeBackOnePage()
    unlock(p->rwlatch);
  }
  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(cachedPages);

  free(freeList);

  lru->deinit(lru);
  stasis_buffer_pool_deinit(stasis_buffer_pool);
  page_handle->close(page_handle);
}

void stasis_buffer_manager_hash_open(stasis_page_handle_t * h) {
  page_handle = h;
  assert(!running);

#ifdef LONG_RUN
  printf("Using expensive bufferHash sanity checking.\n");
#endif

  loadPageImpl = bhLoadPageImpl;
  loadUninitPageImpl = bhLoadUninitPageImpl;
  getCachedPageImpl = bhGetCachedPage;
  releasePageImpl = bhReleasePage;
  writeBackPage = bhWriteBackPage;
  forcePages = bhForcePages;
  forcePageRange = bhForcePageRange;
  stasis_buffer_manager_close = bhBufDeinit;
  stasis_buffer_manager_simulate_crash = bhSimulateBufferManagerCrash;

  stasis_buffer_pool = stasis_buffer_pool_init();

  lru = lruFastInit(pageGetNode, pageSetNode, 0);

  cachedPages = LH_ENTRY(create)(MAX_BUFFER_SIZE);

  freeListLength = 6 + MAX_BUFFER_SIZE / 100;
  freeLowWater  = freeListLength - 5;
  freeCount = 0;
  pageCount = 0;

  freeList = calloc(freeListLength, sizeof(Page*));

  running = 1;

  pthread_create(&worker, 0, writeBackWorker, 0);
}
