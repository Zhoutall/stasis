#include <config.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/common.h>
#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/recovery.h>
#include <stasis/bufferManager.h>
#include <stasis/consumer.h>
#include <stasis/lockManager.h>
#include <stasis/compensations.h>
#include <stasis/pageHandle.h>
#include <stasis/page.h>

#include <stasis/bufferManager/legacy/pageFile.h>


#include <stasis/logger/logger2.h>
#include <stasis/truncation.h>
#include <stasis/io/handle.h>
#include <stasis/blobManager.h> // XXX remove this, move Tread() to set.c
#include <stdio.h>
#include <assert.h>
#include <limits.h>

static TransactionLog XactionTable[MAX_TRANSACTIONS];
static int numActiveXactions = 0;
static int xidCount = 0;

static int initted = 0;

const recordid ROOT_RECORD = {1, 0, -1};
const recordid NULLRID = {0,0,-1};
const short SLOT_TYPE_LENGTHS[] = { 0, 0, sizeof(blob_record_t), -1};
/** 
    Locking for transactional2.c works as follows:
    
    numActiveXactions, xidCount are protected, XactionTable is not.
    This implies that we do not support multi-threaded transactions,
    at least for now.
*/
pthread_mutex_t transactional_2_mutex;

#define INVALID_XTABLE_XID INVALID_XID
#define PENDING_XTABLE_XID (-2)
/** Needed for debugging -- sometimes we don't want to run all of Tinit() */

void setupOperationsTable() {
	memset(XactionTable, INVALID_XTABLE_XID, sizeof(TransactionLog)*MAX_TRANSACTIONS);
	// @todo clean out unused constants...
	operationsTable[OPERATION_SET]       = getSet();
	operationsTable[OPERATION_SET_INVERSE] = getSetInverse();
	operationsTable[OPERATION_INCREMENT] = getIncrement();
	operationsTable[OPERATION_DECREMENT] = getDecrement();
	operationsTable[OPERATION_ALLOC]     = getAlloc();
	//	operationsTable[OPERATION_PREPARE]   = getPrepare();
	/*	operationsTable[OPERATION_LHINSERT]  = getLHInsert(); 
		operationsTable[OPERATION_LHREMOVE]  = getLHRemove(); */
	operationsTable[OPERATION_DEALLOC]     = getDealloc();
	operationsTable[OPERATION_REALLOC]     = getRealloc();

	operationsTable[OPERATION_PAGE_SET_RANGE] = getPageSetRange();
	operationsTable[OPERATION_PAGE_SET_RANGE_INVERSE] = getPageSetRangeInverse();

	/*	operationsTable[OPERATION_UPDATE_FREESPACE]         = getUpdateFreespace();
	operationsTable[OPERATION_UPDATE_FREESPACE_INVERSE] = getUpdateFreespaceInverse();
	operationsTable[OPERATION_UPDATE_FREELIST]          = getUpdateFreelist();
	operationsTable[OPERATION_UPDATE_FREELIST_INVERSE] = getUpdateFreelistInverse();
	
	operationsTable[OPERATION_FREE_PAGE] = getFreePageOperation();
	operationsTable[OPERATION_ALLOC_FREED] = getAllocFreedPage();
	operationsTable[OPERATION_UNALLOC_FREED] = getUnallocFreedPage(); */
	operationsTable[OPERATION_NOOP] = getNoop();

	operationsTable[OPERATION_ARRAY_LIST_ALLOC]  = getArrayListAlloc();
	operationsTable[OPERATION_INITIALIZE_PAGE] = getInitializePage();

	operationsTable[OPERATION_SET_RANGE] = getSetRange();
	operationsTable[OPERATION_SET_RANGE_INVERSE] = getSetRangeInverse();
	
	operationsTable[OPERATION_LINKED_LIST_INSERT] = getLinkedListInsert();
	operationsTable[OPERATION_LINKED_LIST_REMOVE] = getLinkedListRemove();

	operationsTable[OPERATION_LINEAR_HASH_INSERT] = getLinearHashInsert();
	operationsTable[OPERATION_LINEAR_HASH_REMOVE] = getLinearHashRemove();
	
	//operationsTable[OPERATION_SET_RAW] = getSetRaw();
	//operationsTable[OPERATION_INSTANT_SET_RAW] = getInstantSetRaw();

	operationsTable[OPERATION_ALLOC_BOUNDARY_TAG] = getAllocBoundaryTag();

	operationsTable[OPERATION_FIXED_PAGE_ALLOC] = getFixedPageAlloc();

	operationsTable[OPERATION_ALLOC_REGION] = getAllocRegion();
	operationsTable[OPERATION_ALLOC_REGION_INVERSE] = getAllocRegionInverse();

	operationsTable[OPERATION_DEALLOC_REGION] = getDeallocRegion();
	operationsTable[OPERATION_DEALLOC_REGION_INVERSE] = getDeallocRegionInverse();

}

// @todo this factory stuff doesn't really belong here...
static stasis_handle_t * fast_factory(lsn_t off, lsn_t len, void * ignored) { 
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);
  return h;
}
typedef struct sf_args {
  char * filename;
  int    openMode;
  int    filePerm;
} sf_args;
static stasis_handle_t * slow_file_factory(void * argsP) { 
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
static stasis_handle_t * slow_pfile_factory(void * argsP) {
  stasis_handle_t * h = argsP;
  return h;
}
static int (*slow_close)(stasis_handle_t * h) = 0;
static stasis_handle_t * slow_pfile = 0;
static int nop_close(stasis_handle_t*h) { return 0; }

int Tinit() {
        pthread_mutex_init(&transactional_2_mutex, NULL);
        initted = 1;
	numActiveXactions = 0;

	compensations_init();

        setupOperationsTable();
	dirtyPagesInit();
	LogInit(loggerType);
	stasis_page_init();

#ifndef HAVE_O_DIRECT
	if(bufferManagerO_DIRECT) {
	  printf("O_DIRECT not supported by this build; switching to conventional buffered I/O.\n");
	  bufferManagerO_DIRECT = 0;
	}
#endif
	int openMode;
	if(bufferManagerO_DIRECT) {
#ifdef HAVE_O_DIRECT
	  openMode = O_CREAT | O_RDWR | O_DIRECT;
#else
              printf("Can't happen\n");
              abort();
#endif
	} else {
	  openMode = O_CREAT | O_RDWR;
	}

	/// @todo remove hardcoding of buffer manager implementations in transactional2.c

        switch(bufferManagerFileHandleType) {
          case BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING: {
            struct sf_args * slow_arg = malloc(sizeof(sf_args));
            slow_arg->filename = STORE_FILE;

	    slow_arg->openMode = openMode;

            slow_arg->filePerm = FILE_PERM;
            // Allow 4MB of outstanding writes.
            // @todo Where / how should we open storefile?
            stasis_handle_t * pageFile;
            int worker_thread_count = 4;
            if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_PFILE) {
              //              printf("\nusing pread()/pwrite()\n");
              slow_pfile = stasis_handle_open_pfile(0, slow_arg->filename, slow_arg->openMode, slow_arg->filePerm);
              slow_close = slow_pfile->close;
              slow_pfile->close = nop_close;
              pageFile =
		stasis_handle(open_non_blocking)(slow_pfile_factory, slow_pfile, 1, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024 , 1024);

            } else if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_FILE) {
              pageFile =
		stasis_handle(open_non_blocking)(slow_file_factory, slow_arg, 0, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024, 1024);
            } else {
              printf("Unknown value for config option bufferManagerNonBlockingSlowHandleType\n");
              abort();
            }
            //pageFile = stasis_handle(open_debug)(pageFile);
            pageHandleOpen(pageFile);
          } break;
	  case BUFFER_MANAGER_FILE_HANDLE_FILE: {
	    stasis_handle_t * pageFile =
	      stasis_handle_open_file(0, STORE_FILE, openMode, FILE_PERM);
	    pageHandleOpen(pageFile);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_PFILE: {
	    stasis_handle_t * pageFile =
	      stasis_handle_open_pfile(0, STORE_FILE, openMode, FILE_PERM);
	    pageHandleOpen(pageFile);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_DEPRECATED: { 
            printf("\nWarning: Using old I/O routines (with known bugs).\n");
            openPageFile();
          } break;
          default: {
            printf("\nUnknown buffer manager filehandle type: %d\n",
                   bufferManagerFileHandleType);
            abort();
          }
        }
	bufInit(bufferManagerType);
        DEBUG("Buffer manager type = %d\n", bufferManagerType);
	pageOperationsInit();
	TallocInit();
	TnaiveHashInit();
	LinearHashNTAInit();
	LinkedListNTAInit();
	iterator_init();
	consumer_init();
	setupLockManagerCallbacksNil();
	//setupLockManagerCallbacksPage();
	
	InitiateRecovery();
	
	truncationInit();
	if(stasis_truncation_automatic) {
	  autoTruncate(); // should this be before InitiateRecovery?
	}
	return 0;
}


int Tbegin() {

	int i, index = 0;
	int xidCount_tmp;

        assert(initted);

	pthread_mutex_lock(&transactional_2_mutex);

	if( numActiveXactions == MAX_TRANSACTIONS ) {
	  pthread_mutex_unlock(&transactional_2_mutex);
	  return EXCEED_MAX_TRANSACTIONS;
	}
	else {
          DEBUG("%s:%d activate in begin\n",__FILE__,__LINE__);
          numActiveXactions++;
        }
	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		xidCount++;
		if( XactionTable[xidCount%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
			index = xidCount%MAX_TRANSACTIONS;
			break;
		}
	}

	xidCount_tmp = xidCount;

	XactionTable[index].xid = PENDING_XTABLE_XID;

	pthread_mutex_unlock(&transactional_2_mutex);

	XactionTable[index] = LogTransBegin(xidCount_tmp);

	if(globalLockManager.begin) { globalLockManager.begin(XactionTable[index].xid); }

	return XactionTable[index].xid;
}

static compensated_function void TactionHelper(int xid,
					       const void * dat, size_t datlen, int op,
					       Page * p) {
  LogEntry * e;
  assert(xid >= 0 && XactionTable[xid % MAX_TRANSACTIONS].xid == xid);
  try { 
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, p->id);
    }
  } end;

  writelock(p->rwlatch,0);

  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, op, dat, datlen);
  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
  DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
  doUpdate(e, p);
  FreeLogEntry(e);

  unlock(p->rwlatch);
}

compensated_function void TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op) {
  Tupdate(xid, page, dat, datlen, op);
}

compensated_function void Tupdate(int xid, pageid_t page, 
				  const void *dat, size_t datlen, int op) { 
  Page * p = loadPage(xid, page);
  assert(initted);
  TactionHelper(xid, dat, datlen, op, p);
  releasePage(p);
}

compensated_function void TreadStr(int xid, recordid rid, char * dat) {
  Tread(xid, rid, dat);
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;

  readlock(p->rwlatch,0);

  rid = stasis_record_dereference(xid, p, rid);
  if(rid.page != p->id) { 
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid, rid.page);
    readlock(p->rwlatch,0);
  }
  short type = stasis_record_type_read(xid,p,rid);
  if(type == BLOB_SLOT) {
    DEBUG("call readBlob %lld %lld %lld\n", (long long)rid.page, (long long)rid.slot, (long long)rid.size);
    readBlob(xid,p,rid,dat);
    assert(rid.page == p->id);
  } else {
    stasis_record_read(xid, p, rid, dat);
  }
  unlock(p->rwlatch);
  releasePage(p);
}

compensated_function void TreadRaw(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;
  readlock(p->rwlatch,0);
  stasis_record_read(xid, p, rid, dat);
  unlock(p->rwlatch);
  releasePage(p);
}

int Tcommit(int xid) {
  lsn_t lsn;
  assert(xid >= 0);
#ifdef DEBUGGING 
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif

  lsn = LogTransCommit(&XactionTable[xid % MAX_TRANSACTIONS]);
  if(globalLockManager.commit) { globalLockManager.commit(xid); }

  allocTransactionCommit(xid);

  pthread_mutex_lock(&transactional_2_mutex);

  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  DEBUG("%s:%d deactivate %d\n",__FILE__,__LINE__,xid);
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  pthread_mutex_unlock(&transactional_2_mutex);

  return 0;
}

int Tprepare(int xid) {
  assert(xid >= 0);
  off_t i = xid % MAX_TRANSACTIONS;
  assert(XactionTable[i].xid == xid);
  LogTransPrepare(&XactionTable[i]);
  return 0;
}

int Tabort(int xid) {
  lsn_t lsn;
  assert(xid >= 0);

  TransactionLog * t =&XactionTable[xid%MAX_TRANSACTIONS];
  assert(t->xid == xid);

  lsn = LogTransAbort(t);

  /** @todo is the order of the next two calls important? */
  undoTrans(*t);
  if(globalLockManager.abort) { globalLockManager.abort(xid); }

  allocTransactionAbort(xid);

  pthread_mutex_lock(&transactional_2_mutex);

  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  pthread_mutex_unlock(&transactional_2_mutex);
  return 0;
}

int Tdeinit() {
  int i;

  for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
    if( XactionTable[i].xid != INVALID_XTABLE_XID ) {
      if(!stasis_suppress_unclean_shutdown_warnings) {
	fprintf(stderr, "WARNING: Tdeinit() is aborting transaction %d\n",
		XactionTable[i].xid);
      }
      Tabort(XactionTable[i].xid);
    }
  }
  assert( numActiveXactions == 0 );
  truncationDeinit();
  TnaiveHashDeinit();
  TallocDeinit();
  bufDeinit();
  DEBUG("Closing page file tdeinit\n");
  closePageFile();
  if(slow_pfile) {
    slow_close(slow_pfile);
    slow_pfile = 0;
    slow_close = 0;
  }
  stasis_page_deinit();
  LogDeinit();
  dirtyPagesDeinit();

  initted = 0;

  return 0;
}

int TuncleanShutdown() {
  // We're simulating a crash; don't complain when writes get lost,
  // and active transactions get rolled back.
  stasis_suppress_unclean_shutdown_warnings = 1;
  truncationDeinit();
  TnaiveHashDeinit();
  simulateBufferManagerCrash();
  if(slow_pfile) {
    slow_close(slow_pfile);
    slow_pfile = 0;
    slow_close = 0;
  }
  stasis_page_deinit();
  LogDeinit();
  numActiveXactions = 0;
  dirtyPagesDeinit();

  // Reset it here so the warnings will appear if a new stasis
  // instance encounters problems during a clean shutdown.
  stasis_suppress_unclean_shutdown_warnings = 0;
  return 0;
}

void stasis_transaction_table_max_transaction_id_set(int xid) {
  pthread_mutex_lock(&transactional_2_mutex);
  xidCount = xid;
  pthread_mutex_unlock(&transactional_2_mutex);
}
void stasis_transaction_table_active_transaction_count_set(int xid) {
  pthread_mutex_lock(&transactional_2_mutex);
  numActiveXactions = xid;
  pthread_mutex_unlock(&transactional_2_mutex);
}

lsn_t transactions_minRecLSN() { 
  lsn_t minRecLSN = LSN_T_MAX;
  pthread_mutex_lock(&transactional_2_mutex);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) { 
    if(XactionTable[i].xid != INVALID_XTABLE_XID) { 
      lsn_t recLSN = XactionTable[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) { 
	minRecLSN = recLSN;
      }
    }
  }
  pthread_mutex_unlock(&transactional_2_mutex);
  return minRecLSN;
}

int TactiveTransactionCount() {
  return numActiveXactions;
}

int* TlistActiveTransactions() {
  pthread_mutex_lock(&transactional_2_mutex);
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(XactionTable[i].xid != INVALID_XTABLE_XID) {
      ret[retcount] = XactionTable[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = 0;
    }
  }
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}
int TisActiveTransaction(int xid) { 
  if(xid < 0) { return 0; }
  pthread_mutex_lock(&transactional_2_mutex);
  int ret = xid != INVALID_XTABLE_XID && XactionTable[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}

int stasis_transaction_table_roll_forward(int xid, lsn_t lsn, lsn_t prevLSN) {
  TransactionLog * l = &XactionTable[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    pthread_mutex_lock(&transactional_2_mutex);
    assert(l->xid == INVALID_XTABLE_XID);
    l->xid = xid;
    l->recLSN = lsn;
    numActiveXactions++;
    pthread_mutex_unlock(&transactional_2_mutex);
  }
  l->prevLSN = lsn;
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(XactionTable[xid%MAX_TRANSACTIONS].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(xid, lsn, prevLSN);
}
int stasis_transaction_table_forget(int xid) {
  assert(xid != INVALID_XTABLE_XID);
  TransactionLog * l = &XactionTable[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    pthread_mutex_lock(&transactional_2_mutex);
    l->xid = INVALID_XTABLE_XID;
    l->prevLSN = -1;
    l->recLSN = -1;
    numActiveXactions--;
    assert(numActiveXactions >= 0);
    pthread_mutex_unlock(&transactional_2_mutex);
  } else {
    assert(l->xid == INVALID_XTABLE_XID);
  }
  return 0;
}

int TdurabilityLevel() {
  if(bufferManagerType == BUFFER_MANAGER_MEM_ARRAY) { 
    return VOLATILE;
  } else if(loggerType == LOG_TO_MEMORY) { 
    return PERSISTENT;
  } else { 
    return DURABLE;
  }
}

typedef struct {
  lsn_t prev_lsn;
  lsn_t compensated_lsn;
} stasis_nta_handle;

/** @todo TbeginNestedTopAction's API might not be quite right.
    Are there cases where we need to pass a recordid in?

    @return a handle that must be passed into TendNestedTopAction
*/
void * TbeginNestedTopAction(int xid, int op, const byte * dat, int datSize) {
  assert(xid >= 0);
  LogEntry * e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], NULL, op, dat, datSize);
  DEBUG("Begin Nested Top Action e->LSN: %ld\n", e->LSN);
  stasis_nta_handle * h = malloc(sizeof(stasis_nta_handle));

  h->prev_lsn = e->prevLSN;
  h->compensated_lsn = e->LSN;

  FreeLogEntry(e);
  return h;
}

/**
    Call this function at the end of a nested top action.
    @return the lsn of the CLR.  Most users (everyone?) will ignore this.
*/
lsn_t TendNestedTopAction(int xid, void * handle) {
  stasis_nta_handle * h = handle;
  assert(xid >= 0);

  // Write a CLR.
  lsn_t clrLSN = LogDummyCLR(xid, h->prev_lsn, h->compensated_lsn);

  // Ensure that the next action in this transaction points to the CLR.
  XactionTable[xid % MAX_TRANSACTIONS].prevLSN = clrLSN;

  DEBUG("NestedTopAction CLR %d, LSN: %ld type: %ld (undoing: %ld, next to undo: %ld)\n", e->xid, 
	 clrLSN, undoneLSN, *prevLSN);

  free(h);

  return clrLSN;
}
