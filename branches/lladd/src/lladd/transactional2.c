#include <lladd/transactional.h>
#include <lladd/constants.h>
#include <lladd/logger/logger2.h>
#include <lladd/bufferManager.h>
#include <lladd/recovery2.h>
#include <string.h>
#include <assert.h>

TransactionLog XactionTable[MAX_TRANSACTIONS];
int numActiveXactions = 0;
int xidCount = 0;
#define INVALID_XTABLE_XID -1

int Tinit() {

	memset(XactionTable, INVALID_XTABLE_XID, sizeof(TransactionLog)*MAX_TRANSACTIONS);
	operationsTable[OPERATION_SET]       = getSet();
	operationsTable[OPERATION_INCREMENT] = getIncrement();
	operationsTable[OPERATION_DECREMENT] = getDecrement();
	operationsTable[OPERATION_PREPARE]   = getPrepare();
	operationsTable[OPERATION_LHINSERT]  = getLHInsert();
	operationsTable[OPERATION_LHREMOVE]  = getLHRemove();
	operationsTable[OPERATION_ALLOC]     = getAlloc();
	operationsTable[OPERATION_DEALLOC]     = getDealloc();
	
	pageInit();
	bufInit();

	openLogWriter();

	InitiateRecovery();
	/* 	logInit();  */

	return 0;
}


int Tbegin() {

	int i, index = 0;

	if( numActiveXactions == MAX_TRANSACTIONS )
		return EXCEED_MAX_TRANSACTIONS;
	else
		numActiveXactions++;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		xidCount++;
		if( XactionTable[xidCount%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
			index = xidCount%MAX_TRANSACTIONS;
			break;
		}
	}

	assert( i < MAX_TRANSACTIONS );

	XactionTable[index] = LogTransBegin(xidCount);

	return XactionTable[index].xid;
}

void Tupdate(int xid, recordid rid, const void *dat, int op) {
  LogEntry * e;
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], rid, op, dat);

  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);

  /*  printf("e->LSN: %ld\n", e->LSN); */

  writeLSN(e->LSN, rid.page);
  doUpdate(e);
}

/* @todo what about locking? */
void Tread(int xid, recordid rid, void * dat) {
  readRecord(xid, rid, dat);
}

int Tcommit(int xid) {
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  LogTransCommit(&XactionTable[xid % MAX_TRANSACTIONS]);
  bufTransCommit(xid); /* unlocks pages */
  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  return 0;
}

int Tabort(int xid) {
  /* should call undoTrans after log trans abort.  undoTrans will cause pages to contain CLR values corresponding to  */
  undoTrans(XactionTable[xid%MAX_TRANSACTIONS]);
  LogTransAbort(&XactionTable[xid%MAX_TRANSACTIONS]);
  bufTransAbort(xid);
  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  return 0;
}

int Tdeinit() {
	int i;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		if( XactionTable[i].xid != INVALID_XTABLE_XID ) {
			Tabort(XactionTable[i].xid);
		}
	}
	assert( numActiveXactions == 0 );

	bufDeinit();
	/*	logDeinit(); */
	closeLogWriter();

	return 0;
}

void Trevive(int xid, long lsn) {
  int index = xid % MAX_TRANSACTIONS;
  if(XactionTable[index].xid != INVALID_XTABLE_XID) {
    if(xid != XactionTable[index].xid) {
      printf("Clashing Tprepare()'ed XID's encountered on recovery!!\n");
      assert(0);
    }
    assert(XactionTable[index].xid == xid);
    assert(XactionTable[index].prevLSN == lsn);
  } else {
    XactionTable[index].xid = xid;
    XactionTable[index].prevLSN = lsn;
    numActiveXactions++;
  }
}

void TsetXIDCount(int xid) {
  xidCount = xid;
}