#include <lladd/operations.h>

#ifndef __NAIVE_LINEAR_HASH_H
#define __NAIVE_LINEAR_HASH_H

/**
   @file 

   @ingroup OPERATIONS

   $Id$
*/



recordid ThashAlloc(int xid, int keySize, int valSize) ;

void TnaiveHashInsert(int xid, recordid hashRid, 
		 void * key, int keySize, 
		 void * val, int valSize);
/*void ThashDelete(int xid, recordid hashRid, 
  void * key, int keySize);*/
int TnaiveHashDelete(int xid, recordid hashRid, 
		 void * key, int keySize, int valSize);
void TnaiveHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int TnaiveHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize);
void ThashInit();
void ThashDeinit();
int ThashOpen(int xid, recordid hashRid, int keySize, int valSize);
int ThashClose(int xid, recordid hashRid) ;
void lockBucket(int bucket);
void unlockBucket(int bucket);
int lockBucketForKey(const byte * key, int keySize, recordid * headerRidB);
#endif
