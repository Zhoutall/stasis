/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/

#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
/*#include <lladd/logger/logEntry.h> */
#include "../../src/lladd/logger/logHandle.h"
#include <lladd/logger/logger2.h>
//#include "../../src/lladd/logger/logWriter.h"

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"
#include <lladd/truncation.h>


#define LOG_NAME   "check_logWriter.log"

//static int logType = LOG_TO_MEMORY;

static void setup_log() {
  int i;
  lsn_t prevLSN = -1;
  int xid = 42;
  deleteLogWriter();
  lladd_enableAutoTruncation = 0;
  Tinit();

  //  LogDeinit();
  //deleteLogWriter();
  //  openLogWriter();

  //  LogInit(logType);
  
  for(i = 0 ; i < 1000; i++) {
    LogEntry * e = allocCommonLogEntry(prevLSN, xid, XBEGIN);
    const LogEntry * f;
    recordid rid;
    byte * args = (byte*)"Test 123.";
    long args_size = 10;  /* Including null */
    unsigned long preImage = 42;

    rid.page = 0;
    rid.slot = 0;
    rid.size = sizeof(unsigned long);

    genericLogWrite(e);
    prevLSN = e->LSN;

    f = LogReadLSN(prevLSN);

    fail_unless(sizeofLogEntry(e) == sizeofLogEntry(f), "Log entry changed size!!");
    fail_unless(0 == memcmp(e,f,sizeofLogEntry(e)), "Log entries did not agree!!");

    FreeLogEntry (e);
    FreeLogEntry (f);

    e = allocUpdateLogEntry(prevLSN, xid, 1, rid, args, args_size, (byte*) &preImage);

    genericLogWrite(e);
    prevLSN = e->prevLSN;

    LogEntry * g = allocCLRLogEntry(100, 1, 200, rid, 0); //prevLSN);

    genericLogWrite(g);
    assert (g->type == CLRLOG);
    prevLSN = g->LSN; 

    FreeLogEntry (e);
    FreeLogEntry (g);
  }
  //  truncationDeinit();

}
/**
   @test 

   Quick test of log writer and log handler.  Not very extensive.
   Just writes out 3000 log entries, checks that 1000 of them make
   sense, and then closes, opens and iterates over the resulting log
   file to make sure that it contains 3000 entries, and none of its
   builtin assertions fail.

   In particular, logWriter checks to make sure that each log entry's
   size matches the size that it recorded before the logEntry.  Also,
   when checking the 1000 of 3000 entries, this test uses
   LogReadLSN, which tests the logWriter's ability to succesfully
   manipulate LSN's.

   @todo Test logHandle more thoroughly. (Still need to test the guard mechanism.)

*/
START_TEST(logWriterTest)
{
  const LogEntry * e;
  LogHandle h;
  int i = 0;


  setup_log();
  //  syncLog();
  //closeLogWriter();
  //  LogDeinit();
  //  openLogWriter();
  //  LogInit(logType);
  
  h = getLogHandle();
  /*  LogReadLSN(sizeof(lsn_t)); */

  while((e = nextInLog(&h))) {
    FreeLogEntry(e);
    i++;
  }


  fail_unless(i = 3000, "Wrong number of log entries!");

  deleteLogWriter();
  LogDeinit();


}
END_TEST

/** 
    @test
    Checks for a bug ecountered during devlopment.  What happens when
    previousInTransaction is called immediately after the handle is
    allocated? */

START_TEST(logHandleColdReverseIterator) {
  const LogEntry * e;
  setup_log();
  LogHandle lh = getLogHandle();
  int i = 0;


  while(((e = nextInLog(&lh)) && (i < 100)) ) {
    FreeLogEntry(e);
    i++;
  }
  
  i = 0;
  //  printf("getLogHandle(%ld)\n", e->LSN);
  lh = getLSNHandle(e->LSN);  // was 'getLogHandle...'
  while((e = previousInTransaction(&lh))) {
    i++;
    FreeLogEntry(e);
  }
  /*  printf("i = %d\n", i); */
  //  assert(i == 1);
  assert(i < 4); /* We should almost immediately hit a clr that goes to the beginning of the log... */
  //  fail_unless( i == 1 , NULL);  
  LogDeinit();
  deleteLogWriter();

}
END_TEST

/** 
    @test

    Build a simple log, truncate it, and then test the logWriter routines against it.
*/
START_TEST(logWriterTruncate) {
  const LogEntry * le;
  const LogEntry * le2;
  const LogEntry * le3 = NULL;
  const LogEntry * tmp;
  setup_log();

  LogHandle lh = getLogHandle();
  int i = 0;

  while(i < 234) {
    i++;
    le = nextInLog(&lh);
  }
 
  le2 = nextInLog(&lh);
  i = 0;
  while(i < 23) {
    i++;
    le3 = nextInLog(&lh);
  }
  

  //  truncateLog(le->LSN);
  LogTruncate(le->LSN);
  
  tmp = LogReadLSN(le->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le->LSN, NULL);
  
  FreeLogEntry(tmp);
  tmp = LogReadLSN(le2->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le2->LSN, NULL);

  FreeLogEntry(tmp);
  tmp = LogReadLSN(le3->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le3->LSN, NULL);
  
  FreeLogEntry(tmp);

  lh = getLogHandle();
  
  i = 0;
  
  FreeLogEntry(le);
  FreeLogEntry(le2);
  FreeLogEntry(le3);

  while((le = nextInLog(&lh))) {
    i++;
    FreeLogEntry(le);
  }


  assert(i == (3000 - 234 + 1));
  //  fail_unless(i == (3000 - 234 + 1), NULL);
  
  LogDeinit();

} END_TEST

#define ENTRIES_PER_THREAD 200

pthread_mutex_t random_mutex;

lsn_t truncated_to = 4;
//pthread_mutex_t truncated_to_mutex = PTHREAD_MUTEX_INITIALIZER;

static void* worker_thread(void * arg) {
  long key = *(int*)arg;
  long i = 0;




  lsn_t lsns[ENTRIES_PER_THREAD];


  /*  fail_unless(NULL != le, NULL); */

  while(i < ENTRIES_PER_THREAD) {
    LogEntry * le = allocCommonLogEntry(-1, -1, XBEGIN);
    int threshold;
    long entry;
    int needToTruncate = 0;
    lsn_t myTruncVal = 0;
    pthread_mutex_lock(&random_mutex);

    threshold = (int) (2000.0*random()/(RAND_MAX+1.0));
    entry = (long) (ENTRIES_PER_THREAD*random()/(RAND_MAX+1.0));

    if(threshold < 3) { 
      if(i > 10) {
	needToTruncate = 1;
	if(lsns[i - 10] > truncated_to) {
	  truncated_to = lsns[i - 10];
	  myTruncVal = truncated_to;
	}
      }
    }

    pthread_mutex_unlock(&random_mutex);

    if(needToTruncate) { 
      LogTruncate(myTruncVal);
      assert(LogTruncationPoint() >= myTruncVal);
    }

    if(threshold < 3) {
    } else {
      le->xid = i+key;
      genericLogWrite(le);
      //printf("reportedLSN: %ld\n", le->LSN);
      lsns[i] = le->LSN;
      i++;
    }
    /*    fail_unless(1, NULL); */
    pthread_mutex_lock(&random_mutex);
    if(lsns[entry] > truncated_to && entry < i) {
      /*printf("X %d\n", (LogReadLSN(lsns[entry])->xid == entry+key)); fflush(stdout); */
      const LogEntry * e = LogReadLSN(lsns[entry]);
      pthread_mutex_unlock(&random_mutex);
      assert(e->xid == entry+key);
      FreeLogEntry(e);
      /*      fail_unless(LogReadLSN(lsns[entry])->xid == entry+key, NULL); */
    } else { 
      pthread_mutex_unlock(&random_mutex);
    }
    /*    fail_unless(1, NULL); */
	
    /* Try to interleave requests as much as possible */
    /*pthread_yield(); */
    sched_yield();
    FreeLogEntry(le);
  }



  return 0;
}

START_TEST(logWriterCheckWorker) {
  int four = 4;

  pthread_mutex_init(&random_mutex, NULL);

  Tinit();
  worker_thread(&four);
  Tdeinit();

} END_TEST

START_TEST(logWriterCheckThreaded) {

#define  THREAD_COUNT 100
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);

  Tinit();

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, worker_thread, &i);
    //    printf("%d", i); fflush(stdout);
  }
  //  printf("\n\n\n\n\n");
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
    //    printf("%d", i); fflush(stdout);
  }
  Tdeinit();

} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");
  tcase_set_timeout(tc, 0);
  /* Sub tests are added, one per line, here */
  
  tcase_add_test(tc, logWriterTest);
  tcase_add_test(tc, logHandleColdReverseIterator);
  tcase_add_test(tc, logWriterTruncate);
  tcase_add_test(tc, logWriterCheckWorker);
  tcase_add_test(tc, logWriterCheckThreaded); 

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
