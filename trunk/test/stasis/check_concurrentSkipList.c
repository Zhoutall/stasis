/*
 * check_concurrentSkipList.c
 *
 *  Created on: Feb 8, 2012
 *      Author: sears
 */
#include <string.h>
#include <assert.h>
#ifdef STRINGS
static inline int stasis_util_skiplist_cmp(const void *a, const void *b) {
  if(a == NULL) return -1;
  if(b == NULL) return 1;
  assert(!(a == NULL && b == NULL));
  return strcmp(a,b);
}
#else
static inline long stasis_util_skiplist_cmp(const void *a, const void *b) {
  // Note: Below, we ensure a and b are both >= 0 and small.
  return (long)(a-b);
}
#endif
#include <stasis/util/concurrentSkipList.h>
#include "../check_includes.h"

#include <stasis/constants.h>
#include <stasis/util/random.h>

#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_lhtable.log"

int num_keys    = 1000000;
int num_threads = 4;
int concurrent = 0;
stasis_skiplist_t * list;
void * worker(void* p) {
  char ** keys = p;
  int collisions = 0;
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_insert(list, keys[i]);
    if(ret != NULL) {
      assert(!stasis_util_skiplist_cmp(ret, keys[i]));
      collisions++;
    }
  }
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_search(list, keys[i]);
    if(!concurrent) assert(!stasis_util_skiplist_cmp(ret, keys[i]));
  }
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_delete(list, keys[i]);
    if(ret == NULL) {
      collisions--;
    }
  }
  return (void*) collisions;
}
/**
   @test
*/
START_TEST(concurrentSkipList_smokeTest) {
  list = stasis_util_skiplist_init();
  char ** const keys = malloc(sizeof(char*) * num_keys);
  for(int i = 0; i < num_keys; i++) {
#ifdef STRINGS
    int err = asprintf(&keys[i], "%d", (int)stasis_util_random64(2*num_keys));
    (void) err;
#else
    keys[i] = (void*)(1+stasis_util_random64(2*num_keys));
#endif
  }
  printf("Initted\n");
  fflush(stdout);
  struct timeval tv;
  gettimeofday(&tv,0);
  double start = stasis_timeval_to_double(tv);
  int collisions = worker(keys);
  assert(collisions == 0);
  gettimeofday(&tv,0);
  double stop = stasis_timeval_to_double(tv);
  double elapsed = stop - start;
  double opspersec = 3.0*num_keys / elapsed;
  printf("Run took %f seconds.  %f ops/sec 1 thread %f ops/thread-second\n", elapsed, opspersec, opspersec);
  stasis_util_skiplist_deinit(list);
#ifdef STRINGS
  for(int i = 0; i < num_keys; i++) {
    free(keys[i]);
  }
#endif
  free(keys);
} END_TEST

START_TEST(concurrentSkipList_concurrentTest) {
  list = stasis_util_skiplist_init();
  concurrent = 1;
  char *** const keys = malloc(sizeof(char**) * num_threads);
  for(int j = 0; j < num_threads; j++) {
    keys[j] = malloc(sizeof(char*) * num_keys);
    for(int i = 0; i < num_keys; i++) {
  #ifdef STRINGS
      int err = asprintf(&keys[i], "%d", (int)stasis_util_random64(2*num_keys));
      (void) err;
  #else
      keys[j][i] = (void*)(1+stasis_util_random64(2*num_keys));
  #endif
    }
  }
  printf("Initted\n");
  fflush(stdout);
  pthread_t * threads = malloc(sizeof(pthread_t) * num_threads);
  struct timeval tv;
  gettimeofday(&tv,0);
  double start = stasis_timeval_to_double(tv);
  int collisions = 0;
  for(int j = 0; j < num_threads; j++) {
    pthread_create(&threads[j], 0, worker, keys[j]);
  }
  for(int j = 0; j < num_threads; j++) {
    intptr_t ret;
    pthread_join(threads[j], (void*)&ret);
    collisions += ret;
#ifdef STRINGS
    for(int i = 0; i < num_keys; i++) {
      free(keys[j][i]);
    }
#endif
    free(keys[j]);
  }
  assert(collisions == 0);
  free(threads);
  gettimeofday(&tv,0);
  double stop = stasis_timeval_to_double(tv);
  double elapsed = stop - start;
  double opspersec = 3.0*(double)num_keys*num_threads / elapsed;
  double opsperthsec = 3.0*(double)num_keys / elapsed;
  printf("Run took %f seconds.  %f ops/sec %d threads %f ops/thread-second\n", elapsed, num_threads, opspersec, opsperthsec);
  stasis_util_skiplist_deinit(list);
  free(keys);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("concurrentSkipList");
  /* Begin a new test */
  TCase *tc = tcase_create("concurrentSkipList");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
//  tcase_add_test(tc, concurrentSkipList_smokeTest);
  tcase_add_test(tc, concurrentSkipList_concurrentTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"


