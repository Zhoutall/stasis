/*
 * concurrentSkipList.h
 *
 *  Created on: Feb 8, 2012
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/util/random.h>
#include <stasis/util/hazard.h>
#include <stdio.h>
#ifndef CONCURRENTSKIPLIST_H_
#define CONCURRENTSKIPLIST_H_

static inline int stasis_util_skiplist_random_level(pthread_key_t k) {
  kiss_table_t * kiss = pthread_getspecific(k);
  if(kiss == 0) {
    kiss = malloc(sizeof(*kiss));
    stasis_util_random_kiss_settable(kiss,
        random(), random(), random(), random(), random(), random());
    pthread_setspecific(k, kiss);
  }
  // MWC is weaker but faster than KISS.  The main drawback is that it has a
  // period of 2^60.  I can't imagine that mattering for our purposes.

  // __builtin_ctz counts trailing zeros, so, this function hardcodes p = 0.5.
  // MWC returns a 32-bit int; above 2^32 elements we start to violate the
  // O(log n) bounds.
  return 1+__builtin_ctz(stasis_util_random_kiss_MWC(kiss));
}
typedef struct stasis_skiplist_node_t {
  void * key;
  pthread_mutex_t level_mut;
  char level;
} stasis_skiplist_node_t;
static inline stasis_skiplist_node_t ** stasis_util_skiplist_get_forward(
    stasis_skiplist_node_t * x, int n) {
  return (stasis_skiplist_node_t**)(((intptr_t)(x + 1))
      +n*(sizeof(stasis_skiplist_node_t*)+sizeof(pthread_mutex_t)));
}
static inline pthread_mutex_t * stasis_util_skiplist_get_forward_mutex(
    stasis_skiplist_node_t * x, int n) {
  return (pthread_mutex_t*)(stasis_util_skiplist_get_forward(x,n)+1);
}

typedef struct {
  stasis_skiplist_node_t * header;
  int levelCap;
  int levelHint;
  pthread_mutex_t levelHint_mut;
  pthread_key_t k;
  hazard_t * h;
} stasis_skiplist_t;

static inline stasis_skiplist_node_t * stasis_util_skiplist_make_node(int level, void * key) {
  stasis_skiplist_node_t * x
    = malloc(sizeof(*x)
        + (level+1) * (sizeof(hazard_ptr) + sizeof(pthread_mutex_t)));
  x->key = key;
  x->level = level;
  pthread_mutex_init(&x->level_mut,0);
  for(int i = 0; i < level+1; i++) {
    pthread_mutex_init(stasis_util_skiplist_get_forward_mutex(x, i), 0);
    *stasis_util_skiplist_get_forward(x, i) = 0;
  }
  return x;
}
static inline void stasis_util_skiplist_cleanup_tls(void * p) {
  free(p);
}
static inline int stasis_util_skiplist_cmp_helper(
    stasis_skiplist_node_t *a, void * b) {
  if(a == NULL) { return 1; }
  return stasis_util_skiplist_cmp(a->key, b);
}
static inline int stasis_util_skiplist_cmp_helper2(
    stasis_skiplist_node_t *a, stasis_skiplist_node_t * b) {
  if(b == NULL) { return a == NULL ? 0 : -1; }
  return stasis_util_skiplist_cmp_helper(a, b->key);
}
static inline stasis_skiplist_t * stasis_util_skiplist_init() {
  stasis_skiplist_t * list = malloc(sizeof(*list));
  list->h = hazard_init(2, 10);
  list->levelCap = 32;
  list->levelHint = 1;
  pthread_mutex_init(&list->levelHint_mut, 0);
  list->header = stasis_util_skiplist_make_node(list->levelCap, NULL);
  pthread_key_create(&(list->k), stasis_util_skiplist_cleanup_tls);
  return list;
}
static inline void stasis_util_skiplist_deinit(stasis_skiplist_t * list) {
  hazard_deinit(list->h);
  pthread_mutex_destroy(&list->levelHint_mut);
  free(list->header);
  kiss_table_t * kiss = pthread_getspecific(list->k);
  if(kiss) {
    stasis_util_skiplist_cleanup_tls(kiss);
    pthread_setspecific(list->k, 0);
  }
  pthread_key_delete(list->k);
  free(list);
}

static inline void * stasis_util_skiplist_search(stasis_skiplist_t * list, void * searchKey) {
  // the = 0 here are to silence GCC -O3 warnings.
  stasis_skiplist_node_t *x, *y = 0;
  int cmp = 0;
  x = list->header;
  for(int i = list->levelHint; i > 0; i--) {
    y = *stasis_util_skiplist_get_forward(x, i);
    while((cmp = stasis_util_skiplist_cmp_helper(y, searchKey)) < 0) {
      x = y;
      y = *stasis_util_skiplist_get_forward(x, i);
    }
  }
  if(cmp == 0) {
    return y->key;
  } else {
    return 0;
  }
}
static inline stasis_skiplist_node_t * stasis_util_skiplist_get_lock(stasis_skiplist_node_t * x, void * searchKey, int i) {
  stasis_skiplist_node_t * y = *stasis_util_skiplist_get_forward(x, i);
  while(stasis_util_skiplist_cmp_helper(y, searchKey) < 0) {
    x = y;
    y = *stasis_util_skiplist_get_forward(x, i);
  }
  pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(x, i));
  y = *stasis_util_skiplist_get_forward(x, i);
  while(stasis_util_skiplist_cmp_helper(y, searchKey) < 0) {
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
    x = y;
    pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(x, i));
    y = *stasis_util_skiplist_get_forward(x, i);
  }
  return x;
}
/**
 * Insert a value into the skiplist.  Any existing value will be replaced.
 * @return the old value or null if there was no such value.
 */
static inline void * stasis_util_skiplist_insert(stasis_skiplist_t * list, void * searchKey, void * newValue) {
  stasis_skiplist_node_t * update[list->levelCap+1];
  stasis_skiplist_node_t *x, *y;
  x = list->header;
  int L = list->levelHint;
  for(int i = L; i > 0; i--) {
    y = *stasis_util_skiplist_get_forward(x, i);
    while(stasis_util_skiplist_cmp_helper(y, searchKey) < 0) {
      x = y;
      y = *stasis_util_skiplist_get_forward(x, i);
    }
    update[i] = x;
  }
  x = stasis_util_skiplist_get_lock(x, searchKey, 1);
  if(stasis_util_skiplist_cmp_helper(*stasis_util_skiplist_get_forward(x, 1), searchKey) == 0) {
    void * oldKey = (*stasis_util_skiplist_get_forward(x, 1))->key;
    (*stasis_util_skiplist_get_forward(x, 1))->key = searchKey;
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, 1));
    return oldKey;
  }
  y = stasis_util_skiplist_make_node(stasis_util_skiplist_random_level(list->k), searchKey);
  pthread_mutex_lock(&y->level_mut);
  for(int i = L+1; i <= y->level; i++) {
    update[i] = list->header;
  }
  for(int i = 1; i <= y->level; i++) {
    if(i != 1) {
      x = stasis_util_skiplist_get_lock(update[i], searchKey, i);
    }
    *stasis_util_skiplist_get_forward(y, i) = *stasis_util_skiplist_get_forward(x, i);
    *stasis_util_skiplist_get_forward(x, i) = y;
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
  }
  pthread_mutex_unlock(&y->level_mut);

  L = list->levelHint;
  if(L < list->levelCap && *stasis_util_skiplist_get_forward(list->header, L+1) != 0) {
    if(pthread_mutex_trylock(&list->levelHint_mut) == 0) {
      while(list->levelHint < list->levelCap &&
          *stasis_util_skiplist_get_forward(list->header, list->levelHint+1) != 0) {
        list->levelHint = list->levelHint+1; // XXX atomics?
      }
      pthread_mutex_unlock(&list->levelHint_mut);
    }
  }
  return NULL;
}
/**
 * Delete a value from the list, returning it if it existed.
 * @return The old value, or null.
 */
static inline void * stasis_util_skiplist_delete(stasis_skiplist_t * list, void * searchKey) {
  stasis_skiplist_node_t * update[list->levelCap+1];
  stasis_skiplist_node_t *x, *y;
  x = list->header;
  int L = list->levelHint;
  // for i = L downto 1
  int i;
  for(i = L+1; i > 1;) {
    i--; // decrement after check, so that i is 1 at the end of the loop.
    y = *stasis_util_skiplist_get_forward(x, i);
    while(stasis_util_skiplist_cmp_helper(y, searchKey) < 0) {
      x = y;
      y = *stasis_util_skiplist_get_forward(x, i);
    }
    update[i] = x;
  }
  y = x;
  int isGarbage;
  int first = 1;
  do {
    if(first) {
      first = 0;
    } else {
      if(!isGarbage) {
        // This unlock was not in the pseudocode, but seems to be necessary...
        pthread_mutex_unlock(&y->level_mut);
        printf("Unlocking y in code that was missing from pseudocode\n");
      }
    }
    y = *stasis_util_skiplist_get_forward(y, i);
    if(stasis_util_skiplist_cmp_helper(y, searchKey) > 0) { return NULL; }
    pthread_mutex_lock(&y->level_mut);
    isGarbage = stasis_util_skiplist_cmp_helper2(y, *stasis_util_skiplist_get_forward(y, i)) > 0;
    if(isGarbage) { pthread_mutex_unlock(&y->level_mut); }
  } while(!(stasis_util_skiplist_cmp_helper(y, searchKey) == 0 && !isGarbage));
  for(int i = L+1; i <= y->level; i++) { update[i] = list->header; }
  for(int i = y->level; i > 0; i--) {
    x = stasis_util_skiplist_get_lock(update[i], searchKey, i);
    pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(y, i));
    *stasis_util_skiplist_get_forward(x, i) = *stasis_util_skiplist_get_forward(y, i);
    *stasis_util_skiplist_get_forward(y, i) = x;
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(y, i));
  }
  void * oldKey = y->key;
  pthread_mutex_unlock(&y->level_mut);
  hazard_free(list->h, y);
  L = list->levelHint;
  if(L > 1 && *stasis_util_skiplist_get_forward(list->header, L) == 0) {
    if(pthread_mutex_trylock(&list->levelHint_mut) == 0) {
      while(list->levelHint > 1 && *stasis_util_skiplist_get_forward(list->header, list->levelHint) == 0) {
        list->levelHint = list->levelHint - 1;
      }
      pthread_mutex_unlock(&list->levelHint_mut);
    }
  }
  return oldKey;
}
#endif /* CONCURRENTSKIPLIST_H_ */
