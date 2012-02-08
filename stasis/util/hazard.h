/*
 * hazard.h
 *
 *  Created on: Feb 7, 2012
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/util/time.h>
#ifndef HAZARD_H_
#define HAZARD_H_

typedef intptr_t hazard_ptr;
typedef struct hazard_t hazard_t;

typedef struct hazard_ptr_rec_t {
  hazard_t * h;
  hazard_ptr * hp;
  hazard_ptr * rlist;
  int rlist_len;
  struct hazard_ptr_rec_t * next;
} hazard_ptr_rec_t;

struct hazard_t {
  pthread_key_t hp;
  int num_slots;
  int num_r_slots;
  hazard_ptr_rec_t * tls_list;
  pthread_mutex_t tls_list_mut;
  pthread_cond_t thread_shutdown;
};
static int intptr_cmp(const void * ap, const void *bp) {
  intptr_t a = *(intptr_t*)ap;
  intptr_t b = *(intptr_t*)bp;
  return (a < b) ? -1 : ( (a > b) ? 1 : 0);
}
static inline void hazard_scan(hazard_t * h, hazard_ptr_rec_t * rec) {
  if(rec == NULL) {
    rec = pthread_getspecific(h->hp);
  }
  if(rec == NULL) { return; }
  qsort(rec->rlist, rec->rlist_len, sizeof(void*), intptr_cmp);
  hazard_ptr * ptrs = 0;
  int ptrs_len = 0;
  pthread_mutex_lock(&h->tls_list_mut);
  hazard_ptr_rec_t * list = h->tls_list;
  while(list != NULL) {
    ptrs_len += h->num_slots;
    ptrs = realloc(ptrs, sizeof(hazard_ptr) * ptrs_len);
    for(int i = 0; i < h->num_slots; i++) {
      ptrs[i + ptrs_len - h->num_slots] = list->hp[i];
    }
    list = list->next;
  }
  pthread_mutex_unlock(&h->tls_list_mut);
  qsort(ptrs, ptrs_len, sizeof(void*), intptr_cmp);
  int i = 0, j = 0;
  while(j < rec->rlist_len) {
    while(i < ptrs_len && rec->rlist[j] > ptrs[i]) { i++; }
    if(i == ptrs_len || rec->rlist[j] != ptrs[i]) {
      /// XXX for testing
      *(char*)rec->rlist[j] = (char)0xcd;
      free((void*)rec->rlist[j]);
      rec->rlist[j] = 0;
    }
    j++;
  }
  j = 0;
  for(i = 0; i < rec->rlist_len; i++) {
    if(rec->rlist[i] != 0) {
      rec->rlist[j] = rec->rlist[i];
      j++;
    }
  }
  rec->rlist_len = j;
  free(ptrs);
}
static void hazard_deinit_thread(void * p) {
  hazard_ptr_rec_t * rec = p;
  if(rec != NULL) {
    while(rec->rlist_len != 0) {
      hazard_scan(rec->h, rec);
      if(rec->rlist_len != 0) {
        pthread_mutex_lock(&rec->h->tls_list_mut);
        struct timespec ts = stasis_double_to_timespec(0.1);
        pthread_cond_timedwait(&rec->h->thread_shutdown,
          &rec->h->tls_list_mut, &ts);
        pthread_mutex_unlock(&rec->h->tls_list_mut);
      }
    }
    pthread_cond_broadcast(&rec->h->thread_shutdown);
    hazard_ptr_rec_t ** last = &rec->h->tls_list;
    hazard_ptr_rec_t * list = *last;
    while(list != rec) {
      last = &list->next;
      list = *last;
    }
    *last = rec->next;
    free(rec->hp);
    free(rec->rlist);
    free(rec);
  }
}
static inline hazard_t* hazard_init(int hp_slots, int r_slots) {
  hazard_t * ret = malloc(sizeof(hazard_t));
  pthread_key_create(&ret->hp, hazard_deinit_thread);
  ret->num_slots = hp_slots;
  ret->num_r_slots = r_slots;
  ret->tls_list = NULL;
  pthread_mutex_init(&ret->tls_list_mut,0);
  pthread_cond_init(&ret->thread_shutdown, 0);
  return ret;
}
static inline hazard_ptr_rec_t * hazard_ensure_tls(hazard_t * h) {
  hazard_ptr_rec_t * rec = pthread_getspecific(h->hp);
  if(rec == NULL) {
    rec = malloc(sizeof(hazard_ptr_rec_t));
    rec->hp = calloc(h->num_slots, sizeof(hazard_ptr));
    rec->rlist = calloc(h->num_r_slots, sizeof(hazard_ptr));
    rec->rlist_len = 0;
    pthread_setspecific(h->hp, rec);
    pthread_mutex_lock(&h->tls_list_mut);
    rec->next = h->tls_list;
    rec->h = h;
    h->tls_list = rec;
    pthread_mutex_unlock(&h->tls_list_mut);
  }
  return rec;
}
static inline void hazard_deinit(hazard_t * h) {
  hazard_ptr_rec_t * rec = pthread_getspecific(h->hp);
  hazard_deinit_thread(rec);
  pthread_key_delete(h->hp);
  assert(h->tls_list == NULL);
  pthread_mutex_destroy(&h->tls_list_mut);
  pthread_cond_destroy(&h->thread_shutdown);
  free(h);
}
static inline void * hazard_ref(hazard_t* h, int slot, hazard_ptr* ptr) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  do {
    rec->hp[slot] = *ptr;
    __sync_synchronize();
  } while(rec->hp[slot] != *ptr);
  return (void*) rec->hp[slot];
}
// Must be called *after* all references to ptr are removed.
static inline void hazard_free(hazard_t* h,  hazard_ptr ptr) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  rec->rlist[rec->rlist_len] = ptr;
  (rec->rlist_len)++;
  if(rec->rlist_len == h->num_r_slots) {
    hazard_scan(h, rec);
  }
}
#endif /* HAZARD_H_ */
