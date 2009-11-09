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

/**
 * @file
 *
 * Implementation of in memory buffer pool
 *
 * $Id$
 *
 */
#include <assert.h>

#include <stasis/bufferPool.h>
#include <stasis/page.h>

struct stasis_buffer_pool_t {
	pageid_t nextPage;
	Page* pool;
	pthread_mutex_t mut;
	void * addr_to_free;
};

stasis_buffer_pool_t* stasis_buffer_pool_init() {

  stasis_buffer_pool_t * ret = malloc(sizeof(*ret));

  ret->nextPage = 0;

  pthread_mutex_init(&(ret->mut), NULL);

  byte * bufferSpace = calloc((MAX_BUFFER_SIZE + 2), PAGE_SIZE);
  assert(bufferSpace);
  ret->addr_to_free = bufferSpace;

  bufferSpace = (byte*)(((long)bufferSpace) +
			PAGE_SIZE -
			(((long)bufferSpace) % PAGE_SIZE));

  // We need one dummy page for locking purposes,
  //  so this array has one extra page in it.
  ret->pool = malloc(sizeof(ret->pool[0])*(MAX_BUFFER_SIZE+1));

  for(pageid_t i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    ret->pool[i].rwlatch = initlock();
    ret->pool[i].loadlatch = initlock();
    ret->pool[i].memAddr = &(bufferSpace[i*PAGE_SIZE]);
    ret->pool[i].dirty = 0;
  }
  return ret;
}

void stasis_buffer_pool_deinit(stasis_buffer_pool_t * ret) {
  for(pageid_t i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    deletelock(ret->pool[i].rwlatch);
    deletelock(ret->pool[i].loadlatch);
  }
  free(ret->addr_to_free); // breaks efence
  pthread_mutex_destroy(&ret->mut);
}

Page* stasis_buffer_pool_malloc_page(stasis_buffer_pool_t * ret) {
  Page *page;

  pthread_mutex_lock(&ret->mut);

  page = &(ret->pool[ret->nextPage]);

  (ret->nextPage)++;
  /* There's a dummy page that we need to keep around, thus the +1 */
  assert(ret->nextPage <= MAX_BUFFER_SIZE + 1);

  pthread_mutex_unlock(&ret->mut);

  return page;

}

void stasis_buffer_pool_free_page(stasis_buffer_pool_t * ret, Page *p, pageid_t id) {
  writelock(p->rwlatch, 10);
  p->id = id;
  p->LSN = 0;
  p->dirty = 0;
  writeunlock(p->rwlatch);
}