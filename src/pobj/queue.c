#include <string.h>
#include "common.h"
#include "debug.h"
#include "xmem.h"

#define QUEUE_SEG_MAX_EXP  10
#define QUEUE_SEG_MAX      (1 << QUEUE_SEG_MAX_EXP)

struct queue_seg {
    int head;
    int tail;
    struct queue_seg *next;
};
#define SEG_OFFSET         ALIGN(sizeof(struct queue_seg))
#define SEG_TOTAL_SIZE(n)  (SEG_OFFSET + ((n) * sizeof(unsigned long)))
#define SEG_BUF(s)         ((unsigned long *) ((char *)s + SEG_OFFSET))

struct queue {
    struct queue_seg *head_seg;
    struct queue_seg *tail_seg;
    int seg_size;
};

static struct queue_seg *
queue_seg_alloc (int seg_size)
{
    struct queue_seg *seg;

    seg = (struct queue_seg *) XMALLOC (SEG_TOTAL_SIZE (seg_size));
    if (! seg)
	return NULL;

    memset (seg, 0, sizeof (struct queue_seg));
    return seg;
}

struct queue *
queue_new (int seg_size)
{
    struct queue *q;

    debug_start ();

    q = (struct queue *) XMALLOC (sizeof (struct queue));
    if (q)
	q->tail_seg = q->head_seg = queue_seg_alloc (seg_size);
    if (! (q && q->head_seg)) {
	if (q)
	    XFREE (q);
	debug ("allocation failed");
	debug_end ();
	return NULL;
    }

    q->seg_size = seg_size;
    debug_end ();
    return q;
}

void
queue_free (struct queue *q)
{
    struct queue_seg *seg, *next;

    for (seg = q->head_seg; seg; seg = next) {
	next = seg->next;
	XFREE (seg);
    }
    XFREE (q);
}


unsigned long
queue_deq (struct queue *q)
{
    struct queue_seg *seg = q->head_seg;
    unsigned long *buf = SEG_BUF (seg);
    unsigned long val;

    debug_start ();

    /* Verify head segment is not empty. */
    if (seg->head == seg->tail && ! seg->next) {
	debug ("queue empty");
	debug_end ();
	return 0;
    }

    /* Dequeue head value. */
    val = buf[seg->head++];
    if (seg->head == q->seg_size)
	seg->head = 0;
    debug ("dequeued %lu (%p)", val, (void *) val);

    /* Deallocate empty head segment, if it isn't the last one. */
    if (seg->head == seg->tail && seg->next) {
	q->head_seg = seg->next;
	XFREE (seg);
    }

    debug_end ();
    return val;
}

int
queue_enq (struct queue *q, unsigned long val)
{
    struct queue_seg *seg = q->tail_seg;
    unsigned long *buf = SEG_BUF (seg);

    debug_start ();
    
    /* Enqueue value. */
    buf[seg->tail++] = val;
    if (seg->tail == q->seg_size)
	seg->tail = 0;

    debug ("enqueued %lu (%p)", val, (void *) val);

    /* Allocate new segment if current was exhausted. */
    if (seg->head == seg->tail) {
	seg->next = queue_seg_alloc (q->seg_size);
	if (! seg->next) {
	    if (! seg->tail--)
		seg->tail += q->seg_size;
	    debug ("segment allocation failed, enqueue undone");
	    debug_end ();
	    return -1;
	}
	q->tail_seg = seg->next;
    }

    debug_end ();
    return 0;
}
