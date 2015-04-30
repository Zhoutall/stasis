# Stasis wiki #

The wiki contains ideas for future Stasis development

## Buffer Manager notes ##

Stasis' current buffer manager assumes that the underlying OS provides a correct pthreads implementation, but for portability only uses extremely basic I/O routines, like read() and write().  In practice, that means it's complicated, and has high synchronization overhead.  It also can interact badly with the underlying OS, as it needs to have multiple file handles open in order to issue concurrent I/O requests.

There are two alternative API's that we'd like to support:

  * _**Done**_ **pread() / pwrite()**  These two functions are like read and write, but atomically update the file handle location, and perform the I/O operation.  This should greatly reduce the complexity and overhead of the "legacy" buffer manager on OS's that support pread/pwrite.

  * **libaio** Although not portable, Linux AIO seems to be a good way to implement Stasis's next buffer manager. In particular, it can return once a prefix of a request has returned, providing nice prefetching support.

  * **mmap** + mlock lets us treat the OS as a buffer manager; linux provides a number of potentially interesting vm hints

## General purpose mechanisms that databases typically don't provide ##
  * _skiplock_ - Traverse queue; move to next entry if head is locked; allows concurrent dequeue) for reliable queues
  * _field calls_ - register check at update. On commit, see if condition still holds. If so, apply change. (also look at p`*` time in vldb for tricks w/ XOR; was done by company called Transact In Memory)

## B-Tree plans ##

Stasis' initial B-Tree implementation is geared toward LSM-Trees, and only supports append, scan and lookup operations.  This allows it to support fast bulk loading operations, and avoids the complexity of dealing with page splits, etc. A MySQL storage engine would be interested in the following types of index probes:

  * Full table scan
  * Full index scan
  * Range scan
  * Point query

Moving forward there are a number of optimizations / implementation strategies to consider:

  * Row Cache: Frequently accessed items could be moved into an in-memory data strucuture. (InnoDB does this)
  * Sparse Index: Instead of storing each value in the tree, store a single key per data page. If items are sorted within each page, then this is sufficient.
    * This is a nice general purpose technique if index probes usually return a tuple.