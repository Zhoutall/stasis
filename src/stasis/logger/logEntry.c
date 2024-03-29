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
#include <stasis/page.h> // For stasis_record_type_to_size()
#include <stasis/logger/logger2.h> // needed for LoggerSizeOfInternalLogEntry()
#include <stasis/logger/logEntry.h>
#include <stasis/util/crc32.h>

#include <assert.h>

LogEntry * mallocScratchCommonLogEntry(lsn_t LSN, lsn_t prevLSN, int xid, unsigned int type) {
  LogEntry * ret = calloc(1, sizeof(struct __raw_log_entry));
  ret->LSN = LSN;
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = type;
  return ret;
}
LogEntry * allocCommonLogEntry(stasis_log_t* log, lsn_t prevLSN, int xid, unsigned int type) {
  LogEntry * ret = log->reserve_entry(log,sizeof(struct __raw_log_entry));
  ret->prevLSN = prevLSN;
  ret->xid     = xid;
  ret->type    = type;
  return ret;
}
LogEntry * allocPrepareLogEntry(stasis_log_t* log, lsn_t prevLSN, int xid, lsn_t recLSN) {
  LogEntry * ret = log->reserve_entry(log,sizeof(struct __raw_log_entry)+sizeof(lsn_t));
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = XPREPARE;
  *(lsn_t*)(((struct __raw_log_entry*)ret)+1)=recLSN;
  return ret;
}

const void * stasis_log_entry_update_args_cptr(const LogEntry * ret) {
  assert(ret->type == UPDATELOG ||
	 ret->type == CLRLOG);
  if(ret->update.arg_size == 0) {
    return NULL;
  } else {
    return ((const byte*)ret) +
      sizeof(struct __raw_log_entry) +
      sizeof(UpdateLogEntry);
  }
}

void * stasis_log_entry_update_args_ptr(LogEntry * ret) {
  return (void*)stasis_log_entry_update_args_cptr(ret);
}

lsn_t getPrepareRecLSN(const LogEntry *e) {
  lsn_t ret = *(lsn_t*)(((struct __raw_log_entry*)e)+1);
  if(ret == -1) { ret = e->LSN; }
  return ret;
}

// XXX get rid of the mallocScratch* functions.
LogEntry * mallocScratchUpdateLogEntry(lsn_t LSN, lsn_t prevLSN, int xid,
                   unsigned int op, pageid_t page,
                   unsigned int arg_size) {
  size_t logentrysize =
    sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + arg_size;

  LogEntry * ret = calloc(1, logentrysize);
  ret->LSN = LSN;
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = UPDATELOG;
  ret->update.funcID = op;
  ret->update.page    = page;
  ret->update.arg_size = arg_size;

  return ret;
}

LogEntry * allocUpdateLogEntry(stasis_log_t* log, lsn_t prevLSN, int xid,
			       unsigned int op, pageid_t page,
			       unsigned int arg_size) {
  size_t logentrysize =
    sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + arg_size;

  LogEntry * ret = log->reserve_entry(log,logentrysize);
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = UPDATELOG;
  ret->update.funcID = op;
  ret->update.page    = page;
  ret->update.arg_size = arg_size;

  return ret;
}

LogEntry * allocCLRLogEntry(stasis_log_t* log, const LogEntry * old_e) {
  CLRLogEntry * ret = (CLRLogEntry*)log->reserve_entry(log,sizeof(struct __raw_log_entry)+sizeofLogEntry(0, old_e));

  ret->prevLSN = old_e->prevLSN;
  ret->xid = old_e->xid;
  ret->type = CLRLOG;
  DEBUG("compensates: %lld\n", old_e->LSN);
  memcpy((void*)getCLRCompensated(ret), old_e, sizeofLogEntry(0, old_e));

  return (LogEntry*)ret;
}

lsn_t sizeofLogEntry(stasis_log_t * log, const LogEntry * e) {
  switch (e->type) {
  case CLRLOG:
    {
      const LogEntry * contents = getCLRCompensated((const CLRLogEntry*) e);
      assert(contents->type != CLRLOG);
      return sizeof(struct __raw_log_entry) + sizeofLogEntry(log, contents);
    }
  case UPDATELOG:
    {
      return sizeof(struct __raw_log_entry) +
        sizeof(UpdateLogEntry) + e->update.arg_size;
    }
  case INTERNALLOG:
    assert(log);
    return log->sizeof_internal_entry(log,e);
  case XPREPARE:
    return sizeof(struct __raw_log_entry)+sizeof(lsn_t);
  default:
    return sizeof(struct __raw_log_entry);
  }
}
