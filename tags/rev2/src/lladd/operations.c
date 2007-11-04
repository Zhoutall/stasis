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
#include <lladd/operations.h>

#include <lladd/logger/logWriter.h>
#include <lladd/bufferManager.h>
#include <assert.h>

Operation operationsTable[MAX_OPERATIONS];  

void doUpdate(const LogEntry * e) {
  operationsTable[e->contents.update.funcID].run(e->xid, e->contents.update.rid, getUpdateArgs(e));  
}

void redoUpdate(const LogEntry * e) {
  if(e->type == UPDATELOG) {
#ifdef DEBUGGING
    recordid rid = e->contents.update.rid;
#endif
    if(e->LSN > readLSN(e->contents.update.rid.page)) {
      DEBUG("Redo, %ld {%d %d %d}\n", e->LSN, rid.page, rid.slot, rid.size);
      doUpdate(e);
    } else {
      DEBUG("Skipping redo, %ld {%d %d %d}\n", e->LSN, rid.page, rid.slot, rid.size);
    }
  } else if(e->type == CLRLOG) {
    LogEntry * f = readLSNEntry(e->contents.clr.thisUpdateLSN);
#ifdef DEBUGGING    
    recordid rid = f->contents.update.rid;
#endif
    /* See if the page contains the result of the undo that this CLR is supposed to perform. If it
       doesn't, then undo the original operation. */
    if(f->LSN > readLSN(e->contents.update.rid.page)) {

      DEBUG("Undoing for clr, %ld {%d %d %d}\n", f->LSN, rid.page, rid.slot, rid.size);
      undoUpdate(f);
    } else {
      DEBUG("Skiping undo for clr, %ld {%d %d %d}\n", f->LSN, rid.page, rid.slot, rid.size);
    }
  } else {
    assert(0);
  }

}


void undoUpdate(const LogEntry * e) {

  int undo = operationsTable[e->contents.update.funcID].undo;
  /*  printf("FuncID: %d Undo op: %d\n",e->contents.update.funcID, undo); fflush(NULL); */
  if(e->LSN <= readLSN(e->contents.update.rid.page)) {
#ifdef DEBUGGING
    recordid rid = e->contents.update.rid;
#endif
    
    

    /* Actually execute the undo */
    if(undo == NO_INVERSE) {
      /* Physical undo */ 
      
      DEBUG("Physical undo, %ld {%d %d %d}\n", e->LSN, rid.page, rid.slot, rid.size);
      /** @todo Why were we passing in RECOVERY_XID? */
      writeRecord(e->xid, e->contents.update.rid, getUpdatePreImage(e));
    } else {
      /* @see doUpdate() */
      /*      printf("Logical undo"); fflush(NULL); */
      DEBUG("Logical undo, %ld {%d %d %d}\n", e->LSN, rid.page, rid.slot, rid.size);
      operationsTable[undo].run(e->xid, e->contents.update.rid, getUpdateArgs(e));  
    }
  }
  /*  printf("Undo done."); fflush(NULL); */

}