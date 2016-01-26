/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.journal;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.service.AbstractTransactionService;

/**
 * A utility to fix a store with invalid addresses in the deferred
 * free list of a commitRecord.
 * <p>
 * This condition is rare and not well understood, but if an invalid
 * address has been added to the deferredFree list then the store cannot
 * be opened, or at some point will not be able to be opened.
 * <p>
 * This utility attempts to update the retained CommitRecords with a
 * zero address for the associated deferredFrees.
 * <p>
 * The assumption is that it is a DiskRW store.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1228" > Recycler error in 1.5.1 </a>
 * @author Martyn Cutcher
 */
public class RemoveDeferredFreesFromCommitRecords {

   static class MyJournal extends Journal {
      MyJournal(Properties props) {
         super(props);
      }
      
      IIndex getMutableCommitIndexManager() {
         return getCommitRecordIndex(
                    getRootBlockView().getCommitRecordIndexAddr(), false/* readOnly */);
      }
   }
   
   static public void main(final String[] args) {
      if (args.length != 1) {
         System.err.println("Usage: <storename>");
         return;
      }
      
      final File f = new File(args[0]);
      if (!f.exists()) {
         System.err.println("File not found: " + args[0]);
         return;
      }
      
      final Properties props = new Properties();
      props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
      props.setProperty(Options.FILE, args[0]);
      
      // Here's the trick! Set minimum release age to NEVER so we can safely update the commit records
      props.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, AbstractTransactionService.Options.MIN_RELEASE_AGE_NEVER);
      
      final MyJournal jnl = new MyJournal(props);
      try {
         { // Simplest approach is to remove the commitRecords
              final IIndex commitRecordIndex = jnl.getMutableCommitIndexManager();
      
              final IndexMetadata metadata = commitRecordIndex
                      .getIndexMetadata();
              final byte[] fromKey = metadata.getTupleSerializer()
                      .serializeKey(0L);
              final byte[] toKey = metadata.getTupleSerializer()
                      .serializeKey(System.currentTimeMillis());
      
            jnl.removeCommitRecordEntries(fromKey, toKey);
         }
         
         jnl.commit();
      
      } finally {
         jnl.shutdownNow();
      }
   }
}
