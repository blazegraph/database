/*

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
package com.bigdata.rdf.task;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.Journal;

/**
 * Wrapper for a task to be executed on the {@link IConcurrencyManager} of a
 * {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
 *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
 * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
 *      Concurrent unisolated operations against multiple KBs </a>
 */
public class ApiTaskForJournal<T> extends AbstractTask<T> {

   private final IApiTask<T> delegate;

   @Override
   public String toString() {

      return super.toString() + "::{delegate=" + delegate + "}";

   }

   public ApiTaskForJournal(final IConcurrencyManager concurrencyManager,
         final long timestamp, final String[] resource,
         final IApiTask<T> delegate) {

      super(concurrencyManager, timestamp, resource);

      this.delegate = delegate;

   }

   @Override
   protected T doTask() throws Exception {

      // Set reference to Journal on the delegate.
      delegate.setIndexManager(getJournal());

      try {

         // Run the delegate task.
         final T ret = delegate.call();

         return ret;

      } finally {

         // Clear reference to the Journal from the delegate.
         delegate.setIndexManager(null);

      }

   }

}
