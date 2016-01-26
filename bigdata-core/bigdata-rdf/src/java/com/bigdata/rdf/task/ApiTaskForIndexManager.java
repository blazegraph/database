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

import java.util.concurrent.Callable;

import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.service.IBigdataFederation;

/**
 * Wrapper for a task to be executed in a stand-off fashion. This can be used
 * for the {@link IBigdataFederation} since it provides access to the global
 * view of a scale-out index. It can also be used for a {@link Journal} if are
 * not relying on the {@link IConcurrencyManager} to guard the resources
 * declared by the task.
 * <p>
 * Note: Global locks are NOT used in scale-out and operations will be only
 * shard-wise ACID.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 */
public class ApiTaskForIndexManager<T> implements Callable<T> {

   private final IIndexManager indexManager;
   private final IApiTask<T> delegate;

   public ApiTaskForIndexManager(final IIndexManager indexManager,
         final IApiTask<T> delegate) {

      this.indexManager = indexManager;
      this.delegate = delegate;

   }

   @Override
   public T call() throws Exception {

      delegate.setIndexManager(indexManager);

      try {

         // Run the delegate task.
         final T ret = delegate.call();

         return ret;

      } finally {

         delegate.setIndexManager(null);

      }

   }

}
