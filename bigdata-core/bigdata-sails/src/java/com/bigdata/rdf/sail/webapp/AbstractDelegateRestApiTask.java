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
package com.bigdata.rdf.sail.webapp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.task.IApiTask;

/**
 * Base class for delegation patterns where the delegate is an {@link IApiTask}.
 * This is necessary in order to pass through the {@link #isGRSRequired()} and
 * {@link #setIndexManager(IIndexManager)} method to both the base class and the
 * delegate.
 * 
 * @author bryan
 * 
 * @param <T>
 */
abstract public class AbstractDelegateRestApiTask<T> extends
      AbstractRestApiTask<T> {

   private final IApiTask<T> delegate;

   public AbstractDelegateRestApiTask(final HttpServletRequest req,
         final HttpServletResponse resp, final String namespace,
         final long timestamp, final IApiTask<T> delegate) {

      super(req, resp, namespace, timestamp, delegate.isGRSRequired());

      this.delegate = delegate;
      
   }

   @Override
   public T call() throws Exception {
      
      return delegate.call();
      
   }

   @Override
   public void setIndexManager(final IIndexManager indexManager) {

      super.setIndexManager(indexManager);
      
      delegate.setIndexManager(indexManager);
      
   }

   @Override
   public boolean isGRSRequired() {
      
      return super.isGRSRequired() || delegate.isGRSRequired();
      
   }
   
}
