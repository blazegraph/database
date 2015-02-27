/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Apr 13, 2011
 */
package com.bigdata.rdf.sail;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.webapp.DatasetNotFoundException;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;

/**
 * Task destroys a KB for the given namespace. The correct use of this class is
 * as follows:
 * 
 * <pre>
 * AbstractApiTask.submitApiTask(indexManager, new DestroyKBTask(namespace)).get();
 * </pre>
 * 
 * @see CreateKBTask
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DestroyKBTask extends AbstractApiTask<Void> {

   private static final transient Logger log = Logger
         .getLogger(DestroyKBTask.class);

   public DestroyKBTask(final String namespace) {
    
      super(namespace, ITx.UNISOLATED, true/* isGRSRequired */);
      
   }

   @Override
   final public boolean isReadOnly() {
      return false;
   }

   /**
    * @throws DatasetNotFoundException if the namespace does not exist.
    */
   @Override
   public Void call() throws Exception, DatasetNotFoundException {

      final AbstractTripleStore tripleStore = getTripleStore();

      if (tripleStore == null) {

         throw new DatasetNotFoundException("Not found: namespace="
               + namespace + ", timestamp="
               + TimestampUtility.toString(timestamp));

      }

      // Destroy the KB instance.
      tripleStore.destroy();

      // Note: This is the commit point only if group commit is NOT enabled.
      tripleStore.commit();
      
      if (log.isInfoEnabled())
         log.info("Destroyed: namespace=" + namespace);
      
      return null;
      
   }

}
