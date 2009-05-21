/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
package edu.lehigh.swat.bench.ubt.bigdata;

import java.util.Properties;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Configuration for a {@link LocalTripleStore} on a {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LTS extends BigdataRepositoryFactory {

    public LTS(String database) {

        super(database);
        
    }

    @Override
    public IRepositoryLifeCycle newLifeCycle() {
        
        return new LifeCycleLTS();
        
    }

    protected class LifeCycleLTS implements IRepositoryLifeCycle<Journal>  {

        public Properties getProperties() {
            
            return LTS.this.getProperties();
            
        }

        /**
         * Opens an existing {@link Journal} identified by
         * {@link com.bigdata.journal.Options#FILE} or create and returned a new
         * {@link Journal} if there is no backing file by that name.
         */
        public Journal open() {

            final Properties properties = getProperties();

            if (BigdataRepositoryFactory.log.isInfoEnabled())
                BigdataRepositoryFactory.log.info("Create/open of journal: "+properties.getProperty(Options.FILE));
            
            return new Journal(properties);
            
        }
        
        public void close(Journal indexManager) {

            if (indexManager.isOpen()) {

                if(BigdataRepositoryFactory.log.isInfoEnabled()) {
                    
                    BigdataRepositoryFactory.log.info("Close: "+indexManager);
                    
                }
                
                /*
                 * NORMAL shutdown (waits for the executor service to
                 * terminate).
                 * 
                 * Note: I was seeing an occasional IllegalStateException that
                 * was apparently arising when the store was closed following
                 * the asynchronous close of a high-level query iterator but
                 * before any thread(s) participating in the query execution
                 * (esp., the JOIN threads) have noticed that they have been
                 * interrupted or otherwise finished executing. It seems that
                 * the root of the problem was calling journal#close() here
                 * (does not wait for the executor service) instead of
                 * journal#shutdown() (waits for threads to halt).
                 */
                indexManager.shutdown();

            }
        
        }

    }

}
