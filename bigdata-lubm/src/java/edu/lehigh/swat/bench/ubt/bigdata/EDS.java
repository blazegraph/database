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

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.EmbeddedFederation;

/**
 * Configuration for an {@link EmbeddedClient}.
 * <p>
 * Note: Opening and closing the {@link EmbeddedFederation} is a heavy
 * operation, roughly equivalent to starting and stopping an RDBMS. Further,
 * this operation can not be performed by the client for a distributed
 * federation!
 * <p>
 * Therefore this class opens the federation once. The caller will automatically
 * re-locate the {@link AbstractTripleStore} with the returned federation
 * instance. The client is disconnected from the federation when the helper
 * class is finalized.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EDS extends BigdataRepositoryFactory {

    public EDS(String database) {

        super(database);

    }

    @Override
    public IRepositoryLifeCycle newLifeCycle() {

        return new LifeCycleEDS();

    }

    protected class LifeCycleEDS implements
            IRepositoryLifeCycle<EmbeddedFederation> {

        private EmbeddedFederation fed;
        
        protected EmbeddedFederation connect() {

            if (fed == null) {

                final Properties properties = getProperties();

//                // This can be used to look for code that is doing point tests.
//                properties.setProperty(
//                        IBigdataClient.Options.CLIENT_BATCH_API_ONLY, "true");

                fed = new EmbeddedClient(properties).connect();
                
            }
            
            return fed;
            
        }
        
        public Properties getProperties() {
            
            return EDS.this.getProperties();
            
        }
        
        public EmbeddedFederation open() {

            return connect();

        }

        public void close(EmbeddedFederation indexManager) {

            if (indexManager.isOpen()) {
                
//                ((DistributedTransactionService) indexManager
//                        .getTransactionService()).snapshot();
//                
//              indexManager.shutdown();
                
            }

        }
        
        protected void finalize() throws Throwable {

            if (fed != null && fed.isOpen()) {

                // normal shutdown please.
                fed.shutdown();

            }

            super.finalize();

        }

    }

}
