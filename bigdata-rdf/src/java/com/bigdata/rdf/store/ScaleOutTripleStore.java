/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on May 19, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.load.ConcurrentDataLoader;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniFederation;

/**
 * Implementation of an {@link ITripleStore} as a client of an
 * {@link IBigdataFederation}.
 * 
 * <h2>Deployment choices</h2>
 * 
 * You can deploy the {@link ScaleOutTripleStore} using any
 * {@link IBigdataClient}.  An
 * {@link EmbeddedFederation} can be used if you want key-range partitioned
 * indices but plan to run on a single machine and do not want to incur the
 * overhead for RMI - all services will run in the same JVM. Finally, a
 * {@link JiniFederation} can be used if you want to use a scale-out deployment.
 * In this case indices will be key-range partitioned and will be automatically
 * re-distributed over the available resources.
 * 
 * <h2>Architecture</h2>
 * 
 * The client uses unisolated writes against the lexicon (terms and ids indices)
 * and the statement indices. The index writes are automatically broken down
 * into one split per index partition. While each unisolated write on an index
 * partition is ACID, the indices are fully consistent iff the total operation
 * is successfull. For the lexicon, this means that the write on the terms and
 * the ids index must both succeed. For the statement indices, this means that
 * the write on each access path must succeed. If a client fails while adding
 * terms, then it is possible for the ids index to be incomplete with respect to
 * the terms index (i.e., terms are mapped into the lexicon and term identifiers
 * are assigned but the reverse lookup by term identifier will not discover the
 * term). Likewise, if a client fails while adding statements, then it is
 * possible for some of the access paths to be incomplete with respect to the
 * other access paths (i.e., some statements are not present in some access
 * paths).
 * <p>
 * Two additional mechanisms are used in order to guarentee reads from only
 * fully consistent data. First, clients providing query answering should read
 * from a database state that is known to be consistent (by using a read-only
 * transaction whose start time is the globally agreed upon commit time for that
 * database state). Second, if a client operation fails then it must be retried.
 * Such fail-safe retry semantics are available when data load operations are
 * executed as part of a map-reduce job.
 * <p>
 * 
 * @todo provide a mechanism to make document loading robust to client failure.
 *       When loads are unisolated, a client failure can result in the
 *       statements being loaded into only a subset of the statement indices.
 *       robust load would require a means for undo or redo of failed loads. a
 *       loaded based on map/reduce would naturally provide a robust mechanism
 *       using a redo model. The {@link ConcurrentDataLoader} does much of this
 *       already.
 * 
 * @todo provide batching and synchronization for database at once and TM update
 *       scenarios with a distributed {@link ITripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScaleOutTripleStore extends AbstractTripleStore {

//    private final IBigdataFederation fed;
//    
//    /**
//     * The {@link IBigdataFederation} that is being used.
//     */
//    public IBigdataFederation getFederation() {
//        
//        return fed;
//        
//    }

    /**
     * Ctor specified by {@link DefaultResourceLocator}
     * <p>
     * Note: KB is NOT created automatically.
     */
    public ScaleOutTripleStore(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super( indexManager, namespace, timestamp, properties );

        /*
         * Note: The indexManager will be an IsolatedJournal if the resource is
         * instantiated from within an AbstractTask.
         */
        
//        this.fed = (IBigdataFederation) indexManager;
     
        /*
         * Note: KB is NOT created automatically.
         */
        
    }
    
//    public IBigdataFederation getIndexManager() {
//        
//        return fed;
//        
//    }
    
    final public boolean isStable() {

        return true;
        
//        return getIndexManager().isStable();
        
    }

//    /**
//     * Drops the indices for the {@link ITripleStore}.
//     */
//    public void __tearDownUnitTest() {
//        
//        destroy();
//        
//        super.__tearDownUnitTest();
//        
//    }

    /**
     * This store is safe for concurrent operations.
     */
    public boolean isConcurrent() {

        return true;
        
    }
    
}
