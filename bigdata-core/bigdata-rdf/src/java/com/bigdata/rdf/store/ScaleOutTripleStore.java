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
/*
 * Created on May 19, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.IBigdataFederation;

/**
 * Implementation of an {@link ITripleStore} as a client of an
 * {@link IBigdataFederation}.
 * 
 * <h2>Deployment choices</h2>
 * 
 * The {@link JiniFederation} provides a scale-out deployment. The indices will
 * be dynamically key-range partitioned and will be automatically re-distributed
 * over the available nodes in the cluster.
 * 
 * <h2>Architecture</h2>
 * 
 * The client uses unisolated writes against the lexicon (terms and ids indices)
 * and the statement indices. The index writes are automatically broken down
 * into one split per index partition. While each unisolated write on an index
 * partition is ACID, the indices are fully consistent iff the total operation
 * is successful. For the lexicon, this means that the write on the terms and
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
 * Two additional mechanisms are used in order to guarantee reads from only
 * fully consistent data. First, clients providing query answering should read
 * from a database state that is known to be consistent (by using a read-only
 * transaction whose start time is the globally agreed upon commit time for that
 * database state). Second, if a client operation fails then it must be retried.
 * Such fail-safe retry semantics are available when data load operations are
 * executed as part of a map-reduce job.
 * <p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ScaleOutTripleStore.java 3186 2010-07-12 17:01:19Z thompsonbry
 *          $
 */
public class ScaleOutTripleStore extends AbstractTripleStore {

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
