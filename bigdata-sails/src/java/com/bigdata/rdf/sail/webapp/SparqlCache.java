/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 25, 2012
 */

package com.bigdata.rdf.sail.webapp;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemoryManager;

/**
 * A SPARQL cache.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href="http://aksw.org/Projects/QueryCache"> Adaptive SPARQL Query
 *      Cache </a>
 * 
 * @see <a
 *      href="http://www.informatik.uni-leipzig.de/~auer/publication/caching.pdf
 *      > Improving the Performance of Semantic Web Applications with SPARQL
 *      Query Caching </a>
 * 
 *      TODO Limit on {@link MemoryManager} via {@link ConfigParams}. Flush
 *      older objects from cache if the {@link MemoryManager} limit would be
 *      exceeded.
 * 
 *      TODO Chain to the {@link QueryServlet}. It might be easiest to parse the
 *      query first, then chain to this servlet if we discover that it is a
 *      DESCRIBE query. Chaining the parsed query could be a strategy which is
 *      useful in general.
 * 
 *      TODO Intercept and cache <code>DESCRIBE ?s</code> query results.
 * 
 *      TODO Listen for and process deltas for DESCRIBEd objects. We need to
 *      group deltas by the subject and by the object, publishing them twice (if
 *      we want to keep both the forward attributes/links and the reverse links
 *      current in the cache). The delta can come from the {@link IChangeLog}
 *      listener. This will require registering an {@link IChangeLog} listener
 *      against the {@link BigdataSail} connections for both SPARQL UPDATE and
 *      the NSS mutation methods.
 * 
 *      TODO Listen for updates to statement patterns and invalide SPARQL result
 *      sets when a triple in a statement pattern in use by the query for that
 *      solution set has been added or removed.
 *      <p>
 *      General match of solution sets should be based on the hash code of the
 *      SPARQL query or the deep hash code of a normalized and optimized AST.
 *      Detailed match must be on either the query text or the AST (deep
 *      equals). AST based caching allows sub-select caching or even caching of
 *      sub-groups. That could be interesting.
 * 
 *      TODO Extract interface and write unit tests.
 * 
 *      TODO Benchmark impact of cache on BSBM explore+update. The cache should
 *      be integrated into the query planner so we can cache solution sets for
 *      sub-groups and sub-selects. However, when BINDINGS are present, then the
 *      query solutions are not the same as when they are not present. This
 *      makes the cache somewhat more difficult to integration since the same
 *      query is not always the same (e.g., include the hash of the exogenous
 *      solutions in the query hash code and we will get less reuse).
 */
public class SparqlCache {

    /**
     * The response body for a cached result is stored on the
     * {@link IMemoryManager}. This allows us to cache TBs of data in main
     * memory.
     * <p>
     * Note: A slight twist on the design would allow us to cache in both main
     * memory and on a backing {@link RWStore} (DISK). However, it would be
     * worth while to migrate records to the {@link RWStore} only if they were
     * expensive to compute and we had a reasonable expectation of reuse before
     * they would be invalidated by an update. In practice, it is probably
     * better to hash partition the cache.
     * <p>
     * A hash partitioned cache design could proceed readily with splitting the
     * cache results between two nodes when adding a node. Any such approach
     * presumes a circular hash function such as is common in distributed row
     * stores, etc.
     */
    private final IMemoryManager mmgr;

    public SparqlCache(final IMemoryManager mmgr) {

        if (mmgr == null)
            throw new IllegalArgumentException();

        this.mmgr = mmgr;

    }

    public void close() {

        mmgr.clear();

    }

    /**
     * Return the result from the cache -or- <code>null</code> if there is a
     * cache miss.
     * 
     * @param queryTask
     *            The query task.
     *            
     * @return The query result iff there is a cache hit.
     */
    public CacheHit get(final AbstractQueryTask queryTask) {

        /*
         * FIXME Implement. Start with a simple DESCRIBE <uri> cache.
         */
        return null;
        
    }
    
//    public static class DescribeCacheResult {
//
//        public final URI uri;
//
//        public final int recId;
//
//    }

    /**
     * A cache hit.
     */
    public static class CacheHit {

        /**
         * The Content-Type.
         */
        final String contentType;

        /**
         * The Content-Length.
         */
        final int contentLength;

        /**
         * The timestamp when the cache entry was created / last updated.
         */
        final long lastModified;

        /**
         * The cached response entity.
         */
        final byte[] data;

        public CacheHit(final String contentType, final long lastModified,
                final byte[] data) {
            this.contentType = contentType;
            this.contentLength = data.length;
            this.lastModified = lastModified;
            this.data = data;

        }

    }

}
