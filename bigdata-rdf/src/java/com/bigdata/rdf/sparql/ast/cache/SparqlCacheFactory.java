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
 * Created on Mar 26, 2012
 */

package com.bigdata.rdf.sparql.ast.cache;

import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.QueryEngine;

/**
 * A factory pattern for the {@link SparqlCache}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SparqlCacheFactory {

    private static final Logger log = Logger
            .getLogger(SparqlCacheFactory.class);

    /**
     * Weak key cache to enforce the singleton pattern.
     * <p>
     * Note: We do not want to keep any {@link SparqlCache} objects wired into
     * the cache unless the application is holding a hard reference to the
     * {@link QueryEngine}.
     */
    private static WeakHashMap<QueryEngine, SparqlCache> instanceCache = new WeakHashMap<QueryEngine, SparqlCache>();

    /**
     * Singleton factory test (does not create the sparql cache).
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     * 
     * @return The query controller iff one has been obtained from the factory
     *         and its weak reference has not been cleared.
     */
    static public ISparqlCache getExistingSparqlCache(
            final QueryEngine queryEngine) {

        return instanceCache.get(queryEngine);
        
    }

    /**
     * Singleton factory.
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     *            
     * @return The {@link SparqlCache}.
     */
    static public ISparqlCache getSparqlCache(final QueryEngine queryEngine) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        SparqlCache sparqlCache = instanceCache.get(queryEngine);

        if (sparqlCache == null) {

            synchronized (instanceCache) {

                if ((sparqlCache = instanceCache.get(queryEngine)) == null) {

                    sparqlCache = newSparqlCache(queryEngine);
                    
                    instanceCache.put(queryEngine, sparqlCache);
                    
                }

            }

        }

        return sparqlCache;

    }

    /**
     * Initialize a new {@link SparqlCache} instance (or a connection to a
     * SPARQL cache fabric).
     * 
     * @param queryEngine
     *            The query controller.
     * 
     * @return The new {@link SparqlCache}.
     */
    private static SparqlCache newSparqlCache(final QueryEngine queryEngine) {

        if (log.isInfoEnabled())
            log.info("Initiallizing: " + queryEngine);

        final SparqlCache sparqlCache = new SparqlCache(queryEngine);

        sparqlCache.init();

        return sparqlCache;

    }
    
    /**
     * Return the #of live query controllers.
     */
    public static int getSparqlCacheCount() {

        return instanceCache.size();

    }
    
}
