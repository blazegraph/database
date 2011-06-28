/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 30, 2010
 */

package com.bigdata.bop.fed;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.util.config.NicUtil;

/**
 * Singleton factory for a query controller.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryEngineFactory {

    /**
     * Weak value cache to enforce the singleton pattern for standalone
     * journals.
     * <p>
     * Note: The backing hard reference queue is disabled since we do not want
     * to keep any {@link QueryEngine} objects wired into the cache unless the
     * application is holding a hard reference to the {@link QueryEngine}.
     */
    private static ConcurrentWeakValueCache<Journal, QueryEngine> standaloneQECache = new ConcurrentWeakValueCache<Journal, QueryEngine>(
            0/* queueCapacity */
    );

    /**
     * Weak value cache to enforce the singleton pattern for
     * {@link IBigdataClient}s (the data services are query engine peers rather
     * than controllers and handle their own query engine initialization so as
     * to expose their resources to other peers).
     * <p>
     * Note: The backing hard reference queue is disabled since we do not want
     * to keep any {@link QueryEngine} objects wired into the cache unless the
     * application is holding a hard reference to the {@link QueryEngine}.
     */
    private static ConcurrentWeakValueCache<IBigdataFederation<?>, FederatedQueryEngine> federationQECache = new ConcurrentWeakValueCache<IBigdataFederation<?>, FederatedQueryEngine>(
            0/* queueCapacity */
    );

    /**
     * Singleton factory test (does not create the query controller) for
     * standalone or scale-out.
     * 
     * @param indexManager
     *            The database.
     * 
     * @return The query controller iff one has been obtained from the factory
     *         and its weak reference has not been cleared.
     */
    static public QueryEngine getExistingQueryController(
            final IIndexManager indexManager) {

        if (indexManager instanceof IBigdataFederation<?>) {

            return federationQECache.get((IBigdataFederation<?>) indexManager);
            
        }
        
        return standaloneQECache.get((Journal)indexManager);
        
    }

    /**
     * Singleton factory for standalone or scale-out.
     * 
     * @param indexManager
     *            The database.
     *            
     * @return The query controller.
     */
    static public QueryEngine getQueryController(final IIndexManager indexManager) {

        if (indexManager instanceof IBigdataFederation<?>) {

            return getFederatedQueryController((IBigdataFederation<?>) indexManager);
            
        }
        
        return getStandaloneQueryController((Journal) indexManager);
        
    }

    /**
     * Singleton factory for standalone.
     * 
     * @param indexManager
     *            The journal.
     *            
     * @return The query controller.
     */
    static public QueryEngine getStandaloneQueryController(
            final Journal indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        QueryEngine queryEngine = standaloneQECache.get(indexManager);

        if (queryEngine == null) {

            synchronized (standaloneQECache) {

                if ((queryEngine = standaloneQECache.get(indexManager)) == null) {

                    queryEngine = newStandaloneQueryEngine(indexManager);
                    
                    standaloneQECache.put(indexManager, queryEngine);
                    
                }

            }

        }

        return queryEngine;

    }

    /**
     * Initialize a new query engine for the journal.
     * 
     * @param indexManager
     *            The journal.
     *            
     * @return The new query engine.
     */
    private static QueryEngine newStandaloneQueryEngine(
            final Journal indexManager) {

        final QueryEngine queryEngine = new QueryEngine(indexManager);

        queryEngine.init();

        return queryEngine;

    }
    
    /**
     * New query controller for scale-out.
     * 
     * @param fed
     *            The federation.
     *            
     * @return The query controller.
     * 
     * @todo parameterize the local resource service and temporary storage.
     */
    static public FederatedQueryEngine getFederatedQueryController(
            final IBigdataFederation<?> fed) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        FederatedQueryEngine queryEngine = federationQECache.get(fed);

        if (queryEngine == null) {

            synchronized (federationQECache) {

                if ((queryEngine = federationQECache.get(fed)) == null) {

                    queryEngine = newFederatedQueryEngine(fed);
                    
                    federationQECache.put(fed, queryEngine);
                    
                }

            }

        }

        return queryEngine;

    }

    /**
     * Initialize a new query engine for the federation.
     * 
     * @param fed
     *            The federation.
     *            
     * @return The new query engine.
     */
    private static FederatedQueryEngine newFederatedQueryEngine(
            final IBigdataFederation<?> fed) {

        final FederatedQueryEngine queryEngine;
        
        // The local resource service for the query controller.
        ManagedResourceService queryEngineResourceService = null;

        // The local persistence store for the query controller.
        Journal queryEngineStore = null;

        try {

            // Create index manager for the query controller.
            {

                final Properties p = new Properties();

                p.setProperty(Journal.Options.BUFFER_MODE,
                        BufferMode.Temporary.toString());

                p.setProperty(Journal.Options.CREATE_TEMP_FILE,
                        "true");

                queryEngineStore = new Journal(p);

            }

            // create resource service for the query controller.
            {
                queryEngineResourceService = new ManagedResourceService(
                        new InetSocketAddress(InetAddress
                                .getByName(NicUtil.getIpAddress(
                                        "default.nic", "default",
                                        true/* loopbackOk */)), 0/* port */
                        ), 0/* requestServicePoolSize */) {

                    @Override
                    protected File getResource(UUID uuid)
                            throws Exception {
                        // Will not serve up files.
                        return null;
                    }
                };
            }

            // create the query controller.
            queryEngine = new FederatedQueryController(fed
                    .getServiceUUID(), fed, queryEngineStore,
                    queryEngineResourceService);

        } catch (Throwable t) {

            if (queryEngineStore != null)
                queryEngineStore.destroy();

            if (queryEngineResourceService != null)
                queryEngineResourceService.shutdownNow();

            throw new RuntimeException(t);

        }

        queryEngine.init();

        return queryEngine;
        
    }
    
    /**
     * Implementation manages its own local storage and resource service.
     */
    private static class FederatedQueryController extends FederatedQueryEngine {

        /** The local persistence store for the {@link #queryEngine}. */
        final Journal queryEngineStore;

        /** The local {@link ResourceService} for the {@link #queryEngine}. */
        final ManagedResourceService queryEngineResourceService;

        /**
         * @param thisService
         * @param fed
         * @param indexManager
         * @param resourceService
         */
        public FederatedQueryController(UUID thisService,
                IBigdataFederation<?> fed, Journal indexManager,
                ManagedResourceService resourceService) {

            super(thisService, fed, indexManager, resourceService);

            this.queryEngineStore = indexManager;
            
            this.queryEngineResourceService = resourceService;
            
        }

        @Override
        public void shutdown() {
            super.shutdown();
            queryEngineResourceService.shutdown();
            tearDown();
        }

        @Override
        public void shutdownNow() {
            super.shutdownNow();
            queryEngineResourceService.shutdownNow();
            tearDown();
        }

        private void tearDown() {
            queryEngineStore.destroy();
        }

    }

    /**
     * Return the #of live query controllers.
     */
    public static int getQueryControllerCount() {

        return standaloneQECache.size() + federationQECache.size();

    }
    
}
