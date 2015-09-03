package com.bigdata.bop.fed;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.util.config.NicUtil;

/**
 * Default {@link IQueryEngineFactory} implementation.
 * 
 * @author bryan
 */
public class QueryEngineFactoryBase implements IQueryEngineFactory {

	private static final Logger log = Logger.getLogger(QueryEngineFactoryBase.class);

	// Note: needs to be public for the ClassPathUtil indirection mechanism.  However
	// callers MUST use QueryEngineFactory.getInstance() rather than directly using
	// this constructor.
	public QueryEngineFactoryBase() {
	}

    /**
     * Weak value cache to enforce the singleton pattern for standalone
     * journals.
     * <p>
     * Note: The backing hard reference queue is disabled since we do not want
     * to keep any {@link QueryEngine} objects wired into the cache unless the
     * application is holding a hard reference to the {@link QueryEngine}.
     */
    private static ConcurrentWeakValueCache<IBTreeManager, QueryEngine> standaloneQECache = new ConcurrentWeakValueCache<IBTreeManager, QueryEngine>(
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

    /* (non-Javadoc)
	 * @see com.bigdata.bop.fed.IQueryEngineFactory#getExistingQueryController(com.bigdata.journal.IBTreeManager)
	 */
    @Override
	public QueryEngine getExistingQueryController(
            final IBTreeManager indexManager) {

        if (indexManager instanceof IBigdataFederation<?>) {

            return federationQECache.get((IBigdataFederation<?>) indexManager);
            
        }
        // Note: Also supports TemporaryStore.
        return standaloneQECache.get(indexManager);
        
    }

    /* (non-Javadoc)
	 * @see com.bigdata.bop.fed.IQueryEngineFactory#getQueryController(com.bigdata.journal.IIndexManager)
	 */
    @Override
	public QueryEngine getQueryController(final IIndexManager indexManager) {

        if (indexManager instanceof IBigdataFederation<?>) {

            return getFederatedQueryController((IBigdataFederation<?>) indexManager);
            
        }
        // Note: Also supports TemporaryStore.
        return getStandaloneQueryController((IBTreeManager) indexManager);
        
    }

    /* (non-Javadoc)
	 * @see com.bigdata.bop.fed.IQueryEngineFactory#getStandaloneQueryController(com.bigdata.journal.IBTreeManager)
	 */
    @Override
	public QueryEngine getStandaloneQueryController(
            final IBTreeManager indexManager) {

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
    protected QueryEngine newStandaloneQueryEngine(
            final IBTreeManager indexManager) {

        if (log.isInfoEnabled())
            log.info("Initializing query engine: " + indexManager);

        final QueryEngine queryEngine = new QueryEngine(indexManager);

        queryEngine.init();

        return queryEngine;

    }
    
    /* (non-Javadoc)
	 * @see com.bigdata.bop.fed.IQueryEngineFactory#getFederatedQueryController(com.bigdata.service.IBigdataFederation)
	 */
    @Override
	public FederatedQueryEngine getFederatedQueryController(
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
     * 
     *         TODO Parameterize the local resource service and temporary
     *         storage. For example, maybe it should be using a journal backed
     *         by a MemStore?
     */
    private FederatedQueryEngine newFederatedQueryEngine(
            final IBigdataFederation<?> fed) {

		if (log.isInfoEnabled())
			log.info("Initiallizing query engine: " + fed);

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

                queryEngineStore = new Journal(p) {

                    /**
                     * Locator resources on the federation NOT the embedded
                     * Journal used by the query controller.
                     */
                    @Override
                    protected IResourceLocator<?> newResourceLocator() {
                    
                        return fed.getResourceLocator();
                        
                    }
                    
                };

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
    private class FederatedQueryController extends FederatedQueryEngine {

        /** The local persistence store for the query engine. */
        final Journal queryEngineStore;

        /** The local {@link ResourceService} for the query engine. */
        final ManagedResourceService queryEngineResourceService;

        /**
         * @param thisService
         * @param fed
         * @param indexManager
         * @param resourceService
         */
        public FederatedQueryController(final UUID thisService,
                final IBigdataFederation<?> fed, final Journal indexManager,
                final ManagedResourceService resourceService) {

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

    /* (non-Javadoc)
	 * @see com.bigdata.bop.fed.IQueryEngineFactory#getQueryControllerCount()
	 */
    @Override
	public int getQueryControllerCount() {

        return standaloneQECache.size() + federationQECache.size();

    }
    
}
