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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.IIndexManager;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for both transient and persistent {@link ITripleStore}
 * implementations using local storage.
 * <p>
 * This implementation presumes unisolated writes on local indices and a single
 * client writing on a local database. Unlike the {@link ScaleOutTripleStore}
 * this implementation does NOT feature auto-commit for unisolated writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    /**
     * @todo configure capacity using
     *       {@link IBigdataClient.Options#DEFAULT_CLIENT_THREAD_POOL_SIZE}
     *       semantics.
     */
    public AbstractLocalTripleStore(Properties properties) {
        
        super(properties);

        // client thread pool setup.
        final int threadPoolSize;
        {
         
            threadPoolSize = Integer.parseInt(properties.getProperty(
                    IBigdataClient.Options.CLIENT_THREAD_POOL_SIZE,
                    IBigdataClient.Options.DEFAULT_CLIENT_THREAD_POOL_SIZE));

            log.info(IBigdataClient.Options.CLIENT_THREAD_POOL_SIZE + "=" + threadPoolSize);

        }

        if (threadPoolSize == 0) {

            threadPool = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(DaemonThreadFactory
                            .defaultThreadFactory());

        } else {

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    threadPoolSize, DaemonThreadFactory.defaultThreadFactory());

        }
        
    }

    /**
     * Note: The local triple stores do not {@link IBigdataClient} and therefore
     * do not have access to the {@link IBigdataFederation#getThreadPool()}.
     * Therefore a local {@link ExecutorService} is created on a per-{@link AbstractLocalTripleStore}
     * basis.
     */
    private final ExecutorService threadPool;

    /**
     * Note: The capacity of the thread pool may be configured using the
     * {@link IBigdataClient.Options#CLIENT_THREAD_POOL_SIZE} property.
     */
    public ExecutorService getThreadPool() {
        
        return threadPool;
        
    }

    /**
     * Terminates the {@link #threadPool}.
     */
    protected void shutdown() {
            
        threadPool.shutdown();

        try {

            threadPool.awaitTermination(2, TimeUnit.SECONDS);

        } catch (InterruptedException ex) {

            log.warn("Write service did not terminate within timeout.");

        }
            
        super.shutdown();
        
    }
    
    /**
     * A factory returning the singleton for the {@link FullTextIndex}.
     */
    synchronized public FullTextIndex getSearchEngine() {

        if (searchEngine == null) {

            // FIXME namespace once used by localtriplestore.
            final String namespace = "";

            searchEngine = new FullTextIndex(getProperties(), namespace,
                    getStore(), 
                    getThreadPool()
//                    Executors
//                            .newSingleThreadExecutor(DaemonThreadFactory
//                                    .defaultThreadFactory())
                                    );

        }

        return searchEngine;

    }

    private FullTextIndex searchEngine;

    abstract IIndexManager getStore();

    public void abort() {
        
        searchEngine = null;

        
    }
    
}
