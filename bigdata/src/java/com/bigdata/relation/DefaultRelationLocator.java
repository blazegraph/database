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
/*
 * Created on Jun 30, 2008
 */

package com.bigdata.relation;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.SparseRowStore;

/**
 * Generic implementation relies on a ctor for the relation with the following
 * method signature:
 * 
 * <pre>
 * public RELATION ( ExecutorService service, IIndexManager indexManager, String namespace, Long timestamp, Properties properties )
 * </pre>
 * 
 * This {@link IRelationLocator} can be chained after a locator that knows about
 * a specific relation, e.g., on a {@link TemporaryStore} ({@link TemporaryStoreRelationLocator})
 * or a {@link Journal} ({@link JournalRelationLocator}).
 * 
 * @todo add atomic "create if not found" operation - requires operation with
 *       those semantics on the {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <R>
 *            The generic type of the [R]elation.
 */
public class DefaultRelationLocator<R> extends
        AbstractCachingRelationLocator<R> {

    protected static final transient Logger log = Logger.getLogger(DefaultRelationLocator.class);
    
    protected final transient ExecutorService service;
    
    protected final transient IIndexManager indexManager;

    /**
     * Provides locks on a per-namespace basis for higher concurrency.
     */
    private final transient NamedLock<String> namedLock = new NamedLock<String>();

    public DefaultRelationLocator(ExecutorService service, Journal journal) {

        this(service, (IIndexManager)journal);

    }

    /**
     * Used by {@link TemporaryStoreRelationLocator}.
     * 
     * @param service
     * @param tempStore
     */
    protected DefaultRelationLocator(ExecutorService service, TemporaryStore tempStore) {

        this(service, (IIndexManager)tempStore);

    }

    public DefaultRelationLocator(IBigdataFederation fed) {

        this(fed.getThreadPool(), (IIndexManager)fed);

    }

    public DefaultRelationLocator(ExecutorService service, AbstractTask task) {

        // @todo is the "client"'s ExecutorService available to the task?  It should be.
        this(service, (IIndexManager) task.getJournal());

    }

    /**
     * Designated constructor - all others delegate to this ctor.
     * 
     * @param service
     * @param indexManager
     */
    public DefaultRelationLocator(ExecutorService service, IIndexManager indexManager) {

        if (service == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.service = service;

        this.indexManager = indexManager;
        
    }

    public IRelation<R> getRelation(IRelationName<R> relationName,
            long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();
        
        final String namespace = relationName.toString();

        if(log.isInfoEnabled()) {
            
            log.info("namespace=" + namespace);

        }

        final Lock lock = namedLock.acquireLock(namespace);

        try {

            IRelation<R> relation = get(namespace, timestamp);
            
            if(relation != null) {
                
                if(log.isInfoEnabled()) {
                    
                    log.info("cached: "+relation);
                    
                }
                
                return relation;
                
            }
            
            final Properties properties = getProperties(namespace);

            if (properties == null) {

                throw new IllegalStateException("Not found: namespace="
                        + namespace);

            }

            if (log.isDebugEnabled()) {

                log.debug(properties.toString());

            }

            // can throw a ClassCastException.
            final String className = properties.getProperty(RelationSchema.CLASS);

            if (className == null) {

                throw new IllegalStateException("Not found: namespace="
                        + namespace + ", property=" + RelationSchema.CLASS);

            }

            final Class<? extends IRelation<R>> cls;
            try {

                cls = (Class<? extends IRelation<R>>) Class.forName(className);

            } catch (ClassNotFoundException e) {

                throw new RuntimeException(e);

            }

            if (log.isDebugEnabled()) {

                log.debug("Implementation class=" + cls.getName());

            }

            relation = newInstance(cls, service, indexManager, namespace,
                    timestamp, properties);

            if(log.isInfoEnabled()) {
                
                log.info("new instance: "+relation);
                
            }
            
            // add to the cache.
            put(relation);
            
            return relation;

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Return the {@link Properties} that will be used to configure the
     * {@link IRelation} instance. The {@link RelationSchema#CLASS} property
     * MUST be defined and specified the runtime class that will be
     * instantiated.
     * 
     * @param namespace
     * @param timestamp
     * @return
     */
    protected Properties getProperties(String namespace) {

        if(log.isInfoEnabled()) {
            
            log.info("namespace="+namespace);
            
        }
        
        /*
         * Note: This is a READ_COMMITTED request. The [timestamp] does not get
         * passed in as it has different semantics for the row store!  (Caching here may be useful.)
         */
        final Map<String,Object> map = getRowStore().read(RelationSchema.INSTANCE, namespace);

        if (map == null) {

            if(log.isDebugEnabled()) {
                
                log.debug("No properties: namespace="+namespace);
                
            }
            
            return null;
            
        }

        final Properties properties = new Properties();

        properties.putAll(map);

        if (log.isDebugEnabled()) {
            
            log.debug("Read properties: namespace=" + namespace + " :: "
                    + properties);
            
        }

        return properties;

    }
    
    private SparseRowStore getRowStore() {

        return indexManager.getGlobalRowStore();

    }
    
    /**
     * Core impl (all other methods should delegate to this one).
     * 
     * @param service
     *            A service to which you can submit tasks not requiring
     *            concurrency control.
     * @param indexManager
     *            The index views will be obtained from the {@link Journal}.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     * @param properties
     *            Configuration properties for the relation.
     * 
     * @return The {@link IRelation}.
     */
    public IRelation<R> newInstance(Class<? extends IRelation<R>> cls,
            ExecutorService service, IIndexManager indexManager,
            String namespace, long timestamp, Properties properties) {

        if (cls == null)
            throw new IllegalArgumentException();
        if (service == null)
            throw new IllegalArgumentException();
        if (indexManager == null)
            throw new IllegalArgumentException();
        if (namespace == null)
            throw new IllegalArgumentException();
        if (properties == null)
            throw new IllegalArgumentException();

        final Constructor<? extends IRelation<R>> ctor;
        try {

            ctor = cls.getConstructor(new Class[] {//
                    ExecutorService.class,// 
                    IIndexManager.class,//
                    String.class,// relation namespace
                    Long.class, // timestamp of the view
                    Properties.class // configuration properties.
                    });

        } catch (Exception e) {

            throw new RuntimeException("No appropriate ctor?: cls="
                    + cls.getName() + " : " + e, e);

        }

        final IRelation<R> r;
        try {

            r = ctor.newInstance(new Object[] { service, indexManager,
                    namespace, timestamp, properties });

            return r;

        } catch (Exception ex) {

            throw new RuntimeException("Could not instantiate relation: " + ex,
                    ex);

        }

    }

}
