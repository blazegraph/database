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

package com.bigdata.relation.locator;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.SparseRowStore;

/**
 * Generic implementation relies on a ctor for the resource with the following
 * method signature:
 * 
 * <pre>
 * public NAME ( ExecutorService service, IIndexManager indexManager, String namespace, Long timestamp, Properties properties )
 * </pre>
 * 
 * <p>
 * A relation is located using the {@link IIndexStore#getGlobalRowStore()} and
 * materialized by supplying an {@link IIndexManager} that will be able to
 * resolve the indices for the relation's view. Several different contexts are
 * handled:
 * <dl>
 * 
 * <dt>{@link IBigdataFederation}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link IBigdataFederation#getGlobalRowStore()} and the
 * {@link IBigdataFederation} as its {@link IIndexManager}. The makes access to
 * a remote and potentially distributed {@link IIndex} transparent to the
 * {@link IRelation}. However, it is NOT possible to resolve local resources on
 * other JVMs - only scale-out indices registered against the
 * {@link IBigdataFederation}.</dd>
 * 
 * <dt>{@link Journal}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link Journal#getGlobalRowStore()} and will use the local index objects
 * directly.</dd>
 * 
 * <dt>{@link AbstractTask}</dt>
 * 
 * <dd>If the index is local and monolithic then you can declare the index to
 * the {@link AbstractTask} and <em>override</em> the locator to use
 * {@link AbstractTask#getJournal()} as its index manager. This will give you
 * access to the local index object from within the concurrency control
 * mechanism</dd>
 * 
 * <dt>{@link TemporaryStore}</dt>
 * 
 * <dd>When used by itself, this is just like a {@link Journal}. However,
 * {@link TemporaryStore}s are also used to provide local resources for more
 * efficient data storage for a variety of purposes. When used in this manner,
 * you must explicitly notify the locator of the existence of the
 * {@link TemporaryStore} in order to be able to resolve {@link IRelation}s on
 * the {@link TemporaryStore}. It is prudent to use a prefix for such local
 * resources that guarentees uniqueness, e.g., an {@link UUID}</dd>
 * 
 * </dl>
 * 
 * @todo add atomic "create if not found" operation - requires operation with
 *       those semantics on the {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the [R]elation.
 */
public class DefaultResourceLocator<T extends ILocatableResource> extends
        AbstractCachingResourceLocator<T> implements IResourceLocator<T> {

    protected static final transient Logger log = Logger
            .getLogger(DefaultResourceLocator.class);

    protected final transient ExecutorService service;

    protected final transient IIndexManager indexManager;

    private final IResourceLocator<T> delegate;
    
    /**
     * Provides locks on a per-namespace basis for higher concurrency.
     */
    private final transient NamedLock<String> namedLock = new NamedLock<String>();

//    public DefaultResourceLocator(IBigdataFederation fed) {
//
//        this(fed.getThreadPool(), (IIndexManager) fed, null/*delegate*/);
//
//    }

//    public DefaultResourceLocator(ExecutorService service, AbstractTask task) {
//
//        // @todo is the "client"'s ExecutorService available to the task? It
//        // should be.
//        this(service, (IIndexManager) task.getJournal());
//
//    }

    /**
     * Designated constructor - all others delegate to this ctor.
     * 
     * @param service
     * @param indexManager
     * @param delegate
     *            Optional {@link IResourceLocator} to which unanswered requests
     *            are then delegated.
     */
    public DefaultResourceLocator(ExecutorService service,
            IIndexManager indexManager, IResourceLocator<T> delegate) {

        if (service == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.service = service;

        this.indexManager = indexManager;

        this.delegate = delegate;// MAY be null.
        
    }

    public T locate(IResourceIdentifier<T> identifier, long timestamp) {

        if (identifier == null)
            throw new IllegalArgumentException();

        final String namespace = identifier.toString();

        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            // test cache.
            T resource = get(namespace, timestamp);

            if (resource != null) {

                if (log.isInfoEnabled()) {

                    log.info("cached: " + resource);

                }

                return resource;

            }

            /*
             * First, test this locator, including any [seeAlso] IIndexManager
             * objects.
             */
            Properties properties = locateResource(namespace);
            
            if (properties == null) {

                // Not found by this locator.
                
                if(delegate != null) {
                    
                    /*
                     * A delegate was specified, so see if the delegate can
                     * resolve this request.
                     */
                    
                    if(log.isInfoEnabled()) {
                        
                        log.info("Not found - passing to delegate: namespace="
                                + namespace + ", timestamp=" + timestamp);
                        
                    }
                    
                    // pass request to delegate.
                    resource = delegate.locate(identifier, timestamp);
                    
                    if (resource != null) {

                        if (log.isInfoEnabled()) {

                            log.info("delegate answered: " + resource);

                        }

                        return resource;

                    }

                }
                
                throw new IllegalStateException("Not found: namespace="
                        + namespace);

            }

            if (log.isDebugEnabled()) {

                log.debug(properties.toString());

            }

            // can throw a ClassCastException.
            final String className = properties
                    .getProperty(RelationSchema.CLASS);

            if (className == null) {

                throw new IllegalStateException("Not found: namespace="
                        + namespace + ", property=" + RelationSchema.CLASS);

            }

            final Class<? extends T> cls;
            try {

                cls = (Class<? extends T>) Class.forName(className);

            } catch (ClassNotFoundException e) {

                throw new RuntimeException(e);

            }

            if (log.isDebugEnabled()) {

                log.debug("Implementation class=" + cls.getName());

            }

            // create a new instance of the relation.
            resource = newInstance(cls, service, indexManager, namespace,
                    timestamp, properties);

            if (log.isInfoEnabled()) {

                log.info("new instance: " + resource);

            }

            // add to the cache.
            put(resource);

            return resource;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Note: Caller is synchronized for this <i>namespace</i>.
     * 
     * @param namespace
     * 
     * @return
     */
    protected Properties locateResource(String namespace) {

        Properties properties = null;

        synchronized (seeAlso) {

            for (IIndexManager indexManager : seeAlso.keySet()) {

                /*
                 * read properties from the global row store for the default
                 * index manager.
                 */
                try {
                
                    properties = locateResourceOn(indexManager, namespace);
                    
                } catch(IllegalStateException t) {
                    
                    log.warn("Closed? " + indexManager);

                    continue;
                    
                } catch (Throwable t) {

                    log.error(t, t);
                    
                    continue;
                    
                }

                if (properties != null) {

                    if (log.isInfoEnabled()) {

                        log.info("Found: namespace=" + namespace + " on "
                                + indexManager);

                    }

                    return properties;

                }

            }

        }

        if (properties == null && indexManager != null) {

            /*
             * read properties from the global row store for the default index
             * manager.
             */
            properties = locateResourceOn(indexManager, namespace);

            if (properties != null) {

                if (log.isInfoEnabled()) {

                    log.info("Found: namespace=" + namespace + " on "
                            + indexManager);

                }

                return properties;

            }

        }

        return properties;

    }

    /**
     * Return the {@link Properties} that will be used to configure the
     * {@link IRelation} instance. The {@link RelationSchema#CLASS} property
     * MUST be defined and specified the runtime class that will be
     * instantiated.
     * <p>
     * Note: A <code>null</code> return is an indication that the resource was
     * NOT FOUND on this {@link IIndexManager} and the caller SHOULD try another
     * {@link IIndexManager}.
     * 
     * @param namespace
     *            The resource identifier - this is the primary key.
     * 
     * @return The {@link Properties} iff there is a logical row for the given
     *         namespace.
     */
    protected Properties locateResourceOn(IIndexManager indexManager,
            String namespace) {

        if (log.isInfoEnabled()) {

            log.info("indexManager="+indexManager+", namespace=" + namespace);

        }

        /*
         * Note: This is a READ_COMMITTED request. The [timestamp] does not get
         * passed in as it has different semantics for the row store! (Caching
         * here may be useful.)
         */
        final Map<String, Object> map = indexManager.getGlobalRowStore().read(
                RelationSchema.INSTANCE, namespace);

        if (map == null) {

            if (log.isDebugEnabled()) {

                log.debug("No properties: indexManager=" + indexManager
                        + ", namespace=" + namespace);

            }

            return null;

        }

        final Properties properties = new Properties();

        properties.putAll(map);

        if (log.isDebugEnabled()) {

            log.debug("Read properties: indexManager=" + indexManager
                    + ", namespace=" + namespace + " :: " + properties);

        }

        return properties;

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
     * @return A new instance of the identifed resource.
     */
    public T newInstance(Class<? extends T> cls, ExecutorService service,
            IIndexManager indexManager, String namespace, long timestamp,
            Properties properties) {

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

        final Constructor<? extends T> ctor;
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

        final T r;
        try {

            r = ctor.newInstance(new Object[] { service, indexManager,
                    namespace, timestamp, properties });

            return r;

        } catch (Exception ex) {

            throw new RuntimeException("Could not instantiate relation: " + ex,
                    ex);

        }

    }

    /**
     * Causes the {@link IIndexManager} to be tested when attempting to resolve
     * {@link IResourceIdentifier}s. The {@link IIndexManager} will be
     * automatically cleared from the set of {@link IIndexManager}s to be
     * tested if its reference is cleared by the JVM. If it becomes closed
     * asynchronously then a warning will be logged until its reference is
     * cleared.
     * <p>
     * Note: The default {@link IIndexManager} specified to the ctor normally
     * ensures that the global namespace is consistent (it is not possible to
     * have two indices or {@link ILocatableResource}s with the same name).
     * When you add additional {@link IIndexManager}s, the opportunity for an
     * <em>inconsistent</em> unified namespace is introduced. You can protect
     * yourself by ensuring that resources located on {@link TemporaryStore}s
     * and the like are always created with a unique prefix, e.g., the
     * {@link UUID} of the {@link TemporaryStore} itself.
     * 
     * @param indexManager
     *            Typically a {@link TemporaryStore} or {@link Journal}
     *            containing local resources.
     * 
     * @see #locateResource(String)
     */
    public void add(IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        synchronized (seeAlso) {

            /*
             * Note: weak reference keys, nothing stored under the key.
             */
            seeAlso.put(indexManager, null);

            if (log.isInfoEnabled()) {

                log.info("size=" + seeAlso.size() + ", added indexManager="
                        + indexManager);

            }

        }

    }

    private final WeakHashMap<IIndexManager, Void> seeAlso = new WeakHashMap<IIndexManager, Void>();

}
