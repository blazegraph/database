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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.LRUCache;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.NT;

/**
 * Generic implementation relies on a ctor for the resource with the following
 * method signature:
 * 
 * <pre>
 * public NAME ( IIndexManager indexManager, String namespace, Long timestamp, Properties properties )
 * </pre>
 * 
 * <p>
 * A relation is located using the {@link IIndexStore#getGlobalRowStore(long)} and
 * materialized by supplying an {@link IIndexManager} that will be able to
 * resolve the indices for the relation's view. Several different contexts are
 * handled:
 * <dl>
 * 
 * <dt>{@link IBigdataFederation}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link IBigdataFederation#getGlobalRowStore(long)} and the
 * {@link IBigdataFederation} as its {@link IIndexManager}. The makes access to
 * a remote and potentially distributed {@link IIndex} transparent to the
 * {@link IRelation}. However, it is NOT possible to resolve local resources on
 * other JVMs - only scale-out indices registered against the
 * {@link IBigdataFederation}.</dd>
 * 
 * <dt>{@link Journal}</dt>
 * 
 * <dd>The {@link IRelation} will be resolved using the
 * {@link Journal#getGlobalRowStore(long)} and will use the local index objects
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the [R]elation.
 */
public class DefaultResourceLocator<T extends ILocatableResource> // 
        implements IResourceLocator<T> {

    protected static final transient Logger log = Logger
            .getLogger(DefaultResourceLocator.class);

    protected final transient IIndexManager indexManager;

    private final IResourceLocator<T> delegate;

    /**
     * Cache for recently located resources.
     */
    final private transient ConcurrentWeakValueCache<NT, T> resourceCache;

    /**
     * Cache for recently materialized properties from the GRS.
     */
    final /*private*/ transient ConcurrentWeakValueCache<NT, Map<String,Object>> propertyCache;

    /**
     * Provides locks on a per-namespace basis for higher concurrency.
     */
    private final transient NamedLock<String> namedLock = new NamedLock<String>();

    /**
     * The default #of recently located resources whose hard references will be
     * retained by the {@link LRUCache}.
     */
    protected static transient final int DEFAULT_CACHE_CAPACITY = 10;

    /**
     * The default timeout for stale entries in milliseconds.
     */
    protected static transient final long DEFAULT_CACHE_TIMEOUT = (10 * 1000);

    /**
     * Ctor uses {@link #DEFAULT_CACHE_CAPACITY} and
     * {@link #DEFAULT_CACHE_TIMEOUT}.
     * 
     * @param indexManager
     * @param delegate
     *            Optional {@link IResourceLocator} to which unanswered requests
     *            are then delegated.
     */
    public DefaultResourceLocator(final IIndexManager indexManager,
            final IResourceLocator<T> delegate) {
        
        this(indexManager, delegate, DEFAULT_CACHE_CAPACITY,
                DEFAULT_CACHE_TIMEOUT);
    
    }

    /**
     * 
     * @param indexManager
     * @param delegate
     *            Optional {@link IResourceLocator} to which unanswered requests
     *            are then delegated.
     * @param cacheCapacity
     *            The capacity of the internal weak value cache.
     * @param cacheTimeout
     *            The timeout in milliseconds for stale entries in that cache.
     */
    public DefaultResourceLocator(final IIndexManager indexManager,
            final IResourceLocator<T> delegate, final int cacheCapacity,
            final long cacheTimeout) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

        this.delegate = delegate;// MAY be null.

        if (cacheCapacity <= 0)
            throw new IllegalArgumentException();

        if (cacheTimeout < 0)
            throw new IllegalArgumentException();

        this.resourceCache = new ConcurrentWeakValueCacheWithTimeout<NT, T>(
                cacheCapacity, TimeUnit.MILLISECONDS.toNanos(cacheTimeout));

        this.propertyCache = new ConcurrentWeakValueCacheWithTimeout<NT, Map<String, Object>>(
                cacheCapacity, TimeUnit.MILLISECONDS.toNanos(cacheTimeout));

    }

    // @todo hotspot 2% total query time.
    public T locate(final String namespace, final long timestamp) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace + ", timestamp=" + timestamp);

        }

        T resource = null;
        final NT nt;

        /*
         * Note: The drawback with resolving the resource against the
         * [commitTime] is that the views will be the same object instance and
         * will have the timestamp associated with the [commitTime] rather than
         * the caller's timestamp. This breaks the assumption that
         * resource#getTimestamp() returns the transaction identifier for a
         * read-only transaction. In order to fix that, we resort to sharing the
         * Properties object instead of the resource view.
         */
//        if (TimestampUtility.isReadOnly(timestamp)
//                && indexManager instanceof Journal) {
//
//            /*
//             * If we are looking on a local Journal (standalone database) then
//             * we resolve the caller's [timestamp] to the commit point against
//             * which the resource will be located and handle caching of the
//             * resource using that commit point. This is done in order to share
//             * a read-only view of a resource with any request which would be
//             * serviced by the same commit point. Any such views are read-only
//             * and immutable.
//             */
//            
//            final Journal journal = (Journal) indexManager;
//
//            // find the commit record on which we need to read.
//            final long commitTime = journal.getCommitRecord(
//                    TimestampUtility.asHistoricalRead(timestamp))
//                    .getTimestamp();
//
//            nt = new NT(namespace, commitTime);
//
//        } else {

            nt = new NT(namespace, timestamp);
//
//        }

        // test cache: hotspot 93% of method time.
        resource = resourceCache.get(nt);

        if (resource != null) {

            if (log.isDebugEnabled())
                log.debug("cache hit: " + resource);

            // cache hit.
            return resource;

        }

        /*
         * Since there was a cache miss, acquire a lock for the named relation
         * so that the locate + cache.put sequence will be atomic.
         */
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            // test cache now that we have the lock.
            resource = resourceCache.get(nt);

            if (resource != null) {

                if (log.isDebugEnabled())
                    log.debug("cache hit: " + resource);

                return resource;

            }
            
            if (log.isInfoEnabled())
                log.info("cache miss: namespace=" + namespace + ", timestamp="
                        + timestamp);
          
            /*
             * First, test this locator, including any [seeAlso] IIndexManager
             * objects.
             */
            final AtomicReference<IIndexManager> foundOn = new AtomicReference<IIndexManager>();

            final Properties properties = locateResource(namespace, timestamp,
                    foundOn);
            
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
                    resource = delegate.locate(namespace, timestamp);
                    
                    if (resource != null) {

                        if (log.isInfoEnabled()) {

                            log.info("delegate answered: " + resource);

                        }

                        return resource;

                    }

                }

                if (log.isInfoEnabled())
                    log.info("Not found: namespace=" + namespace
                            + ", timestamp=" + timestamp);

                // not found.
                return null;

            }

            if (log.isDebugEnabled()) {

                log.debug(properties.toString());

            }

            // can throw a ClassCastException.
            final String className = properties.getProperty(RelationSchema.CLASS);

            if (className == null) {

                throw new IllegalStateException("Required property not found: namespace="
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
            resource = newInstance(cls, foundOn.get(), namespace, timestamp,
                    properties);

            // Add to the cache.
            resourceCache.put(nt, resource);

            return resource;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Note: Caller is synchronized for this <i>namespace</i>.
     * 
     * @param namespace
     *            The namespace for the resource.
     * @param timestamp
     * @param foundOn
     *            Used to pass back the {@link IIndexManager} on which the
     *            resource was found as a side-effect.
     * 
     * @return The properties for that resource.
     */
    protected Properties locateResource(final String namespace,
            final long timestamp, final AtomicReference<IIndexManager> foundOn) {

        synchronized (seeAlso) {

            for (IIndexManager indexManager : seeAlso.keySet()) {

                /*
                 * read properties from the global row store for the default
                 * index manager.
                 */
                Properties properties = null;

                try {

                    properties = locateResourceOn(indexManager, namespace,
                            timestamp);

                } catch (IllegalStateException t) {
                    
                    if(indexManager instanceof TemporaryStore) {

                        /*
                         * Note: Asynchronous close is common for temporary
                         * stores since they can be asynchronously closed
                         * and removed from the [seeAlso] weak value cache.
                         */
                        
                        if (log.isInfoEnabled())
                            log.info("Closed? " + indexManager);
                        
                    } else {
                        
                        /*
                         * Other stores should more typically remain open, but
                         * they could be closed asynchronously for valid
                         * reasons.
                         */
                        
                        log.warn("Closed? " + indexManager);
                        
                    }

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

                    // tell the caller _where_ we found the resource.
                    foundOn.set(indexManager);
                    
                    return properties;

                }

            }

        }

        /*
         * read properties from the global row store for the default index
         * manager.
         */

        final Properties properties = locateResourceOn(indexManager, namespace,
                timestamp);

        if (properties != null) {

            if (log.isInfoEnabled()) {

                log.info("Found: namespace=" + namespace + " on "
                        + indexManager);

            }

            // tell the caller _where_ we found the resource.
            foundOn.set(indexManager);

            return properties;

        }
        
        // will be null.
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
     * @param indexManager
     * @param namespace
     *            The resource identifier - this is the primary key.
     * @param timestamp
     *            The timestamp of the resource view.
     * 
     * @return The {@link Properties} iff there is a logical row for the given
     *         namespace.
     */
    protected Properties locateResourceOn(final IIndexManager indexManager,
            final String namespace, final long timestamp) {

        if (log.isInfoEnabled()) {

            log.info("indexManager=" + indexManager + ", namespace="
                    + namespace + ", timestamp=" + timestamp);

        }

        /*
         * Look at the global row store view corresponding to the specified
         * timestamp.
         * 
         * Note: caching here is important in order to reduce the heap pressure
         * associated with large numbers of concurrent historical reads against
         * the same commit point when those reads are performed within read-only
         * transactions and, hence, each read is performed with a DISTINCT
         * timestamp. Since the timestamps are distinct, the resource [cache]
         * will have cache misses. This code provides for a [propertyCache]
         * which ensures that we share the materialized properties from the GRS
         * across resource views backed by the same commit point (and also avoid
         * unnecessary GRS reads).
         */
        Long commitTime2 = null;
        final Map<String, Object> map; 
        if (TimestampUtility.isReadOnly(timestamp)
                && !TimestampUtility.isReadCommitted(timestamp)
                && indexManager instanceof Journal) {

            final Journal journal = (Journal) indexManager;

            // find the commit record on which we need to read.
            final ICommitRecord commitRecord = journal
                    .getCommitRecord(TimestampUtility
                            .asHistoricalRead(timestamp));

            if (commitRecord != null) {

                // find the timestamp associated with that commit record.
                final long commitTime = commitRecord.getTimestamp();

                // Save commitTime to stuff into the properties.
                commitTime2 = commitTime;
                
                // Check the cache before materializing the properties from the
                // GRS.
                final Map<String, Object> cachedMap = propertyCache.get(new NT(
                        namespace, commitTime));

                if (cachedMap != null) {

                    // The properties are in the cache.
                    map = cachedMap;

                } else {

                    // Use the GRS view as of that commit point.
                    final SparseRowStore rowStore = journal
                            .getGlobalRowStore(commitTime);

                    // Read the properties from the GRS.
                    map = rowStore == null ? null : rowStore.read(
                            RelationSchema.INSTANCE, namespace);

                    if (map != null) {

                        // Stuff the properties into the cache.
                        propertyCache.put(new NT(namespace, commitTime), map);

                    }

                }

            } else {

                /*
                 * No such commit record.
                 * 
                 * @todo We can probably just return [null] for this case.
                 */

                final SparseRowStore rowStore = indexManager
                        .getGlobalRowStore(/* timestamp */);

                // Read the properties from the GRS.
                map = rowStore == null ? null : rowStore.read(
                        RelationSchema.INSTANCE, namespace);
                
            }

        } else {

            /*
             * @todo The timestamp of the resource view is currently ignored.
             * This probably should be modified to use the corresponding view of
             * the global row store rather than always using the read-committed
             * / unisolated view, which will require exposing a
             * getGlobalRowStore(timestamp) method on IIndexStore.
             */

            final SparseRowStore rowStore = indexManager
                    .getGlobalRowStore(/* timestamp */);

            // Read the properties from the GRS.
            map = rowStore == null ? null : rowStore.read(
                    RelationSchema.INSTANCE, namespace);
            
        }

        if (map == null) {

            if (log.isDebugEnabled()) {

                log.debug("Not found: indexManager=" + indexManager
                        + ", namespace=" + namespace + ", timestamp="
                        + timestamp);

            }

            return null;

        }

        // wrap with properties object to prevent cross view mutation.
        final Properties properties = new Properties();

        properties.putAll(map);

        if (commitTime2 != null) {

            /*
             * Make the commit time against which we are reading accessible to
             * the locatable resource.
             */
            properties.put(RelationSchema.COMMIT_TIME, commitTime2);

        }
        
        if (log.isTraceEnabled()) {

            log.trace("Read properties: indexManager=" + indexManager
                    + ", namespace=" + namespace + ", timestamp=" + timestamp
                    + " :: " + properties);

        }

        return properties;

    }
   
    /**
     * Create a new view of the relation.
     * 
     * @param indexManager
     *            The {@link IIndexManager} that will be used to resolve the
     *            named indices for the relation.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     * @param properties
     *            Configuration properties for the relation.
     * 
     * @return A new instance of the identified resource.
     */
    protected T newInstance(final Class<? extends T> cls,
            final IIndexManager indexManager, final String namespace,
            final long timestamp, final Properties properties) {

        if (cls == null)
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

            r = ctor.newInstance(new Object[] {//
                    indexManager,//
                    namespace, //
                    timestamp, //
                    properties //
                    });

            r.init();
            
            if(log.isInfoEnabled()) {
                
                log.info("new instance: "+r);
                
            }
            
            return r;

        } catch (Exception ex) {

            throw new RuntimeException("Could not instantiate relation: " + ex,
                    ex);

        }

    }

    /**
     * Places the instance into the cache iff there is no existing instance in
     * the cache for the same resource and timestamp.
     * <p>
     * Note: This is done automatically by {@link AbstractResource}.
     * 
     * @param instance
     *            The instance.
     */
    public T putInstance(final T instance) {
      
        if (instance == null)
            throw new IllegalArgumentException();

        final String namespace = instance.getNamespace();
        
        final long timestamp = instance.getTimestamp();
        
        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace+", timestamp="+timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            final NT nt = new NT(namespace, timestamp);
            
            final T tmp = resourceCache.get(nt);

            if (tmp != null) {

                if(log.isInfoEnabled()) {
                    
                    log.info("Existing instance already in cache: "+tmp);
                    
                }
                
                return tmp;
                
            }
            
            resourceCache.put(nt, instance);

            if (log.isInfoEnabled()) {

                log.info("Instance added to cache: " + instance);
                
            }
            
            return instance;
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Resources that hold hard references to local index objects MUST be
     * discarded during abort processing. Otherwise the same resource objects
     * will be returned from the cache and buffered writes on the indices for
     * those relations (if they are local index objects) will still be visible,
     * thus defeating the abort semantics.
     */
    public void discard(final ILocatableResource<T> instance) {
        
        if (instance == null)
            throw new IllegalArgumentException();

        final String namespace = instance.getNamespace();
        
        final long timestamp = instance.getTimestamp();
        
        if (log.isInfoEnabled()) {

            log.info("namespace=" + namespace + ", timestamp=" + timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            final NT nt = new NT(namespace, timestamp);

            /*
             * Clear the resource cache, but we do not need to clear the
             * property cache since it only retains immutable historical state.
             */
            final boolean found = resourceCache.remove(nt) != null;

            if (log.isInfoEnabled()) {

                log.info("instance=" + instance + ", found=" + found);
                
            }
            
        } finally {
            
            lock.unlock();
            
        }        
        
    }
    
    /**
     * Causes the {@link IIndexManager} to be tested when attempting to resolve
     * a resource identifiers. The {@link IIndexManager} will be automatically
     * cleared from the set of {@link IIndexManager}s to be tested if its
     * reference is cleared by the JVM. If it becomes closed asynchronously then
     * a warning will be logged until its reference is cleared.
     * <p>
     * Note: The default {@link IIndexManager} specified to the ctor normally
     * ensures that the global namespace is consistent (it is not possible to
     * have two indices or {@link ILocatableResource}s with the same name).
     * When you add additional {@link IIndexManager}s, the opportunity for an
     * <em>inconsistent</em> unified namespace is introduced. You can protect
     * yourself by ensuring that resources located on {@link TemporaryStore}s
     * and the like are always created with a unique prefix, e.g., the
     * {@link UUID} of the {@link TemporaryStore} itself or a {@link UUID} for
     * each container that is allocated on the {@link TemporaryStore}.
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
