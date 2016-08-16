/*

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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation.locator;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.Iterator;
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
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.GlobalRowStoreHelper;
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
 *
 * @param <T>
 *            The generic type of the [R]elation.
 */
public class DefaultResourceLocator<T extends ILocatableResource<T>> //
        implements IResourceLocator<T> {

    protected static final transient Logger log = Logger
            .getLogger(DefaultResourceLocator.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    protected final transient IIndexManager indexManager;

    private final IResourceLocator<T> delegate;

    /**
     * Cache for recently located resources.
     */
    final private transient ConcurrentWeakValueCache<NT, T> resourceCache;

    /**
     * Cache for recently materialized properties from the GRS.
     * <p>
     * Note: Due to the manner in which the property sets are materialized from
     * the {@link GlobalRowStoreHelper}, items in this cache can not cause
     * references to indices or other persistence capable / high cost resources
     * to remain strongly reachable.
     */
    final /*private*/ transient ConcurrentWeakValueCache<NT, Map<String,Object>> propertyCache;

    /**
     * Special property used to record the {@link IRawStore#getUUID() UUID} of
     * the backing {@link IIndexManager} on which the property set for some
     * namespace was discovered.
     */
    private final String STORE_UUID = DefaultResourceLocator.class.getName()+".storeUUID";
    
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

        /*
         * Configure the property sets cache.
         *
         * Note: This cache capacity is set to a multiple of the specififed
         * capacity. While the property sets in this cache can not cause indices
         * or relations to be retained, a cache hit significantly decreases the
         * cost of materializing a relation or index view. Therefore a larger
         * cache capacity can have a big payoff with relatively little heap
         * overhead. For the same reason, we also boost the cacheTimeout.
         */
        {

            final int propertyCacheCapacity = Math.max(cacheCapacity,
                    cacheCapacity * 10);

            final long propertyCacheTimeout = TimeUnit.MILLISECONDS
                    .toNanos(cacheTimeout) * 10;

            this.propertyCache = new ConcurrentWeakValueCacheWithTimeout<NT, Map<String, Object>>(
                    propertyCacheCapacity,
                    propertyCacheTimeout);

        }

    }

    // @todo hotspot 2% total query time.
    @Override
    public T locate(final String namespace, final long timestamp) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (INFO) {

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
         *
         * TODO Better javadoc. This is a mixed blessing. Obviously, any request
         * for the same timestamp or the same commit time should hit this lock
         * so we serialize such efforts. However, lacking the commitTime for a
         * request, this serializes all lookups against the same namespace. If
         * we have concurrent queries arriving against different commit times
         * then this lock could be too broad. Ideally, the lock would be on
         * (namespace,commitTime). Fix this when we address
         * http://sourceforge.net/apps/trac/bigdata/ticket/266 (Refactor native
         * long tx id to thin object).
         */
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            // test cache now that we have the lock.
            resource = resourceCache.get(nt);

            if (resource != null) {

                if (log.isDebugEnabled())
                    log.debug("cache hit: " + resource);

                return resource;

            } else {

                if (INFO)
                    log.info("cache miss: namespace=" + namespace
                            + ", timestamp=" + timestamp);

                resource = cacheMiss(nt);

            }

            if (resource == null) {

                if (INFO)
                    log.info("Not found: " + nt);

            } else {

                if (INFO)
                    log.info("Caching: " + nt + " as " + resource);

                // Add to the cache.
                resourceCache.put(nt, resource);

            }

            return resource;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Handle a cache miss.
     *
     * @param nt
     *            The (namespace, timestamp) tuple.
     *
     * @return The resolved resource -or- <code>null</code> if the resource
     *         could not be resolved for that namespace and timestamp.
     */
    private T cacheMiss(final NT nt) {

        T resource = null;

        /*
         * First, test this locator, including any [seeAlso] IIndexManager
         * objects.
         */
        final AtomicReference<IIndexManager> foundOn = new AtomicReference<IIndexManager>();

        final Properties properties = locateResource(nt.getName(),
                nt.getTimestamp(), foundOn);

        if (properties == null) {

            // Not found by this locator.

            if(delegate != null) {

                /*
                 * A delegate was specified, so see if the delegate can resolve
                 * this request.
                 */

                if (INFO) {

                    log.info("Not found - passing to delegate: " + nt);

                }

                // pass request to delegate.
                resource = delegate.locate(nt.getName(), nt.getTimestamp());

                if (resource != null) {

                    if (INFO) {

                        log.info("delegate answered: " + resource);

                    }

                    return resource;

                }

            }

            // not found.
            return null;

        }

        if (log.isDebugEnabled()) {

            log.debug(properties.toString());

        }

        // can throw a ClassCastException.
        final String className = properties.getProperty(RelationSchema.CLASS);

        if (className == null) {

            /*
             * Note: This can indicate a deleted resource (all properties have
             * been cleared from the global row store).
             */

//            throw new IllegalStateException(
//                    "Required property not found: namespace=" + nt.getName()
//                            + ", property=" + RelationSchema.CLASS);
            return null;

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
        resource = newInstance(cls, foundOn.get(), nt, properties);

        return resource;

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

        synchronized (seeAlso) { // FIXME Probably a read/write lock since [seeAlso] normally empty.

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

                        if (INFO)
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

                    if(indexManager instanceof IRawStore) {

                        final UUID storeUUID = (UUID) properties.get(STORE_UUID);

                        if (storeUUID != null && !storeUUID.equals(((IRawStore) indexManager).getUUID())) {
                            // Property set for namespace was found on a
                            // different store.
                            continue;
                        }

                    }
                    
                    if (INFO) {

                        log.info("Found: namespace=" + namespace + " on "
                                + indexManager + ", properties=" + properties);

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

            if (INFO) {

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

        if (INFO) {

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
        final boolean propertyCacheHit;
        if (TimestampUtility.isReadOnly(timestamp)
                && !TimestampUtility.isReadCommitted(timestamp)) {

            /*
             * A stable read-only view.
             *
             * Note: This code path is NOT for read-committed views since such
             * views are not stable.
             *
             * Note: We will cache the property set on this code path. This code
             * path is NOT used for read-committed or mutable views since the
             * cache would prevent updated information from becoming visible.
             */

            final long readTime;

            if (indexManager instanceof IJournal) {

                /*
                 * For a Journal, find the commitTime backing that read-only
                 * view.
                 */

                final IJournal journal = (IJournal) indexManager;

                /*
                 * Note: Using the local transaction manager to resolve a
                 * read-only transaction identifer to the ITx object and then
                 * retrieving the readsOnCommitTime from that ITx is a
                 * non-blocking code path. It is preferrable to looking up the
                 * commitRecord in the CommitRecordIndex, which can be
                 * contended. This non-blocking code path will handle any
                 * read-only tx, but it will not be able to locate a timestamp
                 * not protected by a tx.
                 */

                final ITx tx = journal.getLocalTransactionManager().getTx(
                        timestamp);

                if (tx != null) {

                // Fast path.
                commitTime2 = readTime = tx.getReadsOnCommitTime();

                } else {

                // find the commit record on which we need to read.
                final ICommitRecord commitRecord = journal
                        .getCommitRecord(TimestampUtility
                                .asHistoricalRead(timestamp));

                if (commitRecord == null) {

                    /*
                     * No data for that timestamp.
                     */

                    return null;

                }

                /*
                 * Find the timestamp associated with that commit record.
                 *
                 * Note: We also save commitTime2 to stuff into the properties,
                 * thereby revealing the commitTime backing the resource view
                 * whenever possible.
                 */

                commitTime2 = readTime = commitRecord.getTimestamp();

                }

            } else {

                /*
                 * Federation, TemporaryStore, etc.
                 *
                 * Note: The TemporaryStore lacks a history mechanism.
                 *
                 * Note: The federation will cache based on the caller's
                 * [timestamp]. This works well if a global read lock has been
                 * asserted and query is directed against that read-only tx. It
                 * works poorly if a new tx is obtained for each query. Use a
                 * shared read lock for better performance on the federation!
                 *
                 * TODO The IBigdataFederation does not expose the backing
                 * commit time, which is why using a shared read lock is
                 * important for good performance.
                 *
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/266
                 * (replace native long with thin tx interface)
                 */

                readTime = timestamp;

            }

            /*
             * The key for the property cache is based on the 'readTime'. When
             * possible, this will be the commitTime on which the [timestamp] is
             * reading. That provides some reuse of the cache entry across
             * different [timestamp]s for read-only views.
             *
             * TODO The TPS stores the evolution of the property set. We could
             * create a more sophisticated cache which can answer for any
             * historical view for which there is data in the TPS. Under this
             * model, the TPS would be cached under the [namespace] without
             * regard to the [timestamp]. We would then use TPS#asMap(timestamp)
             * to reconstruct the propertySet for the caller's timestamp. We
             * would only need to read-through to the GRS when the caller's
             * [timestamp] was more recent than the last update on the TPS for
             * the namespace. This approach might be an attractive alternative
             * in combination with some sort of invalidation mechanism for the
             * rate writes on the GRS for a relation namespace (relation data
             * writes vastly outpace GRS writes for the metadata declaring that
             * relation so a guaranteed invalidation mechanism might very well
             * pay off).
             *
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/266
             * (replace native long with thin tx interface)
             */
            final NT nt = new NT(namespace, readTime);

            // Check the cache before materializing the properties from GRS.
            final Map<String, Object> cachedMap = propertyCache.get(nt);

            if (cachedMap != null) {

                /*
                 * Property cache hit.
                 */

                // Save reference to the property set.
                map = cachedMap;
                propertyCacheHit = true;

            } else {

                /*
                 * Property cache miss.
                 */
                
                propertyCacheHit = false;

                // Use the GRS view as of that commit point.
                final SparseRowStore rowStore = indexManager
                        .getGlobalRowStore(readTime);

                // Read the properties from the GRS.
                try {
                    map = rowStore == null ? null : rowStore.read(
                            RelationSchema.INSTANCE, namespace);
                } catch (RuntimeException ex) {
                    // Provide more information in trace. 
                    // Provide more information in trace.
                    // See http://jira.blazegraph.com/browse/BLZG-1268
                    throw new RuntimeException(ex.getMessage() + "::namespace="
                            + namespace + ", timestamp="
                            + TimestampUtility.toString(timestamp)
                            + ", readTime="
                            + TimestampUtility.toString(readTime), ex);
                }

                if (map != null) {

                    // Stuff the properties into the cache.
                    if (indexManager instanceof IRawStore) {
                        map.put(STORE_UUID, ((IRawStore) indexManager).getUUID());
                    }
                    propertyCache.put(nt, map);

                }

            }

        } else {

            final SparseRowStore rowStore;

            /*
             * Look up the GRS for a non-read-historical view.
             *
             * Note: We do NOT cache the property set on this code path.
             *
             * Note: This used to use the UNISOLATED view for all such requests.
             * I have modified the code (9/7/2012) to use the caller's view.
             * This allows a caller using a READ_COMMITTED view to obtain a
             * read-only view of the GRS. That is required to support GRS reads
             * on followers in an HA quorum (followers are read-only and do not
             * have access to the unisolated versions of indices).
             */

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * The unisolated view.
                 */

                rowStore = indexManager.getGlobalRowStore();

            } else if (timestamp == ITx.READ_COMMITTED) {

                /*
                 * View for the last commit time.
                 */

                rowStore = indexManager.getGlobalRowStore(indexManager
                        .getLastCommitTime());

            } else if (TimestampUtility.isReadWriteTx(timestamp)) {

                if (indexManager instanceof IJournal) {

                    final IJournal journal = (IJournal) indexManager;

                    final ITx tx = journal.getLocalTransactionManager().getTx(
                            timestamp);

                    if (tx == null) {

                        // No such tx?
                        throw new IllegalStateException("No such tx: "
                                + timestamp);

                    }

                    // Use the view that the tx is reading on.
                    rowStore = indexManager.getGlobalRowStore(tx
                            .getReadsOnCommitTime());

                } else {

                    /*
                     * TODO This should use the readsOnCommitTime on the cluster
                     * as well.
                     *
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/266
                     * (thin txId)
                     */

                    rowStore = indexManager.getGlobalRowStore(/* unisolated */);

                }


            } else {

                throw new AssertionError("timestamp="
                        + TimestampUtility.toString(timestamp));

            }

            // Read the properties from the GRS.
            map = rowStore == null ? null : rowStore.read(
                    RelationSchema.INSTANCE, namespace);
            
            propertyCacheHit = false;

        }

        if (map == null) {

            if (log.isDebugEnabled()) {

                log.debug("Not found: indexManager=" + indexManager
                        + ", namespace=" + namespace + ", timestamp="
                        + timestamp);

            }

            return null;

        }

        // Note: Prevent cross view mutation.
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

            log.trace("Read properties: indexManager=" + indexManager + ", namespace=" + namespace + ", timestamp="
                    + timestamp + ", propertyCacheHit=" + propertyCacheHit + ", properties=" + properties);

        }

        return properties;

    }

    /**
     * Create a new view of the relation.
     *
     * @param indexManager
     *            The {@link IIndexManager} that will be used to resolve the
     *            named indices for the relation.
     * @param nt
     *            The namespace and timestamp for the view of the relation.
     * @param properties
     *            Configuration properties for the relation.
     *
     * @return A new instance of the identified resource.
     */
    protected T newInstance(final Class<? extends T> cls,
            final IIndexManager indexManager, final NT nt,
            final Properties properties) {

        if (cls == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (nt == null)
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
                    nt.getName(), //
                    nt.getTimestamp(), //
                    properties //
                    });

            r.init();

            if(INFO) {

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

        if (INFO) {

            log.info("namespace=" + namespace+", timestamp="+timestamp);

        }

        // lock is only for the named relation.
        final Lock lock = namedLock.acquireLock(namespace);

        try {

            final NT nt = new NT(namespace, timestamp);

            final T tmp = resourceCache.get(nt);

            if (tmp != null) {

                if(INFO) {

                    log.info("Existing instance already in cache: "+tmp);

                }

                return tmp;

            }

            resourceCache.put(nt, instance);

            if (INFO) {

                log.info("Instance added to cache: nt=" + nt + ", instance=" + instance + ", indexManager="
                        + (instance instanceof AbstractResource ? ((AbstractResource<?>) instance).getIndexManager()
                                : "n/a"));

            }

            return instance;

        } finally {

            lock.unlock();

        }

    }

    @Override
    public void discard(final ILocatableResource<T> instance,
            final boolean destroyed) {

        if (instance == null)
            throw new IllegalArgumentException();

        final String namespace = instance.getNamespace();

        final long timestamp = instance.getTimestamp();

        if (INFO) {

            log.info("namespace=" + namespace + ", timestamp=" + timestamp
                    + ", destroyed=" + destroyed);

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

            if (INFO) {

                log.info("instance=" + instance + ", found=" + found);

            }

            if (destroyed) {

                // Also discard the unisolated view.
                resourceCache.remove(new NT(namespace, ITx.UNISOLATED));

                // Also discard the read-committed view.
                resourceCache.remove(new NT(namespace, ITx.READ_COMMITTED));

            }

        } finally {

            lock.unlock();

        }

    }

    @Override
    public void clearUnisolatedCache() {

        // Discard any unisolated resource views.
//        resourceCache.clear();
//        propertyCache.clear();
        {
            final Iterator<Map.Entry<NT, WeakReference<T>>> itr = resourceCache.entryIterator();
            while (itr.hasNext()) {
                final Map.Entry<NT, WeakReference<T>> e = itr.next();
                final NT nt = e.getKey();
                if (TimestampUtility.isUnisolated(nt.getTimestamp())) {
                    itr.remove();
//                    resourceCache.remove(nt);
                }
            }
        }{
            final Iterator<Map.Entry<NT, WeakReference<Map<String,Object>>>> itr = propertyCache.entryIterator();
            while (itr.hasNext()) {
                final Map.Entry<NT, WeakReference<Map<String,Object>>> e = itr.next();
                final NT nt = e.getKey();
                if (TimestampUtility.isUnisolated(nt.getTimestamp())) {
                    itr.remove();
//                    propertyCache.remove(nt);
                }
            }
        }

        if (delegate != null) {
            // And do this on the delegate as well.
            delegate.clearUnisolatedCache();
        }

        if (log.isInfoEnabled())
            log.info("Cleared resource cache");

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
    public void add(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        synchronized (seeAlso) {

            /*
             * Note: weak reference keys, nothing stored under the key.
             */
            seeAlso.put(indexManager, null);

            if (INFO) {

                log.info("size=" + seeAlso.size() + ", added indexManager="
                        + indexManager);

            }

        }

    }

    private final WeakHashMap<IIndexManager, Void> seeAlso = new WeakHashMap<IIndexManager, Void>();

}
