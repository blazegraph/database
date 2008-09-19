package com.bigdata.service;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.cache.ICacheEntry;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.ITx;
import com.bigdata.util.NT;

/**
     * Abstract base class providing caching for {@link IIndex} like objects. A
     * canonicalizing cache is used with weak references to the {@link IIndex}s
     * back by a hard reference LRU cache. This tends to keep around views that
     * are reused while letting references for unused views be cleared by the
     * garbage collector in a timely manner.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    abstract public class AbstractIndexCache<T extends IRangeQuery> {
        
        /**
         * A canonicalizing cache for the client's {@link IIndex} proxy objects. The
         * keys are {@link NT} objects which represent both the name of the index
         * and the timestamp for the index view. The values are the {@link IIndex}
         * proxy objects.
         * <p>
         * Note: The "dirty" flag associated with the object in this cache is
         * ignored.
         */
        final private WeakValueCache<NT, T> indexCache;
        
        final private NamedLock<NT> indexCacheLock = new NamedLock<NT>();

        /**
         * 
         * @param capacity
         *            The capacity of the backing LRU hard reference cache.
         */
        protected AbstractIndexCache(int capacity) {
            
            indexCache = new WeakValueCache<NT, T>(
                    new LRUCache<NT, T>(capacity));
            
        }
        
        /**
         * Method is invoked on a cache miss and returns a view of the described
         * index.
         * 
         * @param name
         * @param timestamp
         * 
         * @return The index view -or- <code>null</code> if the described
         *         index does not exist.
         */
        abstract protected T newView(final String name, final long timestamp);
        
        /**
         * Request a view of an index. If there is a cache miss then a new view
         * will be created.
         * 
         * @param nt
         * 
         * @return The index view -or- <code>null</code> if the described
         *         index does not exist.
         */
        public T getIndex(final String name, final long timestamp) {
            
            if (AbstractFederation.INFO)
                AbstractFederation.log.info("name="+name+" @ "+timestamp);
            
            final NT nt = new NT(name, timestamp);

            /*
             * Acquire a lock for the index name and timestamp. This allows
             * concurrent resolution of views of the same index and views of other
             * indices as well.
             */
            final Lock lock = indexCacheLock.acquireLock(nt);

            try {

                T ndx = indexCache.get(nt);

                if (ndx == null) {

                    if ((ndx = newView(name, timestamp)) == null) {

                        if (AbstractFederation.INFO)
                            AbstractFederation.log.info("name=" + name + " @ " + timestamp
                                    + " : no such index.");
                        
                        return null;
                        
                    }

                    // add to the cache.
                    indexCache.put(nt, ndx, false/* dirty */);

                    if (AbstractFederation.INFO)
                        AbstractFederation.log.info("name=" + name + " @ " + timestamp
                                + " : index exists.");

                } else {

                    if (AbstractFederation.INFO)
                        AbstractFederation.log.info("name=" + name + " @ " + timestamp
                                + " : cache hit.");

                }

                return ndx;
            
            } finally {
                
                lock.unlock();
                
            }

        }

        /**
         * Drop the {@link ITx#UNISOLATED} and {@link ITx#READ_COMMITTED}
         * entries for the named index from the cache.
         * <p>
         * Historical and transactional reads are still allowed, but we remove
         * the read-committed or unisolated views from the cache once the index
         * has been dropped. If a client wants them, it needs to re-request. If
         * they have been re-registered on the metadata service then they will
         * become available again.
         * <p>
         * Note: Operations against unisolated or read-committed indices will
         * throw exceptions if they execute after the index was dropped.
         */
        protected void dropIndexFromCache(String name) {
            
            synchronized(indexCache) {
                
                final Iterator<ICacheEntry<NT,T>> itr = indexCache.entryIterator();
                
                while(itr.hasNext()) {
                    
                    final ICacheEntry<NT,T> entry = itr.next();
                    
//                    final T ndx = entry.getObject();
                    final NT nt = entry.getKey();
                    
                    if(name.equals(nt.getName())) {
                        
                        final long timestamp = nt.getTimestamp();

                        if (timestamp == ITx.UNISOLATED
                                || timestamp == ITx.READ_COMMITTED) {
                        
                            if (AbstractFederation.INFO)
                                AbstractFederation.log.info("dropped from cache: " + name + " @ "
                                        + timestamp);
                            
                            // remove from the cache.
                            indexCache.remove(entry.getKey());

                        }
                        
                    }
                    
                }
                
            }
            
        }

        protected void shutdown() {
            
            indexCache.clear();
            
        }
        
    }