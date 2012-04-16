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

package com.bigdata.rdf.sparql.ast.cache;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.view.FusedView;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemStrategy;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.service.IDataService;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

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
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/524> SPARQL
 *      Query Cache </a>
 * 
 *      TODO Limit on {@link MemoryManager} via {@link ConfigParams}. Flush
 *      older objects from cache if the {@link MemoryManager} limit would be
 *      exceeded.
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
 *      TODO Benchmark impact of cache on BSBM explore+update. The cache should
 *      be integrated into the query planner so we can cache solution sets for
 *      sub-groups and sub-selects. However, when BINDINGS are present, then the
 *      query solutions are not the same as when they are not present. This
 *      makes the cache somewhat more difficult to integration since the same
 *      query is not always the same (e.g., include the hash of the exogenous
 *      solutions in the query hash code and we will get less reuse).
 *      
 *      TODO If the {@link IEvaluationContext} is not required, then back it
 *      out of the method signatures.
 */
public class SparqlCache implements ISparqlCache {

    private static final Logger log = Logger.getLogger(SparqlCache.class);
    
//    public interface Options {
//
//        /**
//         * The maximum amount of native memory which will be used to cache
//         * solution sets (default is 1/2 of the value reported by
//         * {@link Runtime#maxMemory()}).
//         * <p>
//         * Note: The {@link MemoryManager} backing the cache can use up to 4TB
//         * of RAM.
//         * <p>
//         * Note: Once the cache is full, solution sets will be expired according
//         * to the cache policy until the native memory demand has fallen below
//         * this threshold before a new solution set is added to the cache.
//         */
//        String MAX_MEMORY = SparqlCache.class.getName() + ".maxMemory";
//
//        final long DEFAULT_MAX_MEMORY = Runtime.getRuntime().maxMemory() / 2;
//
//    }
    
    private final QueryEngine queryEngine;
    
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
    private final CacheJournal cache;
    
    /**
     * TODO There are several problems here.
     * <p>
     * 1. We need a common semantics for visibility for the named solution sets
     * and the query and update operations. This cache can not provide that
     * without being somehow integrated with the MVCC architecture.
     * <p>
     * 2. We need to expire (at least some) cache objects. That expiration
     * should have a default but should also be configurable for each cache
     * object. The visibility issue also exists for expiration (we can not
     * expire a result set while it is being used).
     * <p>
     * 3. If we allow updates against named solution sets, then the visibility
     * of those updates must again be consistent with the MVCC architecture for
     * the query and update operations.
     * <p>
     * 4. We need to have metadata about solution sets on hand for explicit
     * CREATEs (e.g., supporting declared join variables).
     */
    private final ConcurrentHashMap<String/*name*/,SolutionSetMetadata> cacheMap;
//    private final ConcurrentWeakValueCacheWithTimeout<String/* name */, IMemoryManager /* allocationContext */> cacheMap;
    
//    /**
//     * The performance counters for the {@link IBufferStrategy} backing the
//     * {@link #cache}.
//     */
//    private final StoreCounters storeCounters = new StoreCounters();
//
//    /**
//     * The performance counters for the {@link IBufferStrategy} backing the
//     * {@link #cache}.
//     * 
//     * TODO Report out to whom? Against query manager?
//     */
//    public final StoreCounters getStoreCounters() {
//        
//        return storeCounters;
//        
//    }
    
//    /**
//     * Mapping from names to solution sets. This is MVCC aware.
//     * 
//     * TODO When we support durable solution sets, this will have to be
//     * modified. We will need to check the {@link Journal} and then the cache
//     * (or the other way around) to ensure that the global namespace is unique.
//     * (Or use a distinguished prefix or suffix for the solution set name to
//     * indicate durable versus transient).
//     */
//    private Name2Addr _name2Addr;
    
    private IIndexManager getLocalIndexManager() {
        
        return queryEngine.getIndexManager();
        
    }
    
    private ConcurrencyManager getConcurrencyManager() {

        /*
         * Note: I have commented this out on the QueryEngine and
         * FederatedQueryEngine until after the 1.2.0 release.
         */
//        return queryEngine.getConcurrencyManager();
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * 
     * Note: A distributed cache fabric could be accessed from any node in a
     * cluster. That means that this could be the {@link Journal} -or- the
     * {@link IndexManager} inside the {@link IDataService} and provides direct
     * access to {@link FusedView}s (aka shards).
     * 
     * @param queryEngine
     *            The {@link QueryEngine}.
     */
    public SparqlCache(final QueryEngine queryEngine) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        this.queryEngine = queryEngine;
        
        /*
         * TODO Setup properties from Journal or Federation (mainly the maximum
         * amount of RAM to use, but we can not limit that if we are using this
         * for to store named solution sets rather than as a cache).
         * 
         * TODO Setup an expire thread or a priority heap for expiring named
         * solution sets from the cache.
         */
        final Properties properties = new Properties();

        /*
         * Note: The cache will be backed by ByteBuffer objects allocated on the
         * native process heap (Zero GC).
         */
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.MemStore.name());

        properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""
                + (10 * Bytes.megabyte));

        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE,
                "true");

//        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
//                "false");
//
//        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
//                "false");
//
//        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        this.cache = new CacheJournal(properties);
        
//        /*
//         * TODO The expire should be per cached object, not global. We would
//         * need a different cache map class for that.
//         */
//        final long timeoutNanos = TimeUnit.SECONDS.toNanos(20);

//        this.cacheMap = new ConcurrentWeakValueCacheWithTimeout<String, IMemoryManager>(
//                0/* queueCapacity */, timeoutNanos);
        this.cacheMap = new ConcurrentHashMap<String, SolutionSetMetadata>();

    }
    
    @Override
    public void init() {
        
    }
    
    /**
     * {@link SparqlCache} is used with a singleton pattern managed by the
     * {@link SparqlCacheFactory}. It will be torn down automatically it is no
     * longer reachable. This behavior depends on not having any hard references
     * back to the {@link QueryEngine}.
     */
    @Override
    protected void finalize() throws Throwable {
        
        close();
        
        super.finalize();
        
    }

    @Override
    public void close() {

        cacheMap.clear();
        
        cache.destroy();

    }

    /**
     * Return the {@link IMemoryManager} backing all transient named solution
     * sets. The caller is responsible for creating a child allocation context
     * when writing a named solution set onto the {@link IMemoryManager}.
     * 
     * @return The shared {@link IMemoryManager}.
     */
    protected IMemoryManager getMemoryManager() {
        
        return ((MemStrategy) cache
                .getBufferStrategy()).getMemoryManager();
        
    }
    
    @Override
    public void clearAllSolutions(final IEvaluationContext ctx) {

        final Iterator<Map.Entry<String, SolutionSetMetadata>> itr = cacheMap
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, SolutionSetMetadata> e = itr.next();

            final String solutionSet = e.getKey();
            
            final SolutionSetMetadata sset = e.getValue();

            if (log.isInfoEnabled())
                log.info("solutionSet: " + solutionSet);

            sset.clear();
            
            itr.remove();
            
        }

    }

    @Override
    public boolean clearSolutions(final IEvaluationContext ctx, final String solutionSet) {

        if (log.isInfoEnabled())
            log.info("solutionSet: " + solutionSet);

        final SolutionSetMetadata sset = cacheMap.remove(solutionSet);

        if (sset != null) {
            sset.clear();

            return true;

        }

        return false;
        
    }

    public void putSolutions(final IEvaluationContext ctx, final String solutionSet,
            final ICloseableIterator<IBindingSet[]> src) {

        if (solutionSet == null)
            throw new IllegalArgumentException();
        
        if (src == null)
            throw new IllegalArgumentException();

        /*
         * TODO Deal with visibility issues on update (when the modified
         * solution set state becomes visible).
         */

        SolutionSetMetadata sset = cacheMap.get(solutionSet);

        if (sset == null) {

            final IMemoryManager mmrgr = getMemoryManager()
                    .createAllocationContext();

            sset = new SolutionSetMetadata(solutionSet, mmrgr,
                    getDefaultMetadata());

            cacheMap.put(solutionSet, sset);
            
        }

        // write the solutions onto the memory manager.
        sset.put(src);

    }

    public void createSolutions(final IEvaluationContext ctx,
            final String solutionSet, final ISPO[] params) {

        if (solutionSet == null)
            throw new IllegalArgumentException();
        
        /*
         * TODO Deal with visibility issues on update (when the modified
         * solution set state becomes visible and race conditions on create).
         */

        SolutionSetMetadata sset = cacheMap.get(solutionSet);

        if (sset != null)
            throw new RuntimeException("Exists: " + solutionSet);
        
        final IMemoryManager mmrgr = getMemoryManager()
                .createAllocationContext();

        sset = new SolutionSetMetadata(solutionSet, mmrgr,
                params == null ? getDefaultMetadata() : params);

        cacheMap.put(solutionSet, sset);

        {

            final List<IBindingSet[]> emptySolutionSet = new LinkedList<IBindingSet[]>();
            
            final ICloseableIterator<IBindingSet[]> src = new CloseableIteratorWrapper<IBindingSet[]>(
                    emptySolutionSet.iterator());

            // write the solutions onto the memory manager.
            sset.put(src);
        }

    }

	public ISolutionSetStats getSolutionSetStats(final String solutionSet) {

        if (solutionSet == null)
            throw new IllegalArgumentException();

        final SolutionSetMetadata sset = cacheMap.get(solutionSet);
        
        if(sset != null) {
        	
        		return sset.getStats();
        	
        }
        
        return null;
        
    }
    
    public ICloseableIterator<IBindingSet[]> getSolutions(final IEvaluationContext ctx,
            final String solutionSet) {

        if (solutionSet == null)
            throw new IllegalArgumentException();

        final SolutionSetMetadata sset = cacheMap.get(solutionSet);

        if (sset == null)
            throw new IllegalStateException("Not found: " + solutionSet);

        // Return iterator over the decoded solutions.
        return sset.get();

    }

    public boolean existsSolutions(final IEvaluationContext ctx,
            final String solutionSet) {

        if (solutionSet == null)
            throw new IllegalArgumentException();

        final SolutionSetMetadata sset = cacheMap.get(solutionSet);

        return sset != null;

    }

    /**
     * Return the default metadata used when a named solution set is declared
     * implicitly rather than explicitly.
     * 
     * @return The metadata describing that solution set.
     */
    protected ISPO[] getDefaultMetadata() {
        
        return new ISPO[]{};
        
    }
    
//    @Override
//    public ICacheHit get(final AST2BOpContext ctx,
//            final QueryBase queryOrSubquery) {
//
//        /*
//         * FIXME Implement. Start with a simple DESCRIBE <uri> cache.
//         */
//
//        return null;
//        
//    }
//    
//    /*
//     * TODO When caching a solution set, make sure that we do so before we apply
//     * the solution modifiers (ORDER BY, GROUP BY/HAVING, OFFSET/LIMIT) and
//     * perhaps before we evaluate DISTINCT.
//     * 
//     * TODO It will be especially easy if the solution set has been pushed into
//     * a hash index, which we can then just pour into the cache.
//     * 
//     * TODO We need a hash code to get started with query matching. However, it
//     * can not be the hash code of the query string or we will not be able to
//     * parameterize the solution modifiers. This suggests computing a hash 
//     * function over the AST which knows to leave off the solution modifiers.
//     */
//    @Override
//    public ICacheHit put(final AST2BOpContext ctx,
//            final QueryBase queryOrSubquery,
//            final ICloseableIterator<IBindingSet> src) {
//        // TODO Auto-generated method stub
//        return null;
//    }

//    /**
//     * Cache invalidation / cache update protocol.
//     * 
//     * FIXME This listener needs to build up a "transaction" of change events
//     * which will cause invalidation of cache entries. Those change events need
//     * to be processed once we have committed the change set, but they MUST be
//     * processed before another operation can read against the new commit point.
//     * <P>
//     * In order to close that gap, we can either invalidate the cache as we go
//     * (this will cause the query engine to regenerate the cache before the
//     * cached results are actually wrong) -or- we need to have a low level
//     * callback from the {@link Journal} where we get notice of the
//     * {@link IChangeLog} commit (including the timestamp) before the
//     * {@link Journal} actually finishes the commit process, but probably after
//     * it check points the various indices. E.g., hooked right into the middle
//     * of the commit protocol. For a tightly integrated cache, this can be fast
//     * enough.
//     * <p>
//     * Another possibility is to permit reads against the cache for any cached
//     * solution for which we have not yet received an invalidation notice. Once
//     * we get an invalidation notice, the cache will only respond for that
//     * cached solution set up to the lastCommitTime before the
//     * {@link IChangeRecord} event (so the cache needs to listen to commit
//     * times, which is easy). This will allow read-only operations against
//     * historical commit points to proceed but will not allow reads against the
//     * cache for cached solution sets which MIGHT be invalidated.
//     * <p>
//     * Each cache entry needs to have the createTime (commit time against which
//     * it was created, which needs to be the actually commit point if the reader
//     * is isolated by a transaction). That is the first commit point for which
//     * the cache entry is valid. It also needs to know the last commit time for
//     * which the cache entry is valid, which is the point at which it was
//     * expired. When a cache invalidation notice ({@link IChangeRecord}) has
//     * been received, but we have not yet observed the commit for that change
//     * record, we need to flag the cache entry as possibly invalid after the
//     * then most current last commit time. Queries which hit that window must be
//     * passed through to the database.
//     * <p>
//     * We need one {@link IChangeLog} listener per update connection. There can
//     * be multiple such listeners concurrently when the database is using full
//     * read/write transactions and/or when there are updates against different
//     * triple/quad store instances.
//     * 
//     * TODO Each Change log event indicates an {@link ISPO} which was added to
//     * (or removed from) the database. Cache entries which depend on statement
//     * patterns which cover those {@link ISPO}s must be invalidated (or updated)
//     * when the database update is committed. Obviously, the cache entries need
//     * to be indexed for rapid discovery for invalidation purposes (in addition
//     * to the discovery for cache hits).
//     * 
//     * TODO Cache entries need to be chained together so we can have hits for
//     * the same query for different commit points. The backing solution set for
//     * a given commit time needs to be expired no later than when we recycle
//     * that commit point. This is yet another place where a low-level
//     * integration with the {@link AbstractTransactionService} is required.
//     */
//    private class CacheChangeLogListener implements IChangeLog {
//
//        @Override
//        public void changeEvent(IChangeRecord record) {
//            // TODO Auto-generated method stub
//
//        }
//
//        @Override
//        public void transactionCommited(final long commitTime) {
//            // TODO Auto-generated method stub
//
//        }
//
//        @Override
//        public void transactionAborted() {
//            // TODO Auto-generated method stub
//
//        }
//
//    }

    /**
     * The {@link CacheJournal} provides the backing store for transient named
     * solution sets.
     */
    private class CacheJournal extends AbstractJournal {

        protected CacheJournal(final Properties properties) {

            super(properties);

//            /*
//             * TODO Report out counters for the cache.
//             */
//            if (getBufferStrategy() instanceof DiskOnlyStrategy) {
//
//                ((DiskOnlyStrategy) getBufferStrategy())
//                        .setStoreCounters(getStoreCounters());
//
//            } else if (getBufferStrategy() instanceof WORMStrategy) {
//
//                ((WORMStrategy) getBufferStrategy())
//                        .setStoreCounters(getStoreCounters());
//
//            }
 
        }

        public String toString() {
            
            /*
             * Note: Should not depend on any state that might be unreachable,
             * e.g., because the store is not open, etc.
             */
            
            final IRootBlockView rootBlock = getRootBlockView();
            
            return getClass().getName()
                    + "{file="
                    + getFile()
                    + ", open="
                    + CacheJournal.this.isOpen()
                    + (rootBlock != null ? ", uuid="
                            + getRootBlockView().getUUID() : "") + "}";
            
        }
        
//        /**
//         * Note: Exposed for the {@link DataService} which needs this for its
//         * 2-phase commit protocol.
//         */
//        public long commitNow(final long commitTime) {
//            
//            return super.commitNow(commitTime);
//            
//        }
        
//        /**
//         * Exposed for {@link StoreManger#getResourcesForTimestamp(long)} which
//         * requires access to the {@link CommitRecordIndex} for the
//         * lastCommitTime on the historical journals.
//         * <p>
//         * Note: This always returns a distinct index object. The code relies on
//         * this fact to avoid contention with the live {@link CommitRecordIndex}
//         * for the live journal.
//         */
//        public CommitRecordIndex getCommitRecordIndex(final long addr) {
//            
//            return super.getCommitRecordIndex(addr);
//            
//        }

        public AbstractLocalTransactionManager getLocalTransactionManager() {

            return (AbstractLocalTransactionManager) getConcurrencyManager()
                    .getTransactionManager();

        }

        public SparseRowStore getGlobalRowStore() {
            
            return getLocalIndexManager().getGlobalRowStore();
            
        }

        public SparseRowStore getGlobalRowStore(final long timestamp) {
            
            return getLocalIndexManager().getGlobalRowStore(timestamp);
            
        }

        public BigdataFileSystem getGlobalFileSystem() {
            
            return getLocalIndexManager().getGlobalFileSystem();
            
        }
        
        public DefaultResourceLocator getResourceLocator() {
            
            return (DefaultResourceLocator) getLocalIndexManager()
                    .getResourceLocator();
            
        }
        
        public ExecutorService getExecutorService() {
            
            return getLocalIndexManager().getExecutorService();
            
        }
        
        public IResourceLockService getResourceLockService() {

            return getLocalIndexManager().getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {
            
            return getLocalIndexManager().getTempStore();
            
        }

        public ScheduledFuture<?> addScheduledTask(Runnable task,
                long initialDelay, long delay, TimeUnit unit) {

            return getLocalIndexManager().addScheduledTask(task, initialDelay,
                    delay, unit);
        
        }

        public boolean getCollectPlatformStatistics() {
            return getLocalIndexManager().getCollectPlatformStatistics();
        }

        public boolean getCollectQueueStatistics() {
            return getLocalIndexManager().getCollectQueueStatistics();
        }

        public int getHttpdPort() {
            return getLocalIndexManager().getHttpdPort();
        }

    } // class CacheJournal

}
