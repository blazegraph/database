package com.bigdata.relation.rule.eval.pipeline;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.DataServiceCallable;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.Session;
import com.bigdata.service.proxy.ClientAsynchronousIterator;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.IKeyOrder;

/**
 * A factory for {@link DistributedJoinTask}s. The factory either creates a new
 * {@link DistributedJoinTask} or returns the pre-existing
 * {@link DistributedJoinTask} for the given {@link JoinMasterTask} instance (as
 * identified by its {@link UUID}), <i>orderIndex</i>, and <i>partitionId</i>.
 * When the desired join task pre-exists, factory will invoke
 * {@link DistributedJoinTask#addSource(IAsynchronousIterator)} and specify the
 * {@link #sourceItrProxy} as another source for that join task.
 * <p>
 * The use of a factory pattern allows us to concentrate all
 * {@link DistributedJoinTask}s which target the same tail predicate and index
 * partition for the same rule execution instance onto the same
 * {@link DistributedJoinTask}. The concentrator effect achieved by the factory
 * only matters when the fan-out is GT ONE (1).
 * 
 * @todo The factory semantics requires something like a "session" concept on
 *       the {@link DataService}. However, it could also be realized by a
 *       canonicalizing mapping of {masterProxy, orderIndex, partitionId} onto
 *       an object that is placed within a weak value cache.
 * 
 * @todo Whenever a {@link DistributedJoinTask} is interrupted or errors it must
 *       make sure that the entry is removed from the session (it could also
 *       interrupt/cancel the remaining {@link DistributedJoinTask}s for the
 *       same {masterInstance}, but we are already doing that in a different
 *       way.)
 * 
 * @todo We need to specify the failover behavior when running query or mutation
 *       rules. The simplest answer is that the query or closure operation fails
 *       and can be retried.
 *       <P>
 *       When retried a different data service instance could take over for the
 *       failed instance. This presumes some concept of "affinity" for a data
 *       service instance when locating a join task. If there are replicated
 *       instances of a data service, then affinity would be the tendency to
 *       choose the same instance for all join tasks with the same master,
 *       orderIndex, and partitionId. That might be more efficient since it
 *       allows aggregation of binding sets that require the same access path
 *       read. However, it might be more efficient to distribute the reads
 *       across the failover instances - it really depends on the workload.
 *       <p>
 *       Ideally, a data service failure would be handled by restarting only
 *       those parts of the operation that had failed. This means that there is
 *       some notion of idempotent for the operation. For at least the RDF
 *       database, this may be achievable. Failure during query leading to
 *       resend of some binding set chunks to a new join task could result in
 *       overgeneration of results, but those results would all be duplicates.
 *       If that is acceptable, then this approach could be considered "safe".
 *       Failure during mutation (aka closure) is even easier for RDF as
 *       redundant writes on an index still lead to the same fixed point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinTaskFactoryTask extends DataServiceCallable<Future> {

    /**
     * 
     */
    private static final long serialVersionUID = -2637166803787195001L;
    
    protected static final transient Logger log = Logger.getLogger(JoinTaskFactoryTask.class);

    /**
     * @deprecated This is only used by a logging statement.
     */
    final String scaleOutIndexName;
    
    final IRule rule;

    final IJoinNexusFactory joinNexusFactory;

    final int[] order;

    final int orderIndex;

    final int partitionId;

    final UUID masterUUID;
    
    final IJoinMaster masterProxy;

    final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
    
    final IKeyOrder[] keyOrders;
    
    /**
     * A list of variables required for each tail, by tailIndex. Used to filter 
     * downstream variable binding sets.  
     */
    final IVariable[][] requiredVars;

//    /**
//     * Set by the {@link DataService} which recognized that this class
//     * implements the {@link IDataServiceCallable}.
//     */
//    private transient DataService dataService;
//    
//    public void setDataService(DataService dataService) {
//        
//        this.dataService = dataService;
//        
//    }

    /**
     * Set by {@link #call()} to the federation instance available on the
     * {@link DataService}.
     */
    private transient AbstractScaleOutFederation fed;
    
    public String toString() {

        return getClass().getSimpleName() + "{ orderIndex=" + orderIndex
                + ", partitionId=" + partitionId + "}";
        
    }
    
    /**
     * 
     * @param scaleOutIndexName
     * @param rule
     * @param joinNexusFactory
     * @param order
     * @param orderIndex
     * @param partitionId
     * @param masterProxy
     * @param masterUUID
     *            (Avoids RMI to obtain this later).
     * @param sourceItrProxy
     * @param nextScaleOutIndexName
     */
    public JoinTaskFactoryTask(final String scaleOutIndexName,
            final IRule rule, final IJoinNexusFactory joinNexusFactory,
            final int[] order, final int orderIndex, final int partitionId,
            final IJoinMaster masterProxy,
            final UUID masterUUID,
            final IAsynchronousIterator<IBindingSet[]> sourceItrProxy,
            final IKeyOrder[] keyOrders,
            final IVariable[][] requiredVars) {
        
        if (scaleOutIndexName == null)
            throw new IllegalArgumentException();
        if (rule == null)
            throw new IllegalArgumentException();
        final int tailCount = rule.getTailCount();
        if (joinNexusFactory == null)
            throw new IllegalArgumentException();
        if (order == null)
            throw new IllegalArgumentException();
        if (order.length != tailCount)
            throw new IllegalArgumentException();
        if (orderIndex < 0 || orderIndex >= tailCount)
            throw new IllegalArgumentException();
        if (partitionId < 0)
            throw new IllegalArgumentException();
        if (masterProxy == null)
            throw new IllegalArgumentException();
        if (masterUUID == null)
            throw new IllegalArgumentException();
        if (sourceItrProxy == null)
            throw new IllegalArgumentException();
        if (keyOrders == null || keyOrders.length != order.length)
            throw new IllegalArgumentException();
        if (requiredVars == null)
            throw new IllegalArgumentException();

        this.scaleOutIndexName = scaleOutIndexName;
        this.rule = rule;
        this.joinNexusFactory = joinNexusFactory;
        this.order = order;
        this.orderIndex = orderIndex;
        this.partitionId = partitionId;
        this.masterProxy = masterProxy;
        this.masterUUID = masterUUID;
        this.sourceItrProxy = sourceItrProxy;
        this.keyOrders = keyOrders;
        this.requiredVars = requiredVars;
        
    }

    /**
     * Either starts a new {@link DistributedJoinTask} and returns its
     * {@link Future} or returns the {@link Future} of an existing
     * {@link DistributedJoinTask} for the same
     * {@link DistributedJoinMasterTask} instance, <i>orderIndex</i>, and
     * <i>partitionId</i>.
     * 
     * @return (A proxy for) the {@link Future} of the
     *         {@link DistributedJoinTask}.
     */
    public Future call() throws Exception {
        
//        if (dataService == null)
//            throw new IllegalStateException();

        this.fed = (AbstractScaleOutFederation) getFederation();

        /*
         * Start the iterator using our local thread pool in order to avoid
         * having it start() with a new Thread().
         * 
         * Note: This MUST be done before we create the join task or the
         * iterator will create its own Thread.
         */
        if (sourceItrProxy instanceof ClientAsynchronousIterator) {

            ((ClientAsynchronousIterator) sourceItrProxy).start(fed
                    .getExecutorService());

        }
        
        final String namespace = getJoinTaskNamespace(masterUUID, orderIndex,
                partitionId);
        
        final Future<Void> joinTaskFuture;

        final Session session = getDataService().getSession();
        
        /*
         * @todo this serializes all requests for a new join task on this data
         * service. However, we only need to serialize requests for the same
         * [uuid, orderIndex, partitionId]. A NamedLock on [namespace] would do
         * exactly that.
         * 
         * Note: The DistributedJoinTask will remove itself from the session
         * when it completes (regardless of success or failure). It does not
         * obtain a lock on the session but instead relies on addSource(itr) to
         * reject new sources until it can be removed from the session.
         */ 
        synchronized (session) {

            // lookup task for that key in the session.
            DistributedJoinTask joinTask = (DistributedJoinTask) session
                    .get(namespace);

            if (joinTask != null) {

                if (joinTask.addSource(sourceItrProxy)) {

                    // use the existing join task.
                    joinTaskFuture = joinTask.futureProxy;

                } else {

                    /*
                     * Create a new join task (the old one has decided that it
                     * will not accept any new sources).
                     */

                    // new task.
                    joinTask = newJoinTask();

                    // put into the session.
                    session.put(namespace, joinTask);

                    // submit task and note its future.
                    joinTaskFuture = submit(joinTask);

                }

            } else {

                /*
                 * There is no join task in the session so we create one now.
                 */
                
                // new task.
                joinTask = newJoinTask();

                // put into the session.
                session.put(namespace, joinTask);

                // submit task and note its future.
                joinTaskFuture = submit(joinTask);
                
            }
            
        }

        return joinTaskFuture;
        
    }

    protected DistributedJoinTask newJoinTask() {

        final DistributedJoinTask task;
        {

            /*
             * Note: This wrapper class passes getIndex(name,timestamp) to the
             * IndexManager for the DataService, which is the class that knows
             * how to assemble the index partition view.
             */
            final IIndexManager indexManager = new DelegateIndexManager(
                    getDataService());

            task = new DistributedJoinTask(/*scaleOutIndexName,*/ rule,
                    joinNexusFactory.newInstance(indexManager), order,
                    orderIndex, partitionId, fed, masterProxy, masterUUID,
                    sourceItrProxy, keyOrders, getDataService(), requiredVars);
            
        }

        return task;

    }
   
    protected Future<Void> submit(final DistributedJoinTask task) {

        if (log.isDebugEnabled())
            log.debug("Submitting new JoinTask: orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId + ", indexName="
                    + scaleOutIndexName);

        Future<Void> joinTaskFuture = getFederation()
                .getExecutorService().submit(task);

        if (fed.isDistributed()) {

            // create a proxy for the future.
            joinTaskFuture = ((AbstractDistributedFederation) fed)
                    .getProxy(joinTaskFuture);

        }

        task.futureProxy = joinTaskFuture;
        
        return joinTaskFuture;

    }
    
    /**
     * 
     * @param masterUUID
     *            The master UUID should be cached locally by the JoinTask so
     *            that invoking this method does not require RMI.
     * @param orderIndex
     * @param partitionId
     * @return
     */
    static public String getJoinTaskNamespace(final UUID masterUUID,
            final int orderIndex, final int partitionId) {

        return masterUUID + "/" + orderIndex + "/" + partitionId;

    }

    /**
     * The index view that we need for the {@link DistributedJoinTask} is on the
     * {@link IndexManager} class, not the live {@link ManagedJournal}. Looking
     * on the live journal we will only see the mutable {@link BTree} and not
     * the entire index partition view. However, {@link IndexManager} does not
     * implement {@link IIndexManager} or even {@link IIndexStore}. Therefore
     * this class was introduced. It passes most of the methods on to the
     * {@link IBigdataFederation} but {@link #getIndex(String, long)} is
     * delegated to {@link IndexManager#getIndex(String, long)} which is the
     * method that knows how to create the index partition view.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo While this class solves our problem I do not know whether or not
     *       this class should this class have more visibility? The downside is
     *       that it is a bit incoherent how it passes along one method to the
     *       {@link IndexManager}, most methods to the
     *       {@link IBigdataFederation} and disallows {@link #dropIndex(String)}
     *       and {@link #registerIndex(IndexMetadata)} in an attempt to stay out
     *       of trouble. That may be enough reason to keep it private.
     */
    static class DelegateIndexManager implements IIndexManager {
        
        private final DataService dataService;
        
        public DelegateIndexManager(final DataService dataService) {
            
            if (dataService == null)
                throw new IllegalArgumentException();
            
            this.dataService = dataService;
            
        }
        
        /**
         * Delegates to the {@link IndexManager}.
         */
        public IIndex getIndex(final String name, final long timestamp) {

            return dataService.getResourceManager().getIndex(name, timestamp);
            
        }

        /**
         * Not allowed.
         */
        public void dropIndex(final String name) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * Not allowed.
         */
        public void registerIndex(IndexMetadata indexMetadata) {

            throw new UnsupportedOperationException();
            
        }

        public void destroy() {
            
            throw new UnsupportedOperationException();
            
        }

        public ExecutorService getExecutorService() {
            
            return dataService.getFederation().getExecutorService();
            
        }

        public BigdataFileSystem getGlobalFileSystem() {

            return dataService.getFederation().getGlobalFileSystem();
            
        }

        public SparseRowStore getGlobalRowStore() {

            return dataService.getFederation().getGlobalRowStore();
            
        }

        public long getLastCommitTime() {

            return dataService.getFederation().getLastCommitTime();
            
        }

        public IResourceLocator getResourceLocator() {

            return dataService.getFederation().getResourceLocator();
            
        }

        public IResourceLockService getResourceLockService() {

            return dataService.getFederation().getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {

            return dataService.getFederation().getTempStore();

        }

    }

}
