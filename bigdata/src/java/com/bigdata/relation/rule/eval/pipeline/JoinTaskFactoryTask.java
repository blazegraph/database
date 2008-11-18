package com.bigdata.relation.rule.eval.pipeline;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.proxy.ClientAsynchronousIterator;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.IKeyOrder;

/**
 * A factory for {@link DistributedJoinTask}s. The factory either creates a
 * new {@link DistributedJoinTask} or returns the pre-existing
 * {@link DistributedJoinTask} for the given {@link JoinMasterTask}
 * instance, orderIndex, and partitionId. The use of a factory pattern
 * allows us to concentrate all {@link DistributedJoinTask}s which target
 * the same tail predicate and index partition for the same rule execution
 * instance onto the same {@link DistributedJoinTask}. The concentrator
 * effect achieved by the factory only matters when the fan-out is GT ONE
 * (1). When the fan-out from the source join dimension is GT ONE(1), then
 * factory achieves an idential fan-in for the sink.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME The factory semantics requires something like a "session" concept
 * on the {@link DataService}. Whenever a {@link DistributedJoinTask} is
 * interrupted or errors it must make sure that the entry is removed from
 * the session. This could also interupt/cancel the remaining
 * {@link DistributedJoinTask}s for the same {masterInstance}, but we are
 * already doing that in a different way.
 * <p>
 * This should not be a problem for a single index partition since fan-in ==
 * fan-out == 1, but it will be a problem for larger fan-in/outs.
 * <p>
 * When the desired join task pre-exists, factory will need to invoke
 * {@link DistributedJoinTask#addSource(IAsynchronousIterator)} and specify
 * the {@link #sourceItrProxy} as another source for that join task.
 */
public class JoinTaskFactoryTask implements Callable<Future>,
        IDataServiceAwareProcedure, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -2637166803787195001L;
    
    protected static final transient Logger log = Logger.getLogger(JoinTaskFactoryTask.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final transient boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final transient boolean DEBUG = log.isDebugEnabled();

    final String scaleOutIndexName;
    
    final IRule rule;

    final IJoinNexusFactory joinNexusFactory;

    final int[] order;

    final int orderIndex;

    final int partitionId;

    final IJoinMaster masterProxy;

    final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
    
    final IKeyOrder[] keyOrders;

    /**
     * Set by the {@link DataService} which recognized that this class
     * implements the {@link IDataServiceAwareProcedure}.
     */
    private transient DataService dataService;
    
    public void setDataService(DataService dataService) {
        
        this.dataService = dataService;
        
    }

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
     * @param sourceItrProxy
     * @param nextScaleOutIndexName
     */
    public JoinTaskFactoryTask(final String scaleOutIndexName,
            final IRule rule, final IJoinNexusFactory joinNexusFactory,
            final int[] order, final int orderIndex, final int partitionId,
            final IJoinMaster masterProxy,
            final IAsynchronousIterator<IBindingSet[]> sourceItrProxy,
            final IKeyOrder[] keyOrders) {
        
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
        if (sourceItrProxy == null)
            throw new IllegalArgumentException();
        if (keyOrders == null || keyOrders.length != order.length)
            throw new IllegalArgumentException();

        this.scaleOutIndexName = scaleOutIndexName;
        this.rule = rule;
        this.joinNexusFactory = joinNexusFactory;
        this.order = order;
        this.orderIndex = orderIndex;
        this.partitionId = partitionId;
        this.masterProxy = masterProxy;
        this.sourceItrProxy = sourceItrProxy;
        this.keyOrders = keyOrders;
        
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
        
        if (dataService == null)
            throw new IllegalStateException();

        final AbstractScaleOutFederation fed = (AbstractScaleOutFederation) dataService
                .getFederation();

        // FIXME new vs locating existing JoinTask in session.
        
        /*
         * Start the iterator using our local thread pool in order to avoid
         * having it start() with a new Thead().
         */
        if (sourceItrProxy instanceof ClientAsynchronousIterator) {

            ((ClientAsynchronousIterator) sourceItrProxy).start(fed
                    .getExecutorService());

        }
        
        /*
         * Note: This wrapper class passes getIndex(name,timestamp) to the
         * IndexManager for the DataService, which is the class that knows
         * how to assemble the index partition view.
         */
        final DistributedJoinTask task;
        {

            final IIndexManager indexManager = new DelegateIndexManager(
                    dataService);

            task = new DistributedJoinTask(scaleOutIndexName, rule,
                    joinNexusFactory.newInstance(indexManager), order,
                    orderIndex, partitionId, fed, masterProxy,
                    sourceItrProxy, keyOrders);
            
        }
        
        if (DEBUG)
            log.debug("Submitting new JoinTask: orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId + ", indexName="
                    + scaleOutIndexName);

        final Future<Void> joinTaskFuture = dataService.getFederation()
                .getExecutorService().submit(task);

        if (fed.isDistributed()) {
            
            // return a proxy for the future.
            return ((AbstractDistributedFederation) fed).getProxy(joinTaskFuture);

        }

        // just return the future.
        return joinTaskFuture;
        
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
        
        public DelegateIndexManager(DataService dataService) {
            
            if (dataService == null)
                throw new IllegalArgumentException();
            
            this.dataService = dataService;
            
        }
        
        /**
         * Delegates to the {@link IndexManager}.
         */
        public IIndex getIndex(String name, long timestamp) {

            return dataService.getResourceManager().getIndex(name, timestamp);
            
        }

        /**
         * Not allowed.
         */
        public void dropIndex(String name) {
            
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
