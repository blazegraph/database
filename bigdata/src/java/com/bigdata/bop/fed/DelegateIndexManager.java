package com.bigdata.bop.fed;

import java.util.concurrent.ExecutorService;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.SparseRowStore;

/**
 * The index view that we need for the {@link DistributedJoinTask} is on the
 * {@link IndexManager} class, not the live {@link ManagedJournal}. Looking on
 * the live journal we will only see the mutable {@link BTree} and not the
 * entire index partition view. However, {@link IndexManager} does not implement
 * {@link IIndexManager} or even {@link IIndexStore}. Therefore this class was
 * introduced. It passes most of the methods on to the
 * {@link IBigdataFederation} but {@link #getIndex(String, long)} is delegated
 * to {@link IndexManager#getIndex(String, long)} which is the method that knows
 * how to create the index partition view.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: JoinTaskFactoryTask.java 3448 2010-08-18 20:55:58Z thompsonbry
 *          $
 * 
 * @todo While this class solves our problem I do not know whether or not this
 *       class should this class have more visibility? The downside is that it
 *       is a bit incoherent how it passes along one method to the
 *       {@link IndexManager}, most methods to the {@link IBigdataFederation}
 *       and disallows {@link #dropIndex(String)} and
 *       {@link #registerIndex(IndexMetadata)} in an attempt to stay out of
 *       trouble. That may be enough reason to keep it private.
 */
class DelegateIndexManager implements IIndexManager {

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
