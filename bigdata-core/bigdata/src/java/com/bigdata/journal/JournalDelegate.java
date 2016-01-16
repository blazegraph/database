/**

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

package com.bigdata.journal;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.sparse.SparseRowStore;

/**
 * {@link IJournal} delegation pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JournalDelegate implements IJournal {
	
	private final AbstractJournal delegate;
	
	public JournalDelegate(final AbstractJournal source) {
		this.delegate = source;
	}
	
	@Override
	public boolean isHAJournal() {
		return false;
	}

	@Override
	public Properties getProperties() {
		return delegate.getProperties();
	}

   @Override
	public void shutdown() {
		delegate.shutdown();	
	}

   @Override
	public void shutdownNow() {
		delegate.shutdownNow();		
	}

   @Override
	public void abort() {
		delegate.abort();
	}

   @Override
	public long commit() {
		return delegate.commit();
	}

   @Override
	public ICommitRecord getCommitRecord(long timestamp) {
		return delegate.getCommitRecord(timestamp);
	}

   @Override
	public long getRootAddr(int index) {
		return delegate.getRootAddr(index);
	}

   @Override
	public IRootBlockView getRootBlockView() {
		return delegate.getRootBlockView();
	}

//    public IRootBlockView getRootBlock(long commitTime) {
//        return delegate.getRootBlock(commitTime);
//    }
//
//	public Iterator<IRootBlockView> getRootBlocks(long startTime) {
//		return delegate.getRootBlocks(startTime);
//	}

   @Override
	public void setCommitter(int index, ICommitter committer) {
		delegate.setCommitter(index, committer);
	}

   @Override
	public void close() {
		delegate.close();
	}

   @Override
	public void delete(long addr) {
		delegate.delete(addr);
	}

   @Override
	public void deleteResources() {
		delegate.deleteResources();
	}

   @Override
	public void destroy() {
		delegate.destroy();
	}

   @Override
	public void force(boolean metadata) {
		delegate.force(metadata);
	}

   @Override
	public CounterSet getCounters() {
		return delegate.getCounters();
	}

   @Override
	public File getFile() {
		return delegate.getFile();
	}

   @Override
	public IResourceMetadata getResourceMetadata() {
		return delegate.getResourceMetadata();
	}

   @Override
	public UUID getUUID() {
		return delegate.getUUID();
	}

   @Override
	public boolean isFullyBuffered() {
		return delegate.isFullyBuffered();
	}

   @Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

   @Override
	public boolean isReadOnly() {
		return delegate.isOpen();
	}

   @Override
	public boolean isStable() {
		return delegate.isStable();
	}

   @Override
	public ByteBuffer read(long addr) {
		return delegate.read(addr);
	}

   @Override
	public long size() {
		return delegate.size();
	}

   @Override
	public long write(ByteBuffer data) {
		return delegate.write(data);
	}

//	@Override
//	public long write(ByteBuffer data, long oldAddr) {
//		return delegate.write(data, oldAddr);
//	}

   @Override
	public int getByteCount(long addr) {
		return delegate.getByteCount(addr);
	}

   @Override
	public long getOffset(long addr) {
		return delegate.getOffset(addr);
	}

   @Override
    public long getPhysicalAddress(final long addr) {
        return delegate.getPhysicalAddress(addr);
    }
    
    @Override
	public long toAddr(int nbytes, long offset) {
		return delegate.toAddr(nbytes, offset);
	}

   @Override
	public String toString(long addr) {
		return delegate.toString(addr);
	}

   @Override
	public IIndex getIndex(String name) {
		return delegate.getIndex(name);
	}

   @Override
	public IIndex registerIndex(String name, BTree btree) {
		return delegate.registerIndex(name, btree);
	}

   @Override
	public IIndex registerIndex(String name, IndexMetadata indexMetadata) {
		return delegate.registerIndex(name, indexMetadata);
	}

   @Override
	public void dropIndex(String name) {
		delegate.dropIndex(name);
	}

   @Override
	public void registerIndex(IndexMetadata indexMetadata) {
		delegate.registerIndex(indexMetadata);
	}

   @Override
	public ExecutorService getExecutorService() {
		return delegate.getExecutorService();
	}

   @Override
	public BigdataFileSystem getGlobalFileSystem() {
		return delegate.getGlobalFileSystem();
	}

   @Override
    public SparseRowStore getGlobalRowStore() {
        return delegate.getGlobalRowStore();
    }

    @Override
    public SparseRowStore getGlobalRowStore(final long timestamp) {
        return delegate.getGlobalRowStore(timestamp);
    }

    @Override
	public IIndex getIndex(String name, long timestamp) {
		return delegate.getIndex(name, timestamp);
	}

   @Override
	public long getLastCommitTime() {
		return delegate.getLastCommitTime();
	}

   @Override
	public IResourceLocator getResourceLocator() {
		return delegate.getResourceLocator();
	}

   @Override
    public ILocalTransactionManager getLocalTransactionManager() {
        return delegate.getLocalTransactionManager();
    }

    @Override
    public IResourceLockService getResourceLockService() {
		return delegate.getResourceLockService();
	}

    @Override
    public TemporaryStore getTempStore() {
		return delegate.getTempStore();
	}
	
   @Override
	public ScheduledFuture<?> addScheduledTask(Runnable task,
			long initialDelay, long delay, TimeUnit unit) {
		return delegate.addScheduledTask(task, initialDelay, delay, unit);
	}

   @Override
	public boolean getCollectPlatformStatistics() {
		return delegate.getCollectPlatformStatistics();
	}

   @Override
	public boolean getCollectQueueStatistics() {
		return delegate.getCollectQueueStatistics();
	}

   @Override
	public int getHttpdPort() {
		return delegate.getHttpdPort();
	}

    @Override
    public Iterator<String> indexNameScan(String prefix, long timestamp) {
        return delegate.indexNameScan(prefix, timestamp);
    }

	@Override
	public IPSOutputStream getOutputStream() {
		return delegate.getOutputStream();
	}

	@Override
	public InputStream getInputStream(long addr) {
		return delegate.getInputStream(addr);
	}

   @Override
   public boolean isDirty() {
      return delegate.isDirty();
   }

   @Override
   public boolean isGroupCommit() {
      return delegate.isGroupCommit();
   }

    @Override
    public ICheckpointProtocol register(String name, IndexMetadata metadata) {
        return delegate.register(name, metadata);
    }

    @Override
    public ICheckpointProtocol getIndexLocal(String name, long commitTime) {
        return delegate.getIndexLocal(name, commitTime);
    }

    @Override
    public ICheckpointProtocol getUnisolatedIndex(String name) {
        return delegate.getUnisolatedIndex(name);
    }

   @Override
   public Quorum<HAGlue, QuorumService<HAGlue>> getQuorum() {
      return delegate.getQuorum();
   }

   @Override
   final public long awaitHAReady(final long timeout, final TimeUnit units)
           throws InterruptedException, TimeoutException,
           AsynchronousQuorumCloseException {
      return delegate.awaitHAReady(timeout, units);
   }
   
}
