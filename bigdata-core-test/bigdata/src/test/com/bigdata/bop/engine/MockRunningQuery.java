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
/*
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.engine;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IQueryContext;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Mock object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockRunningQuery implements IRunningQuery {

    private static final Logger log = Logger.getLogger(MockRunningQuery.class);
    
    private final IBigdataFederation<?> fed;

    private final IIndexManager indexManager;

    private final IQueryContext queryContext;
    
    /**
     * Note: This constructor DOES NOT check its arguments so unit tests may be
     * written with the minimum dependencies
     * 
     * @param fed
     * @param indexManager
     * @param readTimestamp
     * @param writeTimestamp
     */
    public MockRunningQuery(final IBigdataFederation<?> fed,
            final IIndexManager indexManager) {
        this(fed, indexManager, null/* queryContext */);
    }

    public MockRunningQuery(final IBigdataFederation<?> fed,
            final IIndexManager indexManager, final IQueryContext queryContext) {

        this.fed = fed;
        this.indexManager = indexManager;
        this.queryContext = queryContext;

    }

    @Override
    public IBigdataFederation<?> getFederation() {
        return fed;
    }

    @Override
    public IIndexManager getLocalIndexManager() {
        return indexManager;
    }

    @Override
	public void halt(Void v) {
        log.warn("Mock object does not implement halt(Void)");
	}

    @Override
	public <T extends Throwable> T halt(T cause) {
        log.warn("Mock object does not implement halt(Throwable)");
        return cause;
	}

    @Override
    public QueryEngine getQueryEngine() {
        throw new UnsupportedOperationException();
    }

    @Override
	public Map<Integer, BOp> getBOpIndex() {
		return null;
	}

//	public boolean isLastInvocation(final int bopId,final int nconsumed) {
//		throw new UnsupportedOperationException();
//	}
	
    @Override
	public Map<Integer, BOpStats> getStats() {
		return null;
	}

    @Override
	public long getDeadline() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
	public long getDoneTime() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
	public long getElapsed() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
	public long getStartTime() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
    public Throwable getCause() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Throwable getAsThrownCause() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
	public BOp getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
	public UUID getQueryId() {
		return queryContext.getQueryId();
	}

    @Override
    public IAsynchronousIterator<IBindingSet[]> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isCancelled() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDone() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public IQueryClient getQueryController() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IMemoryManager getMemoryManager() {
        
        return queryContext.getMemoryManager();
        
    }

    @Override
    public IQueryAttributes getAttributes() {

        return queryContext.getAttributes();
        
    }

   @Override
   public void setStaticAnalysisStats(StaticAnalysisStats saStats) {
      // not supported by mock query
   }

   @Override
   public StaticAnalysisStats getStaticAnalysisStats() {
      // not supported by mock query
      return null;
   }

}
