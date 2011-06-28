/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
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

        this.fed = fed;
        this.indexManager = indexManager;

    }

    public IBigdataFederation<?> getFederation() {
        return fed;
    }

    public IIndexManager getIndexManager() {
        return indexManager;
    }

	public void halt(Void v) {
        log.warn("Mock object does not implement halt(Void)");
	}

	public <T extends Throwable> T halt(T cause) {
        log.warn("Mock object does not implement halt(Throwable)");
        return cause;
	}

    public QueryEngine getQueryEngine() {
        throw new UnsupportedOperationException();
    }

	public Map<Integer, BOp> getBOpIndex() {
		return null;
	}

//	public boolean isLastInvocation(final int bopId,final int nconsumed) {
//		throw new UnsupportedOperationException();
//	}
	
	public Map<Integer, BOpStats> getStats() {
		return null;
	}

	public long getDeadline() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getDoneTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getElapsed() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getStartTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Throwable getCause() {
		// TODO Auto-generated method stub
		return null;
	}

	public BOp getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	public UUID getQueryId() {
		// TODO Auto-generated method stub
		return null;
	}

    public IAsynchronousIterator<IBindingSet[]> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        // TODO Auto-generated method stub
        return false;
    }

    public Void get() throws InterruptedException, ExecutionException {
        // TODO Auto-generated method stub
        return null;
    }

    public Void get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isCancelled() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isDone() {
        // TODO Auto-generated method stub
        return false;
    }

    public IQueryClient getQueryController() {
        throw new UnsupportedOperationException();
    }

}
