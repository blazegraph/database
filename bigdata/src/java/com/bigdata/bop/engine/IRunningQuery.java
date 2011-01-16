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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.IHaltable;

/**
 * Non-Remote interface exposing a limited set of the state of an executing
 * query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRunningQuery extends IHaltable<Void> {
    
	/**
	 * The query.
	 */
	BOp getQuery();

	/**
	 * The unique identifier for this query.
	 */
	UUID getQueryId();
	
    /**
     * The {@link IBigdataFederation} IFF the operator is being evaluated on an
     * {@link IBigdataFederation}. When evaluating operations against an
     * {@link IBigdataFederation}, this reference provides access to the
     * scale-out view of the indices and to other bigdata services.
     */
    IBigdataFederation<?> getFederation();

    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds shard
     * wise and this {@link IIndexManager} MUST be able to read on the
     * {@link ILocalBTreeView}.
     */
    IIndexManager getIndexManager();

    /**
     * The query engine class executing the query on this node.
     */
    QueryEngine getQueryEngine();

    /**
     * The client coordinate the evaluation of this query (aka the query
     * controller). For a standalone database, this will be the
     * {@link QueryEngine}. For scale-out, this will be the RMI proxy for the
     * {@link QueryEngine} instance to which the query was submitted for
     * evaluation by the application.
     */
    IQueryClient getQueryController();
    
	/**
	 * Return an unmodifiable index from {@link BOp.Annotations#BOP_ID} to
	 * {@link BOp}. This index may contain operators which are not part of the
	 * pipeline evaluation, such as {@link IPredicate}s.
	 */
    Map<Integer/*bopId*/,BOp> getBOpIndex();

	/**
	 * Return an unmodifiable map exposing the statistics for the operators in
	 * the query and <code>null</code> unless this is the query controller.
	 * There will be a single entry in the map for each distinct
	 * {@link PipelineOp}. Entries might not appear until that operator has
	 * either begun or completed at least one evaluation phase. This index only
	 * contains operators which are actually part of the pipeline evaluation.
	 */
    Map<Integer/* bopId */, BOpStats> getStats();
    
    /**
     * Return the query deadline (the time at which it will terminate regardless
     * of its run state).
     * 
     * @return The query deadline (milliseconds since the epoch) and
     *         {@link Long#MAX_VALUE} if no explicit deadline was specified.
     */
    public long getDeadline();

    /**
     * The timestamp (ms) when the query began execution.
     */
    public long getStartTime();

    /**
     * The timestamp (ms) when the query was done and ZERO (0) if the query is
     * not yet done.
     */
    public long getDoneTime();

    /**
     * The elapsed time (ms) for the query. This will be updated for each call
     * until the query is done executing.
     */
    public long getElapsed();
    
    /**
//     * Cancel the running query (normal termination).
//     * <p>
//     * Note: This method provides a means for an operator to indicate that the
//     * query should halt immediately for reasons other than abnormal
//     * termination.
//     * <p>
//     * Note: For abnormal termination of a query, just throw an exception out of
//     * the query operator implementation.
//     */
//    void halt();
//
//    /**
//     * Cancel the query (abnormal termination).
//     * 
//     * @param t
//     *            The cause.
//     *            
//     * @return The argument.
//     * 
//     * @throws IllegalArgumentException
//     *             if the argument is <code>null</code>.
//     */
//    Throwable halt(final Throwable t);
//
//    /**
//     * Return the cause if the query was terminated by an exception.
//     * @return
//     */
//    Throwable getCause();
    
    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     * 
     * @throws UnsupportedOperationException
     *             if this is not the query controller.
     */
    IAsynchronousIterator<IBindingSet[]> iterator();
    
}
