/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 30, 2008
 */

package com.bigdata.relation;

import java.util.concurrent.ExecutorService;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.LocalDataServiceFederation.LocalDataServiceImpl;

/**
 * Factory for views of a class of {@link IRelation} against some specific
 * resource. The indices backing the {@link IRelation} need not exist. If the
 * caller desires then can use {@link IMutableRelation#create()} to create the
 * backing indices (assuming that the view allows mutation, etc).
 * <p>
 * Note: There are several variants of <em>newRelation</em> declared - each
 * one handles a different context within which the {@link IRelation} instance
 * is being resolved (temporary store, local journal, federation, or abstract
 * task running within a local data service).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type of the [R]elation.
 */
public interface IRelationFactory<R> {

    /**
     * Used when accessing an {@link IRelation} on a local {@link Journal}.
     * 
     * @param service
     *            A service to which you can submit tasks not requiring
     *            concurrency control.
     * @param journal
     *            The index views will be obtained from the {@link Journal}.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     */
    public IRelation<R> newRelation(ExecutorService service, Journal journal,
            String namespace, long timestamp);

    /**
     * Used when accessing an {@link IRelation} on a local
     * {@link TemporaryStore}.
     * <p>
     * Note that the {@link TemporaryStore} only supports named indices but only
     * {@link ITx#UNISOLATED} views so there is no timestamp parameter for this
     * ctor.
     * 
     * @param service
     *            A service to which you can submit tasks not requiring
     *            concurrency control.
     * @param tempStore
     *            The index views will be obtained from the
     *            {@link TemporaryStore}.
     * @param namespace
     *            The namespace for the relation.
     */
    public IRelation<R> newRelation(ExecutorService service,
            TemporaryStore tempStore, String namespace);

    /**
     * Used when accessing an {@link IRelation} via an
     * {@link IBigdataFederation}.
     * 
     * @param fed
     *            The federation.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     */
    public IRelation<R> newRelation(IBigdataFederation fed, String namespace,
            long timestamp);

    /**
     * Used when accessing an {@link IRelation} from within an
     * {@link AbstractTask} running on a {@link LocalDataServiceImpl}. This
     * variant is only useful with the {@link LocalDataServiceFederation}. It
     * is used primarily to execute {@link IProgram}s inside of the
     * {@link LocalDataServiceImpl}, which is very efficient.
     * 
     * @param service
     *            A service to which you can submit tasks not requiring
     *            concurrency control.
     * @param task
     *            The task that is being executed.
     * @param namespace
     *            The namespace for the relation.
     */
    public IRelation<R> newRelation(ExecutorService service, AbstractTask task,
            String namespace);

    /**
     * Core impl (all other methods should delegate to this one).
     * 
     * @param service
     *            A service to which you can submit tasks not requiring
     *            concurrency control.
     * @param indexManager
     *            The index views will be obtained from the {@link Journal}.
     * @param namespace
     *            The namespace for the relation.
     * @param timestamp
     *            The timestamp for the view of the relation.
     *            
     * @return The {@link IRelation}.
     */
    public IRelation<R> newRelation(ExecutorService service,
            IIndexManager indexManager, String namespace, long timestamp);

}
