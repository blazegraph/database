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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.service.IBigdataFederation;

/**
 * Generic implementation relies on a ctor for the relation with the following
 * method signature:
 * 
 * <pre>
 * public RELATION ( ExecutorService service, IIndexManager indexManager, String namespace, Long timestamp )
 * </pre>
 * 
 * Concrete implementations may override
 * {@link #newRelation(ExecutorService, IIndexManager, String, long)} to perform
 * additional initialization and are encouraged to strengthen the return types
 * for the {@link IRelationFactory} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRelationFactory<R> implements IRelationFactory<R>, Serializable {
    
    /**
     * De-serialization ctor.
     */
    public AbstractRelationFactory() {
        
    }

    /**
     * The {@link Class} of the {@link IRelation} implementation.
     */
    abstract protected Class<? extends IRelation<R>> getRelationClass(); 
    
    public IRelation<R> newRelation(ExecutorService service, Journal journal,
            String namespace, long timestamp) {

        if (service == null)
            throw new IllegalArgumentException();

        if (journal == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        return newRelation(service, journal, namespace, timestamp);

    }

    public IRelation<R> newRelation(ExecutorService service,
            TemporaryStore tempStore, String namespace) {

        if (service == null)
            throw new IllegalArgumentException();

        if (tempStore == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        return newRelation(service, tempStore, namespace, ITx.UNISOLATED);

    }

    public IRelation<R> newRelation(IBigdataFederation fed, String namespace,
            long timestamp) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        return newRelation(fed.getThreadPool(), fed, namespace, timestamp);

    }

    public IRelation<R> newRelation(ExecutorService service, AbstractTask task,
            String namespace) {

        if (service == null)
            throw new IllegalArgumentException();

        if (task == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        return newRelation(service, task.getJournal(), namespace, task
                .getTimestamp());

    }

    public IRelation<R> newRelation(ExecutorService service,
            IIndexManager indexManager, String namespace, long timestamp) {

        final Class<? extends IRelation<R>> cls = getRelationClass();
        
        final Constructor<? extends IRelation<R>> ctor;
        try {

            ctor = cls.getConstructor(new Class[] {//
                    ExecutorService.class,// 
                    IIndexManager.class,//
                    String.class,// relation namespace
                    Long.class // timestamp
                    });

        } catch (Exception e) {

            throw new RuntimeException("No appropriate ctor?: cls="
                    + cls.getName() + " : " + e, e);

        }

        final IRelation<R> r;
        try {

            r = ctor.newInstance(new Object[] { service, indexManager,
                    namespace, timestamp });

            return r;

        } catch (Exception ex) {

            throw new RuntimeException("Could not instantiate relation: " + ex,
                    ex);

        }

    }

}
