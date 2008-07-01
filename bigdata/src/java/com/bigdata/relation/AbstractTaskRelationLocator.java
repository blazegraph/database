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
import com.bigdata.service.DataService;

/**
 * Locates a view of an {@link IRelation} using an {@link AbstractTask}. This
 * is useful IFF the caller is running as an {@link AbstractTask} on a
 * {@link DataService} and the indices for the {@link IRelation} are (a)
 * monolithic; (b) local; and (c) were declared when the {@link AbstractTask}
 * was created so that the task holds an appropriate lock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractTaskRelationLocator<R> extends AbstractCachingRelationLocator<R> {

    private final ExecutorService service;
    
    private final AbstractTask task;
    
    public AbstractTaskRelationLocator(ExecutorService service,
            AbstractTask task, IRelationFactory<R> relationFactory) {

        super(relationFactory);
        
        if (service == null)
            throw new IllegalArgumentException();

        if (task == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.task = task;

    }

    public AbstractTaskRelationLocator(ExecutorService service,
            AbstractTask task, IRelationFactory<R> relationFactory, int capacity) {

        super(relationFactory, capacity);
        
        if (service == null)
            throw new IllegalArgumentException();

        if (task == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.task = task;

    }

    synchronized public IRelation<R> getRelation(IRelationName<R> relationName,
            long timestamp) {

        if (relationName == null)
            throw new IllegalArgumentException();

        if(timestamp != task.getTimestamp()) {
            
            throw new IllegalArgumentException();
            
        }
        
        IRelation<R> relation = get(relationName, timestamp);

        if (relation == null) {

            final String namespace = relationName.toString();
            
            relation = getRelationFactory().newRelation(service, task, namespace);

            put(relation);
            
        }

        return relation;

    }

}
