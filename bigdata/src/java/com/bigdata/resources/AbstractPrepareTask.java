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
 * Created on Feb 2, 2009
 */

package com.bigdata.resources;

import java.lang.ref.SoftReference;
import java.util.Arrays;

import com.bigdata.service.Event;
import com.bigdata.service.EventType;
import com.bigdata.service.IBigdataFederation;

/**
 * Base class for the prepare phase which reads on the old journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractPrepareTask<T> extends
        AbstractResourceManagerTask<T> {

    /**
     * The action implemented by this task.
     */
    protected final OverflowActionEnum action;

    /**
     * The event used to report on this task within its {@link #doTask()}
     * method.
     */
    protected final Event e;
    
    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     */
    public AbstractPrepareTask(ResourceManager resourceManager, long timestamp,
            String resource, OverflowActionEnum action) {

        this(resourceManager, timestamp, new String[] { resource }, action);

    }

    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     */
    public AbstractPrepareTask(ResourceManager resourceManager, long timestamp,
            String[] resource, OverflowActionEnum action) {

        super(resourceManager, timestamp, resource);

        if (action == null)
            throw new IllegalArgumentException();

        final IBigdataFederation fed = resourceManager.getFederation();
        
        this.action = action;

        switch (action) {
        case Build:
            e = new Event(fed, EventType.IndexPartitionBuild, Arrays
                    .toString(resource));
            break;
        case Merge:
            e = new Event(fed, EventType.IndexPartitionMerge, Arrays
                    .toString(resource));
            break;
        case Join:
            e = new Event(fed, EventType.IndexPartitionJoin, Arrays
                    .toString(resource));
            break;
        case Move:
            e = new Event(fed, EventType.IndexPartitionMove, Arrays
                    .toString(resource));
            break;
        case Split:
            e = new Event(fed, EventType.IndexPartitionSplit, Arrays
                    .toString(resource));
            break;
        default:
            throw new AssertionError(action);
        }
        
    }

    /**
     * Method is responsible for clearing the {@link SoftReference}s held by
     * {@link ViewMetadata} for the source view(s) on the old journal.
     * <p>
     * Note: This method MUST be invoked in order to permit those references to
     * be cleared more eagerly than the end of the entire asynchronous overflow
     * operation (which is when the task references would themselves go out of
     * scope and become available for GC).
     */
    abstract protected void clearRefs();
    
}
