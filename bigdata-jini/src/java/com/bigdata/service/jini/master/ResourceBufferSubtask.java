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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BigdataSet;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.ndx.pipeline.AbstractPendingSetSubtask;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResourceBufferSubtask<//
H extends ResourceBufferStatistics<L, HS>, //
O extends Object, //
E extends Serializable, //
L extends ClientLocator, //
S extends ResourceBufferSubtask, //
HS extends ResourceBufferSubtaskStatistics,//
M extends ResourceBufferTask<H, E, S, L, HS>,//
T extends IKeyArrayIndexProcedure,//
A//
> extends AbstractPendingSetSubtask<HS, M, E, L> {

    /**
     * The set of work items for which there are pending asynchronous
     * operations. Entries are cleared from this set as soon as ANY client has
     * successfully completed the work for that item.
     */
    private final Set<E> pendingSet;
    
    protected Set<E> getPendingSet() {
        
        return pendingSet;
        
    }
    
    public ResourceBufferSubtask(final M master, final L locator,
            final IAsynchronousClientTask<?, E> clientTask,
            final BlockingBuffer<E[]> buffer) {

        super(master, locator, clientTask, buffer);

        this.pendingSet = newPendingSet();

    }

    protected Set<E> newPendingSet() {

        final int initialCapacity = ((MappedTaskMaster.JobState) master.taskMaster
                .getJobState()).pendingSetSubtaskInitialCapacity;

        if (initialCapacity == Integer.MAX_VALUE) {

            final IRawStore store = master.getFederation().getTempStore();

            // anonymous index (unnamed)
            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

            final BTree ndx = BTree.create(store, metadata);

            return new BigdataSet<E>(ndx);

        } else {

            return new LinkedHashSet<E>(initialCapacity);

        }

    }

}
