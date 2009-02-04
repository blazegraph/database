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
 * Created on Feb 3, 2009
 */

package com.bigdata.service;

import com.bigdata.journal.Journal;
import com.bigdata.resources.OverflowActionEnum;

/**
 * Type safe enum for {@link Event}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum EventType {

    /**
     * Synchronous overflow is a fast operation that occurs when the live
     * journal is nearly at its maximum capacity. The index partition views are
     * redefined onto a new journal during synchronous overflow, but little or
     * no data is copied off of the old journal.
     */
    SynchronousOverflow,
    
    /**
     * Asynchronous overflow migrates data off of the old journal and onto
     * read-optimized index segments and is responsible for running operation
     * which split, join, or move index partitions.
     */
    AsynchronousOverflow,

    /*
     * Asynchronous overflow tasks.
     */
    
    /**
     * Move the index partition to another data service. Note that moves either
     * to redistribute the load more equitably among the data services in the
     * federation or to bring the left/right sibling of an index partition onto
     * the same data service as its right/left sibling so that they may be
     * joined.
     */
    IndexPartitionMove,

    /**
     * Join left- and right- index partition siblings which have underflowed.
     */
    IndexPartitionJoin,

    /**
     * Split an index partition that has overflowed into 2 or more siblings.
     */
    IndexPartitionSplit,

    /**
     * Copy the tuples from the last commit point on the old journal into an
     * index segment. Builds are done both in order to improve read performance
     * and to release dependencies on older journals.
     */
    IndexPartitionBuild,

    /**
     * Compacting merge of the sources for the index partition into a single
     * index segment. Compacting merges are done to improve read performance and
     * to keep index partition views from including too many distinct index
     * segment sources.
     */
    IndexPartitionMerge,
    
    /*
     * Other kinds of events.
     */
    
    /**
     * Operation that builds an index segment. Builds may either be simple (from
     * the {@link BTree} on a {@link Journal} used to absorb writes for an index
     * partition) or compacting merges (from the full view of the index
     * partition).
     */
    IndexSegmentBuild,
    
    /**
     * Operation responsible for the atomic update of the index partition view
     * as part of any of the asynchronous overflow tasks
     * 
     * @see OverflowActionEnum
     */
    AtomicViewUpdate;
    
}
