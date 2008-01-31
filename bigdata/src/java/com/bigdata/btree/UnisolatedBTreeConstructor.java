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
 * Created on Jan 11, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * Creates an {@link UnisolatedBTree} instance.
 * <p>
 * The keys of the created index will be variable length unsigned byte[]s. The
 * values will be variable length byte[]s. The index will support version
 * counters and delete markers (it will be compatible with the both use of
 * transactions for concurrency control and the use of history polices for
 * purging data during compacting merges).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnisolatedBTreeConstructor implements IIndexConstructor {

    /**
     * 
     */
    private static final long serialVersionUID = 4752856620314740637L;

    private int branchingFactor;
    
    IKeySerializer keySerializer;

    IValueSerializer valueSerializer;
    
    private IConflictResolver conflictResolver;
    
    /**
     * De-serialization constructor.
     */
    public UnisolatedBTreeConstructor() {
        
    }

//    public UnisolatedBTreeConstructor(int branchingFactor) {
//
//        this(branchingFactor, null/*conflictResolver*/);
//
//    }

    /**
     * 
     * @param branchingFactor
     *            The branching factor.
     * @param keySerializer
     *            The object used to (de-)serialize the keys in the
     *            {@link BTree}.
     * @param valueSerializer
     *            The object used to (de-)serialize values in the {@link BTree}.
     * @param conflictResolver
     *            The conflict resolver (optional).
     */
    public UnisolatedBTreeConstructor(
            int branchingFactor,
            IKeySerializer keySerializer,
            IValueSerializer valueSerializer,
            IConflictResolver conflictResolver
            ) {

        if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {

            throw new IllegalArgumentException();

        }

        if (keySerializer == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        if (valueSerializer == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.branchingFactor = branchingFactor;

        this.keySerializer = keySerializer;
        
        this.valueSerializer = valueSerializer;
        
        this.conflictResolver = conflictResolver;
        
    }
    
    public BTree newInstance(IRawStore store, UUID indexUUID, IPartitionMetadata ignored) {

        return new UnisolatedBTree(store, branchingFactor, indexUUID,
                keySerializer, valueSerializer, conflictResolver);
        
    }

}
