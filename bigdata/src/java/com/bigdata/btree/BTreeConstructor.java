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

import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * Create an instance of a {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTreeConstructor implements IIndexConstructor {

    /**
     * 
     */
    private static final long serialVersionUID = -5146328320212890139L;

    private int branchingFactor;
    
    IValueSerializer valueSerializer;
    
    /**
     * De-serialization constructor.
     */
    public BTreeConstructor() {
        
    }

    /**
     * 
     * @param valueSerializer
     *            The object used to (de-)serialize values in the {@link BTree}.
     */
    public BTreeConstructor(IValueSerializer valueSerializer) {

        this(BTree.DEFAULT_BRANCHING_FACTOR,valueSerializer);
        
    }
    
    /**
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @param valueSerializer
     *            The object used to (de-)serialize values in the {@link BTree}.
     */
    public BTreeConstructor(int branchingFactor, IValueSerializer valueSerializer) {
        
        if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {
            
            throw new IllegalArgumentException();
            
        }

        if (valueSerializer == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.branchingFactor = branchingFactor;
        
        this.valueSerializer = valueSerializer;
        
    }
    
    public BTree newInstance(IRawStore store, UUID indexUUID, IPartitionMetadata ignored) {

        return new BTree(store, branchingFactor, indexUUID, valueSerializer);
        
    }

}
