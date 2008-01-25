package com.bigdata.mdi;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndexConstructor;
import com.bigdata.rawstore.IRawStore;

/**
 * Creates an {@link UnisolatedBTreePartition} instance.
 * <p>
 * Note: Since scale-out indices require isolation support in order to perform
 * compacting merges this is the base class constructor implementation for
 * creating mutable {@link BTree} instances that absorb writes for scale-out
 * indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnisolatedBTreePartitionConstructor implements IIndexConstructor {

    /**
     * 
     */
    private static final long serialVersionUID = 5758014871939321170L;

    protected int branchingFactor;
    
    /**
     * De-serialization constructor.
     */
    public UnisolatedBTreePartitionConstructor() {
     
        if(branchingFactor==0) {
            
            // @todo verify that this default is overriden by de-serialization.
            branchingFactor = UnisolatedBTreePartition.DEFAULT_BRANCHING_FACTOR;
            
        }
        
    }

    /**
     * 
     * @param branchingFactor
     *            The branching factor.
     */
    public UnisolatedBTreePartitionConstructor(int branchingFactor) {
        
        if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {
            
            throw new IllegalArgumentException();
            
        }

        this.branchingFactor = branchingFactor;
        
    }
    
    public BTree newInstance(IRawStore store, UUID indexUUID,IPartitionMetadata pmd) {

        return new UnisolatedBTreePartition(store, branchingFactor, indexUUID,
                (PartitionMetadataWithSeparatorKeys) pmd);
        
    }

}
