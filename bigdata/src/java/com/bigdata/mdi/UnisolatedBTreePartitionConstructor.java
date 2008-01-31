package com.bigdata.mdi;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndexConstructor;
import com.bigdata.btree.IKeySerializer;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.Value;
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
    protected IKeySerializer keySer;
    protected IValueSerializer valSer;
    protected IConflictResolver conflictResolver;
    
    /**
     * De-serialization constructor.
     */
    public UnisolatedBTreePartitionConstructor() {
     
        if(branchingFactor==0) {
            
            // @todo verify that this default is overriden by de-serialization.
            branchingFactor = UnisolatedBTreePartition.DEFAULT_BRANCHING_FACTOR;
            
            keySer = KeyBufferSerializer.INSTANCE;
            
            valSer = Value.Serializer.INSTANCE;
            
            conflictResolver = null;
            
        }
        
    }

    /**
     * 
     * @param branchingFactor
     *            The branching factor.
     */
    public UnisolatedBTreePartitionConstructor(int branchingFactor) {
    
        this(branchingFactor, KeyBufferSerializer.INSTANCE,
                Value.Serializer.INSTANCE, null/* conflictResolver */);
        
    }

    public UnisolatedBTreePartitionConstructor(int branchingFactor,
            IKeySerializer keySerializer, IValueSerializer valueSerializer,
            IConflictResolver conflictResolver) {

        if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {

            throw new IllegalArgumentException();

        }

        this.branchingFactor = branchingFactor;

        if (keySerializer == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        if (valueSerializer == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.branchingFactor = branchingFactor;

        this.keySer = keySerializer;
        
        this.valSer = valueSerializer;
        
        this.conflictResolver = conflictResolver;
       
    }

    public BTree newInstance(IRawStore store, UUID indexUUID,
            IPartitionMetadata pmd) {

        return new UnisolatedBTreePartition(store, branchingFactor, indexUUID,
                keySer, valSer, conflictResolver,
                (PartitionMetadataWithSeparatorKeys) pmd);

    }

}
