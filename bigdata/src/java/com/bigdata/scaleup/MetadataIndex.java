/**

 The Notice below must appear in each file of the Source Code of any
 copy you distribute of the Licensed Product.  Contributors to any
 Modifications may add their own copyright notices to identify their
 own contributions.

 License:

 The contents of this file are subject to the CognitiveWeb Open Source
 License Version 1.1 (the License).  You may not copy or use this file,
 in either source code or executable form, except in compliance with
 the License.  You may obtain a copy of the License from

 http://www.CognitiveWeb.org/legal/license/

 Software distributed under the License is distributed on an AS IS
 basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 the License for the specific language governing rights and limitations
 under the License.

 Copyrights:

 Portions created by or assigned to CognitiveWeb are Copyright
 (c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
 information for CognitiveWeb is available at

 http://www.CognitiveWeb.org

 Portions Copyright (c) 2002-2003 Bryan Thompson.

 Acknowledgements:

 Special thanks to the developers of the Jabber Open Source License 1.0
 (JOSL), from which this License was derived.  This License contains
 terms that differ from JOSL.

 Special thanks to the CognitiveWeb Open Source Contributors for their
 suggestions and support of the Cognitive Web.

 Modifications:

 */
package com.bigdata.scaleup;


import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * A metadata index for the partitions of a distributed index. There is one
 * metadata index for each distributed index. The keys of the metadata index are
 * the first key that would be directed into the corresponding index segment,
 * e.g., a <em>separator key</em> (this is just the standard btree semantics).
 * The values are {@link PartitionMetadata} objects.
 * 
 * @todo locator logic on a cluster (a socket address in addition to the other
 *       information).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo define a UUID so that is at least possible to rename a partitioned
 *       index? the uuid would be store in the metadata record for the metadata
 *       index and in each index segment generated for that metadata index. we
 *       could also define a partition uuid. finally, each btree and index
 *       segment could have its own uuid. the index segment would also carry the
 *       uuid of the partition and the partitioned index. This would also make
 *       it possible to determine which index segments belong to which
 *       partitions of which partitioned indices and effectively reconstruct the
 *       metadata index for a partitioned index from the data on the ground.
 */
public class MetadataIndex extends BTree {

    /**
     * The name of the metadata index, which is the always the same as the name
     * under which the corresponding {@link PartitionedIndex} was registered.
     */
    private final String name;
    
    /**
     * The name of the metadata index, which is the always the same as the name
     * under which the corresponding {@link PartitionedIndex} was registered.
     */
    final public String getName() {
        
        return name;
        
    }
    
    /**
     * Create a new {@link MetadataIndex}.
     * 
     * @param store
     *            The backing store.
     * @param branchingFactor
     *            The branching factor.
     * @param name
     *            The name of the metadata index - this MUST be the name under
     *            which the corresponding {@link PartitionedIndex} was
     *            registered.
     */
    public MetadataIndex(IRawStore store, int branchingFactor, String name) {

        super(store, branchingFactor, PartitionMetadata.Serializer.INSTANCE);
        
        this.name = name;
        
    }

    public MetadataIndex(IRawStore store, BTreeMetadata metadata) {
        
        super(store, metadata);
        
        name = ((MetadataIndexMetadata)metadata).name;
        
    }

    protected BTreeMetadata newMetadata() {
        
        return new MetadataIndexMetadata(this);
        
    }

    /**
     * Extends the {@link BTreeMetadata} record to also hold the name of the
     * partitioned index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MetadataIndexMetadata extends BTreeMetadata {

        private static final long serialVersionUID = -7309267778881420043L;
        
        /**
         * The name of the metadata index, which is the always the same as the name
         * under which the corresponding {@link PartitionedIndex} was registered.
         */
        public final String name;
        
        /**
         * @param mdi
         */
        protected MetadataIndexMetadata(MetadataIndex mdi) {

            super(mdi);
            
            this.name = mdi.name;
            
        }

    }
    
    /**
     * Find the index of the partition spanning the given key.
     * 
     * @return The index of the partition spanning the given key or
     *         <code>-1</code> iff there are no partitions defined.
     * 
     * @exception IllegalStateException
     *                if there are partitions defined but no partition spans the
     *                key. In this case the {@link MetadataIndex} lacks an entry
     *                for the key <code>new byte[]{}</code>.
     */
    public int findIndexOf(byte[] key) {
        
        int pos = super.indexOf(key);
        
        if (pos < 0) {

            /*
             * the key lies between the partition separators and represents the
             * insert position.  we convert it to an index and subtract one to
             * get the index of the partition that spans this key.
             */
            
            pos = -(pos+1);

            if(pos == 0) {

                if(nentries != 0) {
                
                    throw new IllegalStateException(
                            "Partition not defined for empty key.");
                    
                }
                
                return -1;
                
            }
                
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on a partition separator, so we choose the entry with
             * that key.
             */
            
            return pos;
            
        }

    }
    
    /**
     * Find and return the partition spanning the given key.
     * 
     * @return The partition spanning the given key or <code>null</code> if
     *         there are no partitions defined.
     */
    public PartitionMetadata find(byte[] key) {
        
        final int index = findIndexOf(key);
        
        if(index == -1) return null;
        
        return (PartitionMetadata) super.valueAt( index );
        
    }
    
    /**
     * The partition with that separator key or <code>null</code> (exact
     * match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The partition with that separator key or <code>null</code>.
     */
    public PartitionMetadata get(byte[] key) {
        
        return (PartitionMetadata) super.lookup(key);
        
    }
    
    /**
     * Create or update a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * @param val
     *            The parition metadata.
     * 
     * @return The previous partition metadata for that separator key or
     *         <code>null</code> if there was no partition for that separator
     *         key.
     * 
     * @exception IllegalArgumentException
     *                if the key identifies an existing partition but the
     *                partition identifers do not agree.
     */
    public PartitionMetadata put(byte[] key,PartitionMetadata val) {
        
        if (val == null) {

            throw new IllegalArgumentException();

        }
        
        PartitionMetadata oldval = (PartitionMetadata) super.insert(key,
                val);

        if (oldval != null && oldval.partId != val.partId) {

            throw new IllegalArgumentException("Expecting: partId="
                    + oldval.partId + ", but have partId=" + val.partId);

        }

        return oldval;
    
    }

    /**
     * Remove a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The existing partition for that separator key or
     *         <code>null</code> if there was no entry for that separator key.
     */
    public PartitionMetadata remove(byte[] key) {
        
        return (PartitionMetadata) super.remove(key);
        
    }

}
