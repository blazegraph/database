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
/*
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a prototype implementation in order to proof out the concept for
 * transactional isolation. This implementation NOT persistence capable.
 * 
 * @todo Write lots of tests -- we will reuse those for the persistence capable
 *       object index implementation.
 * 
 * @todo Handle index merging during commit.
 * 
 * @todo Write a persistence capable version that is efficient for all the
 *       things that we actually use the object index for. We need benchmarks
 *       that drive all of those activities (including migration to the
 *       read-optimized database and deletion of versions that are no longer
 *       accessible) in order to drive the algorithm, implementation, and
 *       performance tuning.<br>
 *       Some of the complex points are handling transaction isolation
 *       efficiently with minimal duplication of data and high performance and,
 *       in interaction with the slot allocation index, providing fast
 *       deallocation of slots no longer used by any active transaction - the
 *       current scheme essentially forces visitation of the slots to be
 *       deleted.
 * 
 * @todo A larger branching factor in the object index will result in fewer
 *       accesses to resolve a given identifier. This does not matter much when
 *       the journal is fully buffered, but it is critical when a journal uses a
 *       page buffer (unless the base object index and the per transaction
 *       object indices can be wired into memory).<br>
 *       A large branching factor is naturally in opposition to a smaller slot
 *       size. If larger nodes are used with smaller slot sizes then each index
 *       node will occupy multiple slots. It is possible to force allocation of
 *       those slots such that they are contiguous - this approach has the
 *       advantage that the journal remains an append-only store, but introduces
 *       complexity in allocation of index nodes. My expectation is that most
 *       allocations will be much smaller than index node allocations, but that
 *       many allocations will be made together since writes are buffered on a
 *       per tx basis before being applied to the journal. If it is also true
 *       that we tend to release the allocations for entire transactions at
 *       once, then this reduces the likelyhood that we will exhaust an extent
 *       for index node allocations through fragmentation. <br>
 *       Another approach would partition the journal (either within one file or
 *       into two files) so that part was reserved for object index nodes and
 *       part was reserved for slots. This would, pragmatically, result in two
 *       memory spaces each having fixed length slots - one for the objects and
 *       the slot allocation index blocks and one for the object index nodes.
 *       However, partitioning also goes directly against the requirement for an
 *       append-only store and would result in, essentially, twice as many
 *       append only data structures - and hence twice the disk head movement as
 *       a design that does not rely on either within file partition or a two
 *       file scheme.<br>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleObjectIndex implements IObjectIndex {

    /**
     * <p>
     * Map from the int32 within segment persistent identifier to the first slot
     * on which the identifier was written.
     * </p>
     * <p>
     * Note: A deleted version is internally coded as a negative slot
     * identifier. The negative value is computed as
     * </p>
     * <pre>
     * negIndex = -(firstSlot + 1)
     * </pre>
     * <p>
     * The actual slot where the deleted version was written is recovered using:
     * </p> 
     * <pre>
     * firstSlot = (-negIndex - 1);
     * </pre>
     */
    final Map<Integer,Integer> objectIndex;

    /**
     * When non-null, this is the base (or inner) object index that represents
     * the committed object index state as of the time that a transaction began.
     */
    final SimpleObjectIndex baseObjectIndex;
    
    /**
     * Constructor used for the base object index (outside of any transactional
     * scope).
     */
    public SimpleObjectIndex() {

        this.objectIndex = new HashMap<Integer,Integer>();

        this.baseObjectIndex = null;

    }

    /**
     * Private constructor creates a read-only (unmodifiable) deep-copy of the
     * supplied object index state.  This is used to provide a strong guarentee
     * that object index modifications can not propagate through to the inner
     * layer using this API.
     * 
     * @param objectIndex
     */
    private SimpleObjectIndex(Map<Integer,Integer> objectIndex ) {

        Map<Integer,Integer> copy = new HashMap<Integer,Integer>();
        copy.putAll( objectIndex );
        
        this.objectIndex = Collections.unmodifiableMap(copy);

        this.baseObjectIndex = null;

    }
    
    /**
     * Constructor used to isolate a transaction by a read-only read-through
     * view of some committed object index state.
     * 
     * @param baseObjectIndex
     *            Misses on the primary object index read through to this object
     *            index. Writes are ONLY performed on the primary object index.
     *            The base object index is always read-only.
     */
    public SimpleObjectIndex(SimpleObjectIndex baseObjectIndex) {

        assert baseObjectIndex != null;
        
        /*
         * Note: This makes a deep copy of the then current object index. This
         * provides isolation against changes to the object index on the
         * journal.
         * 
         * @todo An efficient implementation MUST NOT make an eager deep copy.
         */

        this.objectIndex = new HashMap<Integer,Integer>();

        this.baseObjectIndex = new SimpleObjectIndex(baseObjectIndex.objectIndex);
        
    }

    /**
     * Read an entry from the object index. If there is a miss on the outer
     * index and an inner index is defined then try a read on the inner index.
     * 
     * @param id
     *            The int32 within segment persistent identifier.
     * 
     * @return The entry for that persistent identifier or null if no entry was
     *         found.
     * 
     * @see #hitOnOuterIndex()
     */
    private Integer get(int id) {
        
        Integer firstSlot = objectIndex.get(id );
        
        if( firstSlot == null && baseObjectIndex != null ) {
            
            firstSlot = baseObjectIndex.objectIndex.get(id);
        
            hitOnOuterIndex = false;
            
        } else {
            
            hitOnOuterIndex = true;
            
        }
        
        return firstSlot;
        
    }
    
    /**
     * This field is set by {@link #getFirstSlot(int)} and MAY be reset by any
     * susequent method invoked against this object index. The field will be
     * true iff there was a hit on the object index and the hit occurred on the
     * outer layer - that is, the layer to which the request was directed. When
     * true, this bit may be interpreted as indicating that a version to be
     * overwritten was written within the transaction scope IFF this object
     * index is providing transactional isolation (vs the journal's native level
     * object index). In this situation the slots allocated to the version that
     * is being overwritten SHOULD be immediately released on the journal since
     * it is impossible for any transaction to read the version stored on those
     * slots.
     */
    boolean hitOnOuterIndex = false;
    
    public int getFirstSlot( int id ) {

        Integer firstSlot = get(id);
        
        if( firstSlot == null ) return NOTFOUND;
        
        int slot = firstSlot.intValue();
        
        if( slot < 0 ) {
            
            throw new DataDeletedException(id);
            
        }
        
        return slot;
        
    }
    
    public void mapIdToSlot( int id, int slot, boolean overwrite ) {
        
        assert slot >= 0;

        // Update the object index.
        Integer oldSlot = objectIndex.put(id, slot);
        
        /*
         * Note: everything below here is just to validate our expectation
         * concerning whether or not we are overwriting an existing version. In
         * order to find the entry for the overwritten layer (since the object
         * index map has two layers) we have to read on the inner layer if the
         * put() on the outer layer does not return a pre-existing entry.
         * 
         * Note: I expect that this will all change drammatically when we move
         * to a persistent and restart safe object index data structure.
         */
        
        if( oldSlot == null && baseObjectIndex != null ) {
            
            // read the entry from the inner layer.
            
            oldSlot = baseObjectIndex.objectIndex.get(id);
            
        }
        
        if( overwrite ) {

            if( oldSlot == null ) {
                
                throw new IllegalStateException(
                        "Identifier is not mapped: id="
                        + id + ", slot=" + slot + ", overwrite=" + overwrite);
                
            }
            
        } else {
        
            if (oldSlot != null) {

                throw new IllegalStateException(
                        "Identifier already mapped: id=" + id + ", oldSlot="
                                + oldSlot + ", newSlot=" + slot);

            }

        }
        
    }

    public int delete(int id) {

        // Get the object index entry.
        Integer firstSlot = get(id);

        if( firstSlot == null ) {
            
            throw new IllegalArgumentException("Not found: id="+id);
            
        }

        // Convert to int.
        final int firstSlot2 = firstSlot.intValue();
        
        if( firstSlot2 < 0 ) {
            
            /*
             * A negative slot index is used to mark objects that have been
             * deleted from the object index but whose slots have not yet been
             * released.  It is an error to double-delete an object.
             */
            
            throw new DataDeletedException(id);
            
        }
        
        /*
         * Update the index to store the negative slot index. The change is
         * always applied to the outer index, even if the object was located
         * using the inner index. E.g., if the object was created in a previous
         * transaction and is deleted in this transaction, then the object is
         * resolved against the read-only object index for the historical state
         * and the deleted flag is set within the transactional scope.
         */
        objectIndex.put(id, -(firstSlot2+1));

        return firstSlot2;
        
    }

    public int removeDeleted(int id) {
    
        /*
         * Remove the object index entry, recovering its contents.
         * 
         * Note: Unlike all other methods that read on the object index, this
         * one does NOT read through to the inner index. The reason is that
         * deleted entries are only written on the outer index. If we read
         * through to the inner index then we will always find the un-deleted
         * entry (if there is any entry for the identifier).
         */ 

        Integer negIndex = objectIndex.remove(id);
        
        if( negIndex == null ) {

            throw new IllegalArgumentException("Not found: id="+id);
            
        }

        // Convert to int.
        final int negIndex2 = negIndex.intValue();
        
        if( negIndex2 >= 0 ) {
            
            /*
             * A negative slot index is used to mark objects that have been
             * deleted from the object index but whose slots have not yet been
             * released.
             */
            
            throw new IllegalStateException("Not deleted: id=" + id);
            
        }
        
        // Convert back to a non-negative slot index.
        final int firstSlot = (-negIndex - 1);

        return firstSlot;
    
    }

}
