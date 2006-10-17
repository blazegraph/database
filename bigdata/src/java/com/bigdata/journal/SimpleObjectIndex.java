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
    final Map<Integer,Integer> objectIndex = new HashMap<Integer,Integer>();

    /**
     * When non-null, this is the base (or inner) object index that represents
     * the committed object index state as of the time that a transaction began.
     */
    final IObjectIndex baseObjectIndex;
    
    /**
     * Constructor used for the base object index (outside of any transactional
     * scope).
     */
    public SimpleObjectIndex() {

        this.baseObjectIndex = null;
        
    }

    /**
     * Constructor used to isolate a transaction by a read-only read-through
     * view of some committed object index state.
     * 
     * @param baseObjectIndex
     *            When non-null, misses on the primary object index MUST read
     *            through. Writes are ONLY performed on the primary object
     *            index. The base object index is always read-only.
     */
    public SimpleObjectIndex(IObjectIndex baseObjectIndex) {

        this.baseObjectIndex = baseObjectIndex;
        
    }

    /**
     * Read an entry from the object index. If there is a miss on the outer
     * index and an inner index is defined then try a read on the inner index.
     * 
     * @param id
     *            The int32 within segment persistent identifier.
     * 
     * @return The first slot to which that persistent identifier is mapped or
     *         {@link IObjectIndex#NOTFOUND}.
     */
    private Integer get(int id) {
        
        Integer firstSlot = objectIndex.get(id );
        
        if( firstSlot == null ) {

            if( baseObjectIndex != null ) {
            
                return baseObjectIndex.getFirstSlot(id);
                
            } else {
                
                return NOTFOUND;
                
            }
            
        } else {
        
            return firstSlot;
            
        }
        
    }
    
    public void mapIdToSlot( int id, int slot ) {
        
        assert slot >= 0;

        // Update the object index.
        Integer oldSlot = objectIndex.put(id, slot);
        
        if( oldSlot != null ) {
            
            throw new AssertionError("Already mapped to slot=" + oldSlot
                    + ": id=" + id + ", slot=" + slot);
            
        }
        
    }

    public int getFirstSlot( int id ) {

        Integer firstSlot = get(id);
        
        int slot = firstSlot.intValue();
        
        if( slot == NOTFOUND ) return NOTFOUND;
        
        if( slot < 0 ) {
            
            throw new DataDeletedException(id);
            
        }
        
        return slot;
        
    }
    
    public void delete(int id) {

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
            
            throw new IllegalStateException("Already deleted: id=" + id);
            
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
