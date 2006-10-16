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
 * FIXME This is a prototype implementation in order to proof out the concept
 * for transactional isolation. This implementation NOT persistence capable;
 * does NOT (yet) read through to a read-only delegate; does NOT handle index
 * merging during commit; etc.
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

    final IObjectIndex baseObjectIndex;
    
    public SimpleObjectIndex() {

        this.baseObjectIndex = null;
        
    }
    
    public SimpleObjectIndex(IObjectIndex baseObjectIndex) {

        this.baseObjectIndex = baseObjectIndex;
        
    }
        
    public void mapIdToSlot( int id, int slot ) {
        
        assert slot != -1;

        // Update the object index.
        Integer oldSlot = objectIndex.put(id, slot);
        
        if( oldSlot != null ) {
            
            throw new AssertionError("Already mapped to slot=" + oldSlot
                    + ": id=" + id + ", slot=" + slot);
            
        }
        
    }

    public int getFirstSlot( int id ) {

        Integer firstSlot = objectIndex.get(id);
        
        if( firstSlot == null ) return NOTFOUND;
        
        int slot = firstSlot.intValue();
        
        if( slot < 0 ) {
            
            throw new DataDeletedException(id);
            
        }
        
        return slot;
        
    }
    
    public void delete(int id) {

        // Get the object index entry.
        Integer firstSlot = objectIndex.get(id);

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
        
        // Update the index to store the negative slot index.
        objectIndex.put(id, -(firstSlot+1));
        
    }

    public int removeDeleted(int id) {
    
        // Remove the object index entry, recovering its contents. 
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
