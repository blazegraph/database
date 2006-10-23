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
 * Created on Oct 22, 2006
 */
package com.bigdata.journal;


/**
 * Basic implementation should be used when the #of slots to be allocated is
 * known in advance and there are at least two slots to be allocated.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CompactSlotAllocation implements ISlotAllocation {

    /**
     * True iff the allocation is closed.
     */
    private transient boolean closed = false;
    
    private final int nbytes;
    
    /**
     * The ordered list of slots.
     */
    private final transient int[] slots;
    
    /**
     * The #of slots added so far.
     */
    private transient int n = 0;
    
    /**
     * The current cursor position or -1 if the iterator is exhausted or has
     * not begun.
     */
    private transient int cursor = -1;
    
    /**
     * Constructor
     * 
     * @param nbytes
     *            The #of bytes for version writte on the slots in this
     *            allocation.
     * @param nslots
     *            The #of slots on which the version will be written.
     * 
     * @see SingletonSlotAllocation, which is more efficient when there will be
     *      just one slot in the allocation.
     */
    public CompactSlotAllocation(int nbytes,int nslots) {
        
        if (nbytes <= 0)
            throw new IllegalArgumentException("nbytes=" + nbytes
                    + " is non-positive.");

        if (nslots <= 0)
            throw new IllegalArgumentException("nslots=" + nslots
                    + " is non-positive.");

        this.nbytes = nbytes;
        
        this.slots = new int[ nslots ];
        
    }

    public int getByteCount() {
        
        return nbytes;
        
    }
    
    public int capacity() {
        
        return slots.length;
        
    }
    
    public void add(int slot) {

        if( slot < 0 ) throw new IllegalArgumentException();

        if( n == slots.length ) throw new IllegalStateException("full");
        
        slots[ n++ ] = slot;
        
    }

    /**
     * @exception IllegalStateException
     *                if the #of slots written is less than the #of slots
     *                declared to the constructor.
     */
    public void close() {
        
        if( closed ) throw new IllegalStateException("Already closed.");
        
        if (n != slots.length)
            throw new IllegalStateException("Only " + n + " of " + slots.length
                    + " slots are defined.");
        
        closed = true;
        
    }

    public boolean isClosed() {
        
        return closed;
        
    }
    
    public int firstSlot() {
        
        // no slots.
        if( n == 0 ) throw new IllegalStateException();

        // next cursor position.
        cursor = 1;
        
        // first slot.
        return slots[ 0 ];
        
    }

    public int nextSlot() {

        if( cursor < 0 ) throw new IllegalStateException();

        if( cursor >= n ) {
            
            cursor = -1;
            
            return -1;
            
        }
        
        return slots[ cursor++ ];
        
    }

    public int getSlotCount() {

        return n;
        
    }

}
