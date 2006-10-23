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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A non-persistence capable implementation of the {@link ISlotAllocation}
 * interface that does not require the caller to declare the size (the #of
 * slots) of the allocation up front.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This implementation can be used if we support a streaming WRITE
 *             API but is not otherwise required.
 */
public class UnboundedSlotAllocation implements ISlotAllocation {

    /**
     * True iff the allocation is closed.
     */
    private boolean closed = false;

    /**
     * Initially zero, but it MUST be set explicitly before the allocation is
     * {@link #closed()}.
     */
    private int nbytes = 0;
    
    /**
     * The ordered list of slots.
     */
    final private List<Integer> slots = new LinkedList<Integer>();

    private transient Iterator<Integer> itr = null;
    
    /**
     * Creates an empty allocation.
     */
    public UnboundedSlotAllocation() {
    }

    /**
     * Initially zero and updated using {@link #setByteCount(int)}.
     * 
     * @return The current byte count.
     */
    public int getByteCount() {
        
        return nbytes;
        
    }

    /**
     * Update the byte count.
     * 
     * @param nbytes
     *            The #of bytes (positive integer).
     * 
     * @exception IllegalArgumentException
     *                if nbytes is non-positive.
     * @exception IllegalArgumentException
     *                if nbytes is smaller than the current value.
     */
    public void setByteCount(int nbytes) {

        if (nbytes <= 0)
            throw new IllegalArgumentException("nbytes=" + nbytes
                    + " is non-positive.");

        if( nbytes < this.nbytes ) {
            
            throw new IllegalArgumentException("Can not shrink the #of bytes.");
            
        }
        
        this.nbytes = nbytes;
        
    }

    /**
     * @exception UnsupportedOperationException
     *                unless the allocation has been closed.
     */
    public int capacity() {
        
        if( ! closed ) {
        
            throw new UnsupportedOperationException();
            
        }
        
        return slots.size();
        
    }
    
    public void add(int slot) {

        if( closed ) throw new IllegalStateException();
        
        if( slot < 0 ) throw new IllegalArgumentException();
        
        slots.add( slot );
        
    }

    public void close() {

        if( closed ) throw new IllegalStateException();

        closed = true;
        
    }
    
    public boolean isClosed() {
        
        return closed;
        
    }
    
    public int firstSlot() {
        
        itr = slots.iterator();
        
        return itr.next();
        
    }

    public int nextSlot() {
        
        if( itr == null ) throw new IllegalStateException();
        
        if( itr.hasNext() ) {
        
            return itr.next();
            
        } else {
            
            itr = null;
            
            return -1;
            
        }
        
    }

    public int getSlotCount() {
        
        return slots.size();
        
    }

}
