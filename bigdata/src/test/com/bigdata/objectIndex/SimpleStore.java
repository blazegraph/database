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
 * Created on Nov 17, 2006
 */
package com.bigdata.objectIndex;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;

/**
 * A simple transient implementation of {@link IRawStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleStore implements IRawStore {

    /**
     * Used to assign slots.
     */
    private int nextSlot = 1;

    /**
     * "Persistence store" - access to objects by the key.
     */
    private final Map<Long, byte[]> store = new HashMap<Long, byte[]>();

    private final int slotSize = 64;
    
    public SlotMath getSlotMath() {
    
        return slotMath;
        
    }

    private final SlotMath slotMath = new SlotMath(slotSize);
    
    public ByteBuffer read(ISlotAllocation slots,ByteBuffer buf) {

        final long key = slots.toLong();
        
        final byte[] bytes = store.get(key);

        if (bytes == null)
            throw new IllegalArgumentException("Not found: key=" + key);

        if( buf != null && buf.remaining() > bytes.length ) {
            
            buf.put(bytes);
         
            return buf;
            
        } else {
        
            return ByteBuffer.wrap(bytes);
            
        }
        
    }
    
    public ISlotAllocation write(ByteBuffer buf) {

        assert buf != null;

        int firstSlot = nextSlot++;

        final byte[] bytes;

        if( buf.hasArray()) {
            
            bytes = buf.array();
            
        } else {
            
            bytes = new byte[buf.remaining()];
            
            buf.get(bytes);
            
        }
        
        int nbytes = bytes.length;
        
        int slotCount = slotMath.getSlotCount(nbytes);
        
        ISlotAllocation slots = new ContiguousSlotAllocation(nbytes, slotCount,
                firstSlot);
        
        store.put(slots.toLong(), bytes);

        return slots;
        
    }

    public void delete(ISlotAllocation key) {

        store.remove(key.toLong());

    }

}
