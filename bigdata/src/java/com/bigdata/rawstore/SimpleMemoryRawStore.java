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
 * Created on Jan 31, 2007
 */

package com.bigdata.rawstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.journal.TemporaryStore;

/**
 * A purely transient append-only implementation useful when data need to be
 * buffered in memory. The writes are stored in an {@link ArrayList}.
 * 
 * @see {@link TemporaryStore}, which provides a more scalable solution for temporary
 *      data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleMemoryRawStore implements IRawStore {

    private boolean open = true;
    
    /**
     * The #of bytes written so far. This is used to generate the
     * {@link Addr address} values returned by {@link #write(ByteBuffer)}. This
     * is necessary in order for this implementation to assign addresses in the
     * same manner as they would be assigned by an implementation using an
     * append only byte[] or file.
     */
    protected int nextOffset = 0;
    
    /**
     * Maps an {@link Addr} onto the index in {@link #records} at which the
     * data for that address was written.
     */
    private final Map<Long,Integer> addrs;
    
    /**
     * The buffered records in the order written. If a record is deleted then
     * that element in the list will be a [null] value.
     */
    protected final ArrayList<byte[]> records;
    
    /**
     * Uses an initial capacity of 1000 records.
     */
    public SimpleMemoryRawStore() {

        this(1000);
        
    }
    
    /**
     * 
     * @param capacity
     *            The #of records that you expect to store (non-negative). If
     *            the capacity is exceeded then the internal {@link ArrayList}
     *            will be grown as necessary.
     */
    public SimpleMemoryRawStore(int capacity) {
        
        if (capacity < 0)
            throw new IllegalArgumentException("capacity is negative");
        
        records = new ArrayList<byte[]>(capacity);
        
        // estimate hash table capacity to avoid resizing.
        addrs = new HashMap<Long,Integer>((int)(capacity*1.25));

    }
    
    public boolean isOpen() {

        return open;
        
    }

    public boolean isStable() {
        
        return false;
        
    }

    public void close() {
        
        if( !open ) throw new IllegalStateException();

        open = false;
        
        // discard all the records.
        records.clear();
        
    }

    public ByteBuffer read(long addr, ByteBuffer dst) {

        if (addr == 0L)
            throw new IllegalArgumentException("Address is 0L");
        
//        final int offset = Addr.getOffset(addr);
        
        final int nbytes = Addr.getByteCount(addr);

        if(nbytes==0) {
            
            throw new IllegalArgumentException(
                    "Address encodes record length of zero");
            
        }
        
        Integer index = addrs.get(addr);
        
        if(index==null) {
            
            throw new IllegalArgumentException("Address never written.");

        }

        final byte[] b = records.get(index);

        if(b == null) {
            
            throw new IllegalArgumentException("Record was deleted");
            
        }
        
        if(b.length != nbytes) {
            
            throw new RuntimeException("Bad address / data");
            
        }
        
        if(dst != null && dst.remaining()>=nbytes) {

            // place limit on the #of valid bytes in dst.
            
            dst.limit(dst.position() + nbytes);
            
            // copy into the caller's buffer.
            
            dst.put(b,0,b.length);
            
            // flip for reading.
            
            dst.flip();
            
            // the caller's buffer.
            
            return dst;
            
        } else {

            // return a read-only view onto the data in the store.
            
            return ByteBuffer.wrap(b).asReadOnlyBuffer();
            
        }
        
    }

    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException("No bytes remaining in buffer");

        // allocate a new array that is an exact fit for the data.
        final byte[] b = new byte[nbytes];

        // copy the data into the array.
        data.get(b);
        
        // the next offset.
        final int offset = nextOffset;
        
        // increment by the #of bytes written.
        nextOffset += nbytes;

        // the position in the records[] where this record is stored.
        final int index = records.size();
        
        // add the record to the records array.
        records.add(b);
        
        // formulate the address that can be used to recover that record.
        long addr = Addr.toLong(nbytes, offset);
        
        // save the mapping from the addr to the records[].
        addrs.put(addr,index);
        
        return addr;
        
    }

//    public void delete(long addr) {
//
//        if(addr==0L) throw new IllegalArgumentException("Address is 0L");
//        
////        final int offset = Addr.getOffset(addr);
//        
//        final int nbytes = Addr.getByteCount(addr);
//
//        if(nbytes==0) {
//            
//            throw new IllegalArgumentException(
//                    "Address encodes record length of zero");
//            
//        }
//        
//        Integer index = addrs.get(addr);
//        
//        if(index==null) {
//         
//            throw new IllegalArgumentException("Address never written.");
//
//        }
//        
//        byte[] b = records.get(index);
//
//        if(b == null) {
//            
//            throw new IllegalArgumentException("Record was deleted");
//            
//        }
//
//        if(b.length != nbytes) {
//            
//            throw new RuntimeException("Bad address / data");
//            
//        }
//        
//        // release that record.
//        records.set(index, null);
//        
//    }

    public void force(boolean metadata) {
        
        // NOP.
        
    }
    
}
