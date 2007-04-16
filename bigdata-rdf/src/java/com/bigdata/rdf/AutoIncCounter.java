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
package com.bigdata.rdf;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.btree.UserDefinedFunction;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.ICommitter;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.rio.BulkRioLoader;

/**
 * Auto-increment counter.
 * 
 * @todo the {@link BulkRioLoader} also uses this counter and that use needs to
 *       be reconciled for consistency if concurrent writers are allowed.
 * 
 * @todo redefine NULL to an unsigned long zero and start assigning identifiers
 *       from an unsigned long one?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AutoIncCounter implements UserDefinedFunction, ICommitter {

    /**
     * 
     */
    private static final long serialVersionUID = -4281749674236461781L;

    /**
     * The next identifier to be assigned to a string inserted into this
     * index.
     */
    private long nextId;

    /**
     * The last value assigned by the counter.
     */
    private Object retval;

    private final IRawStore store;
    
    /**
     * The last address where this counter was written on the store and
     * 0L if it has never been writte.
     */
    private long lastAddr = 0L;

    private boolean dirty = false;
    
    public AutoIncCounter(IRawStore store,long nextId) {

        assert store != null;
        
        assert nextId > 0;
        
        this.nextId = nextId;
        
        this.store = store;
        
    }
    
    /**
     * The next identifier that would be assigned (does not increment the
     * counter).
     */
    public long getCounter() {
        
        return nextId;
        
    }
    
    /**
     * Assigns a new identifier and marks this object as dirty.
     */
    public long nextId() {

        dirty = true;
        
        return nextId++;

    }
    
    /**
     * If the key is found then we do not update the value.
     */
    public Object found(byte[] key, Object val) {

        this.retval = val;
        
        return val;
        
    }

    /**
     * If the key is not found then we insert the current value of the
     * counter and increment the counter.
     */
    public Object notFound(byte[] key) {
        
        retval = Long.valueOf(nextId());
        
        return retval;
        
    }
    
    public Object returnValue(byte[] key,Object oldval) {
        
        return retval;
        
    }

    public byte[] serialize() {

        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeLong(this.nextId);

            dos.flush();

            return baos.toByteArray();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public static AutoIncCounter read(IRawStore store, long addr) {

        try {
    
            ByteBuffer buf = store.read(addr);

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);

            DataInputStream dis = new DataInputStream(bbis);

            final long nextId = dis.readLong();

            AutoIncCounter counter = new AutoIncCounter(store, nextId);
            
            counter.lastAddr = addr;
            
            return counter;

        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }
    
    public long handleCommit() {

        if (dirty || lastAddr == 0L) {

            lastAddr = store.write(ByteBuffer.wrap(serialize()));

            dirty = false;

        }

        return lastAddr;

    }

}
