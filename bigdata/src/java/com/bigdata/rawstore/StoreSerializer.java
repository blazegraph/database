/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.bigdata.btree.BTree;
import com.bigdata.io.ByteBufferInputStream;

/**
 * Serialization support for an {@link IRawStore}.
 * 
 * @todo Consider integration with the extensible serializer instead. The
 *       problem becomes where to persist the state of the serializer and the
 *       concurrency control for that state.
 *       <p>
 *       Note that the {@link IStoreSerializer} is used primarily for
 *       {@link BTree} records and associated things such as metadata records
 *       for btrees while applications are responsible for handling the
 *       serialization of their data objects. Concurrency control may be the
 *       biggest hurdle, but also objects must be deserialized in order to be
 *       transferred between index partitions if serialization state is
 *       per-partition.
 * 
 * @todo test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StoreSerializer implements IStoreSerializer {

    private final IRawStore store;

    public IRawStore getStore() {

        return store;

    }

    public StoreSerializer(IRawStore store) {

        if (store == null)
            throw new IllegalArgumentException();

        this.store = store;

    }

    public static class StoreObjectOutputStream extends ObjectOutputStream implements IStoreObjectOutputStream {

        protected final IRawStore store;
        
        public IRawStore getStore() {

            return store;
            
        }
        
        /**
         * @param store
         * @param arg0
         * @throws IOException
         */
        public StoreObjectOutputStream(IRawStore store,OutputStream arg0) throws IOException {
        
            super(arg0);
            
            if(store==null) throw new IllegalArgumentException();
            
            this.store = store;
            
        }

    }
    
    public static class StoreObjectInputStream extends ObjectInputStream implements IStoreObjectInputStream {

        protected final IRawStore store;

        public IRawStore getStore() {

            return store;
            
        }

        /**
         * @param store
         * @param arg0
         * @throws IOException
         */
        public StoreObjectInputStream(IRawStore store,InputStream arg0) throws IOException {
        
            super(arg0);

            if(store==null) throw new IllegalArgumentException();
            
            this.store = store;

        }
        
    }
    
    final public byte[] serialize(Object obj) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            ObjectOutputStream oos = new StoreObjectOutputStream(store,baos);

            oos.writeObject(obj);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return baos.toByteArray();

    }

    final public Object deserialize(byte[] b) {

        return deserialize(b, 0, b.length);

    }

    final public Object deserialize(byte[] b, int off, int len) {

        ByteArrayInputStream bais = new ByteArrayInputStream(b, off, len);

        try {

            ObjectInputStream ois = new StoreObjectInputStream(store,bais);

            return ois.readObject();

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    final public Object deserialize(ByteBuffer buf) {

        if (buf.hasArray()) {

            final int off = buf.arrayOffset() + buf.position();

            final int len = buf.remaining();

            final Object ret = deserialize(buf.array(), off, len);

            buf.position(buf.limit());

            return ret;

        }

        ByteBufferInputStream bais = new ByteBufferInputStream(buf);

        try {

            ObjectInputStream ois = new StoreObjectInputStream(store,bais);

            return ois.readObject();

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

}
