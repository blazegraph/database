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

            int off = buf.arrayOffset();

            int len = buf.remaining();

            Object ret = deserialize(buf.array(), off, len);

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
