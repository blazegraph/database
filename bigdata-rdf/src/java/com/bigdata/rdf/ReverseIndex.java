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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.openrdf.model.Value;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.BytesUtil;
import com.bigdata.objndx.DefaultEvictionListener;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.PO;

/**
 * A persistent index for reversing term identifiers to {@link String}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReverseIndex extends BTree {

    /**
     * The next identifier to be assigned to a string inserted into this
     * index.
     * 
     * @todo this needs to be (a) shared across all transactional instances
     *       of this index; (b) restart safe; (c) set into a namespace that
     *       is unique to the journal so that multiple writers on multiple
     *       journals for a single distributed database can not collide;
     *       and (d) set into a namespace that is unique to the 
     */
    protected long nextId = 1;
    
    /**
     * Create a new index.
     * 
     * @param store
     *            The backing store.
     */
    public ReverseIndex(IRawStore store) {
        super(store,
                DEFAULT_BRANCHING_FACTOR,
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        DEFAULT_HARD_REF_QUEUE_CAPACITY,
                        DEFAULT_HARD_REF_QUEUE_SCAN),
                ValueSerializer.INSTANCE,
                null // new RecordCompressor() // record compressor
        );
    }
    
    /**
     * Load an index from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record identifier for the index.
     */
    public ReverseIndex(IRawStore store, long metadataId) {
        super(  store,
                BTreeMetadata.read(BTree
                        .getTransitionalRawStore(store), metadataId),
                new HardReferenceQueue<PO>(
                new DefaultEvictionListener(), DEFAULT_HARD_REF_QUEUE_CAPACITY,
                DEFAULT_HARD_REF_QUEUE_SCAN));
    }

    /**
     * Add a term to the reverse index.
     * 
     * @param idAsKey
     *            The term identifier, which is a long integer converted into an
     *            unsigned byte[] by {@link RdfKeyBuilder#id2key(long)}.
     * @param value
     *            The RDF {@link Value}.
     */
    public void add(byte[] idAsKey, Value value) {

        Value oldValue = (Value)lookup(idAsKey);
        
        if( oldValue == null ) {
            
            super.insert(idAsKey,value);
            
        } else {

            /*
             * Paranoia test will fail if the id has already been assigned to
             * another term. The most likley cause is a failure to keep the
             * segments of the term index in distinct namespaces resulting in an
             * identifier assignment collision.
             */
            if( ! oldValue.equals(value)) {
                
                throw new RuntimeException("insert(id="
                        + BytesUtil.toString(idAsKey) + ", term=" + value
                        + "), but id already assigned to term=" + oldValue);
                
            }
            
        }
        
    }
            
//    /**
//     * Key serializer.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class KeySerializer implements IKeySerializer {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = -3286178987689136643L;
//        static final IKeySerializer INSTANCE = new KeySerializer();
//
//        public ArrayType getKeyType() {
//            
//            return ArrayType.LONG;
//            
//        }
//
//        public void getKeys(DataInputStream is, Object keys, int nkeys) throws IOException {
//
//            long[] a = (long[])keys;
//            
//            for( int i=0; i<nkeys; i++) {
//                
//                a[i] = LongPacker.unpackLong(is);
//                
//            }
//            
//        }
//
//        /**
//         * May overestimate since we pack values.
//         */
//        public int getSize(int n) {
//
//            return Bytes.SIZEOF_LONG * n;
//            
//        }
//
//        public void putKeys(DataOutputStream os, Object keys, int nkeys) throws IOException {
//
//            long[] a = (long[])keys;
//            
//            for( int i=0; i<nkeys; i++) {
//                
//                LongPacker.packLong(os,a[i]);
//                
//            }
//
//        }
//        
//    }

    /**
     * Serializes the RDF {@link Value}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo this is using object input and object output streams which are
     *       extremely fat and slow. this could be improved by implementing the
     *       Externalizable interface by a custom serialization helper or by
     *       using extser (if we have a place to stick the state).
     * 
     * @todo use UTF compression utilities from ICU4J.
     * 
     * @todo use a per-leaf dictionary to factor out common strings as codes,
     *       e.g., Hamming codes.
     */
    public static class ValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = 2393897553755023082L;
        
        public static transient final IValueSerializer INSTANCE = new ValueSerializer();
        
        public ValueSerializer() {}
        
        public void getValues(DataInputStream is, Object[] vals, int n)
                throws IOException {

            Object[] a = (Object[]) vals;
            
            ObjectInputStream ois = new ObjectInputStream(is);
            
            try {

                for (int i = 0; i < n; i++) {

                    a[i] = ois.readObject();
                }
                
            } catch( Exception ex ) {
                
                IOException ex2 = new IOException();
                
                ex2.initCause(ex);
                
                throw ex2;
                
            }
            
        }

        public void putValues(DataOutputStream os, Object[] vals, int n)
                throws IOException {

            if (n == 0)
                return;

            Object[] a = (Object[]) vals;

            ObjectOutputStream oos = new ObjectOutputStream(os);

            for (int i = 0; i < n; i++) {

                oos.writeObject(a[i]);

            }
            
            oos.flush();

        }
        
    }
    
}
