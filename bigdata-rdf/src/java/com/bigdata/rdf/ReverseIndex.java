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
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.Value;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.BytesUtil;
import com.bigdata.objndx.DefaultEvictionListener;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.PO;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.ibm.icu.text.UnicodeCompressor;
import com.ibm.icu.text.UnicodeDecompressor;

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
            
    /**
     * Serializes the RDF {@link Value} using custom logic.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Optimize ICU UTF compression using the incremental APIs. This will
     *       reduce heap allocation as well during (de-)serialization. Without
     *       optimization, UTF compression is just slightly slower on write.
     * 
     * @todo use a per-leaf dictionary to factor out common strings as codes,
     *       e.g., Hamming codes.
     */
    public static class ValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = 6950535691724083790L;

        public static final int VERSION0 = 0x0;
        
        public static transient final IValueSerializer INSTANCE = new ValueSerializer();
        
        public static final boolean utfCompression = false;
        
        public ValueSerializer() {}
        
        protected void writeUTF(DataOutputStream os,String s) throws IOException {
            
            if (utfCompression) {

                byte[] data = UnicodeCompressor.compress(s);

                LongPacker.packLong(os, data.length);

                os.write(data);

            } else {

                os.writeUTF(s);

            }
            
        }
        
        protected String readUTF(DataInputStream is) throws IOException {
            
            if(utfCompression) {
            
                int len = (int)LongPacker.unpackLong(is);
                
                byte[] data = new byte[len];
                
                is.readFully(data);
                
                return UnicodeDecompressor.decompress(data);
            
            } else {
                
                return is.readUTF();
                
            }
            
        }
        
        public void getValues(DataInputStream is, Object[] vals, int n)
                throws IOException {

            final int version = (int)LongPacker.unpackLong(is);
            
            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);
            
            for (int i = 0; i < n; i++) {
                
                final _Value val;
        
                final byte code = is.readByte();

                final String term = readUTF(is); 
                
                switch(code) {
                case RdfKeyBuilder.CODE_URI:
                    val = new _URI(term);
                    break;
                case RdfKeyBuilder.CODE_LIT:
                    val = new _Literal(term);
                    break;
                case RdfKeyBuilder.CODE_LCL:
                    val = new _Literal(term,readUTF(is));
                    break;
                case RdfKeyBuilder.CODE_DTL:
                    val = new _Literal(term,new _URI(readUTF(is)));
                    break;
                case RdfKeyBuilder.CODE_BND:
                    val = new _BNode(term);
                    break;
                default: throw new AssertionError("Unknown code="+code);
                }

                vals[i] = val;
                
            }
            
        }

        public void putValues(DataOutputStream os, Object[] vals, int n)
                throws IOException {

            LongPacker.packLong(os, VERSION0);
            
            for (int i = 0; i < n; i++) {

                _Value value = (_Value)vals[i];
                
                byte code = value.getTermCode();
                
                os.writeByte(code);
                
                writeUTF(os,value.term);
                
                switch(code) {
                case RdfKeyBuilder.CODE_URI: break;
                case RdfKeyBuilder.CODE_LIT: break;
                case RdfKeyBuilder.CODE_LCL:
                    writeUTF(os,((_Literal)value).language);
                    break;
                case RdfKeyBuilder.CODE_DTL:
                    writeUTF(os,((_Literal)value).datatype.term);
                    break;
                case RdfKeyBuilder.CODE_BND: break;
                default: throw new AssertionError("Unknown code="+code);
                }

            }

        }
        
    }
    
    /**
     * Serializes the RDF {@link Value}s using default Java serialization. The
     * {@link _Value} class heirarchy implements {@link Externalizable} in order
     * to boost performance a little bit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultJavaValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = 2393897553755023082L;
        
        public static transient final IValueSerializer INSTANCE = new DefaultJavaValueSerializer();
        
        public DefaultJavaValueSerializer() {}
        
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
