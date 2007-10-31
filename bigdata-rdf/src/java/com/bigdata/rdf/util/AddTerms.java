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
 * Created on May 21, 2007
 */
package com.bigdata.rdf.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.IProcedure;

/**
 * This unisolated operation inserts terms into the terms index, assigning
 * identifiers to terms as a side-effect. The use of this operation MUST be
 * followed by the the use of {@link AddIds} to ensure that the reverse
 * mapping from id to term is defined before any statements are inserted
 * using the assigned term identifiers. The client MUST NOT make assertions
 * using the assigned term identifiers until the corresponding
 * {@link AddIds} operation has suceeded.
 * <p>
 * In order for the lexicon to remain consistent if the client fails for any
 * reason after the forward mapping has been made restart-safe and before
 * the reverse mapping has been made restart-safe clients MUST always use a
 * successful {@link AddTerms} followed by a successful {@link AddIds}
 * before inserting statements using term identifiers into the statement
 * indices. In particular, a client MUST NOT treat lookup against the terms
 * index as satisifactory evidence that the term also exists in the reverse
 * mapping.
 * <p>
 * Note that it is perfectly possible that a concurrent client will overlap
 * in the terms being inserted. The results will always be fully consistent
 * if the rules of the road are observed since (a) unisolated operations are
 * single-threaded; (b) term identifiers are assigned in an unisolated
 * atomic operation by {@link AddTerms}; and (c) the reverse mapping is
 * made consistent with the assignments made/discovered by the forward
 * mapping.
 * <p>
 * Note: The {@link AddTerms} and {@link AddIds} operations may be analyzed
 * as a batch and variant of the following pseudocode.
 * 
 * <pre>
 *  
 *  for each term:
 *  
 *  termId = null;
 *  
 *  synchronized (ndx) {
 *    
 *    counter = ndx.getCounter();
 *  
 *    termId = ndx.lookup(term.key);
 *    
 *    if(termId == null) {
 * 
 *       termId = counter.inc();
 *       
 *       ndx.insert(term.key,termId);
 *       
 *       }
 *  
 *  }
 *  
 * </pre>
 * 
 * In addition, the actual operations against scale-out indices are
 * performed on index partitions rather than on the whole index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AddTerms implements IProcedure, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1679137119603652663L;
    
    private IKeyBuffer keys;

    /**
     * De-serialization constructor.
     */
    public AddTerms() {
        
    }
    
    public AddTerms(IKeyBuffer keys) {

        assert keys != null;

        this.keys = keys;
        
    }
    
    /**
     * For each term whose serialized key is mapped to the current index
     * partition, lookup the term in the <em>terms</em> index. If it is
     * there then note its assigned termId. Otherwise, use the partition
     * local counter to assign the term identifier, note the term identifer
     * so that it can be communicated back to the client, and insert the
     * {term,termId} entry into the <em>terms</em> index.
     * 
     * @param ndx
     *            The terms index.
     * 
     * @return The {@link Result}, which contains the discovered / assigned
     *         term identifiers.
     */
    public Object apply(IIndexWithCounter ndx) throws Exception {

        /*
         * When true, we serialize the values as byte[]s.  When false, the
         * btree has a serializer that will accept Long values.
         */
        final boolean isUnisolatedBTree = ndx instanceof UnisolatedBTree;
        
        final int numTerms = keys.getKeyCount();
        
        // used to store the discovered / assigned term identifiers.
        final long[] ids = new long[numTerms];
        
        // used to assign term identifiers.
        final ICounter counter = ndx.getCounter();
        
        if (counter.get() == IRawTripleStore.NULL) {
            /*
             * Note: we never assign this value as a term identifier.
             */
            counter.inc();
        }
        
        // used to serialize term identifers.
        final DataOutputBuffer idbuf = (isUnisolatedBTree ?new DataOutputBuffer(
                Bytes.SIZEOF_LONG) : null);
        
        for (int i = 0; i < numTerms; i++) {

            final byte[] term = keys.getKey(i);
            
            final long termId;

            // Lookup in the forward index.
            Object tmp = ndx.lookup(term);

            if (tmp == null) { // not found.

                // assign termId.
                termId = counter.inc();

                // format as packed long integer.
                if(isUnisolatedBTree) idbuf.reset().packLong(termId);

                // insert into index.
                if (ndx.insert(term, (isUnisolatedBTree?idbuf.toByteArray():Long.valueOf(termId))) != null) {

                    throw new AssertionError();

                }

            } else { // found.

                if(isUnisolatedBTree) {
                                    
                    termId = new DataInputBuffer((byte[])tmp).unpackLong();
                   
                } else {
                
                    termId = (Long)tmp;

                }

            }

            ids[i] = termId;

        }

        return new Result(ids);

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        /*
         * Read the entire input stream into a buffer.
         */
        DataOutputBuffer buf = new DataOutputBuffer(in);
        
        /*
         * Unpack the buffer.
         */
        DataInputBuffer is = new DataInputBuffer(buf.buf,0,buf.len);
        
        keys = KeyBufferSerializer.INSTANCE.getKeys(is);
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        /*
         * Setup a buffer for serialization.
         */
        DataOutputBuffer buf = new DataOutputBuffer();
        
        /*
         * Serialize the keys onto the buffer.
         */
        KeyBufferSerializer.INSTANCE.putKeys(buf, keys);
        
        /*
         * Copy the serialized form onto the caller's output stream.
         */
        out.write(buf.buf,0,buf.len);
                    
    }

    /**
     * Object encapsulates the discovered / assigned term identifiers and
     * provides efficient serialization for communication of those data to
     * the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class Result implements Externalizable {

        public long[] ids;
        
        /**
         * 
         */
        private static final long serialVersionUID = -8307927320589290348L;

        /**
         * De-serialization constructor.
         */
        public Result() {
            
        }
        
        public Result(long[] ids) {
            
            assert ids != null;
            
            assert ids.length > 0;
            
            this.ids = ids;
            
        }

        private final static transient short VERSION0 = 0x0;
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            final short version = ShortPacker.unpackShort(in);
            
            if(version!=VERSION0) {
                
                throw new IOException("Unknown version: "+version);
                
            }
            
            final int n = (int) LongPacker.unpackLong(in);
            
            ids = new long[n];
            
            for(int i=0; i<n; i++) {
                
                ids[i] = LongPacker.unpackLong(in);
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            final int n = ids.length;
            
            ShortPacker.packShort(out, VERSION0);
            
            LongPacker.packLong(out,n);

            for(int i=0; i<n; i++) {
                
                LongPacker.packLong(out, ids[i]);
                
            }
            
        }
        
    }
    
}
