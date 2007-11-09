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
 * Created on May 21, 2007
 */
package com.bigdata.rdf.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IRawTripleStore;
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
