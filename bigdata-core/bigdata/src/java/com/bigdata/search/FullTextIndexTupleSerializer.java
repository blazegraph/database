/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 13, 2011
 */

package com.bigdata.search;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * Class manages the encoding and decoding of keys for the full text index. You
 * can override this class to change the way in which the keys and/or values of
 * the index are stored. For example, the RDF database does this to use variable
 * length document identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FullTextIndexTupleSerializer.java 4702 2011-06-13 16:25:38Z
 *          thompsonbry $
 */
public class FullTextIndexTupleSerializer<V extends Comparable<V>> extends
        DefaultTupleSerializer<ITermDocKey<V>, ITermDocVal> {

    final private static transient Logger log = Logger
            .getLogger(FullTextIndexTupleSerializer.class);

    private boolean fieldsEnabled;
//    private boolean doublePrecision;
    
    public boolean isFieldsEnabled() {
        return fieldsEnabled;
    }

//    public boolean isDoublePrecision() {
//        return doublePrecision;
//    }
    
//    /**
//     * Used to serialize the values for the tuples in the index.
//     * <p>
//     * Note: While this object is not thread-safe, the mutable B+Tree is
//     * restricted to a single writer so it does not have to be thread-safe.
//     */
//    final transient private DataOutputBuffer buf = new DataOutputBuffer(24);

    /**
     * De-serialization constructor.
     */
    public FullTextIndexTupleSerializer() {
    }

    /**
     * @param keyBuilderFactory
     *            This factory governs the Unicode collation order that will be
     *            imposed on the indexed tokens.
     * @param leafKeysCoder
     *            The coder used for the leaf keys (prefix coding is fine).
     * @param leafValsCoder
     *            The coder used for the leaf values (custom coding may provide
     *            tighter representations of the {@link ITermDocVal}s in the
     *            index entries).
     * @param fieldsEnabled
     *            When <code>true</code> the <code>fieldId</code> will be
     *            included as a component in the generated key. When
     *            <code>false</code> it will not be present in the generated
     *            key.
     */
    public FullTextIndexTupleSerializer(//
            final IKeyBuilderFactory keyBuilderFactory,//
            final IRabaCoder leafKeysCoder, //
            final IRabaCoder leafValsCoder,//
            final boolean fieldsEnabled//
//            final boolean doublePrecision//
            ) {
   
        super(keyBuilderFactory, leafKeysCoder, leafValsCoder);

        this.fieldsEnabled = fieldsEnabled;
//        this.doublePrecision = doublePrecision;
        
    }

    @Override
    public byte[] serializeKey(final Object obj) {

        @SuppressWarnings("unchecked")
        final ITermDocKey<V> entry = (ITermDocKey<V>) obj;

        final String termText = entry.getToken();
        
        final double termWeight = entry.getLocalTermWeight();
        
        /*
         * See: http://lucene.apache.org/core/old_versioned_docs/versions/3_0_2/api/all/org/apache/lucene/search/Similarity.html
         * 
         * For more information on the round-trip of normalized term weight.
         */
        final byte termWeightCompact =
        	org.apache.lucene.search.Similarity.encodeNorm((float) termWeight);
        
        final V docId = entry.getDocId();

        final IKeyBuilder keyBuilder = getKeyBuilder();

        keyBuilder.reset();

        // the token text (or its successor as desired).
        keyBuilder
                .appendText(termText, true/* unicode */, false/* successor */);
        
        keyBuilder.append(termWeightCompact);

        keyBuilder.append((V) docId);

        if (fieldsEnabled)
            keyBuilder.append(entry.getFieldId());

        final byte[] key = keyBuilder.getKey();

        if (log.isDebugEnabled()) {

            log.debug("{" + termText + "," + docId
                    + (fieldsEnabled ? "," + entry.getFieldId() : "")
                    + "}, key=" + BytesUtil.toString(key));

        }

        return key;

    }

    @Override
    public byte[] serializeVal(final ITermDocVal obj) {

    	return null;
    	
//        final ITermDocVal val = (ITermDocVal) obj;
//        
//        if (log.isDebugEnabled()) {
//            log.debug(val);
//        }
//
//        buf.reset();
//
//        final int termFreq = val.termFreq();
//
//        final double localTermWeight = val.getLocalTermWeight();
//
//        // The term frequency
//        buf.putShort(termFreq > Short.MAX_VALUE ? Short.MAX_VALUE
//                : (short) termFreq);
//
//        // The term weight
//        if (doublePrecision)
//            buf.putDouble(localTermWeight);
//        else
//            buf.putFloat((float) localTermWeight);
//
//        return buf.toByteArray();

    }

    @Override
    public ITermDocKey<V> deserializeKey(final ITuple tuple) {

        return deserialize(tuple, true/* keyOnly */);
        
    }

    @Override
    public ITermDocRecord<V> deserialize(final ITuple tuple) {
        
        return (ITermDocRecord<V>) deserialize(tuple, false/* keyOnly */);

    }
    
    protected ITermDocKey<V> deserialize(final ITuple tuple,
            final boolean keyOnly) {
    
        // key is {term,docId,fieldId}
        // final byte[] key = tuple.getKey();
        //      
        // // decode the document identifier.
        // final long docId = KeyBuilder.decodeLong(key, key.length
        // - Bytes.SIZEOF_LONG /*docId*/ - Bytes.SIZEOF_INT/*fieldId*/);

        final ByteArrayBuffer kbuf = tuple.getKeyBuffer();

        /*
         * The byte offset of the docId in the key.
         * 
         * Note: This is also the byte length of the match on the unicode sort
         * key, which appears at the head of the key.
         */
        final int docIdOffset = kbuf.limit() - Bytes.SIZEOF_LONG /* docId */
                - (fieldsEnabled ? Bytes.SIZEOF_INT/* fieldId */: 0);

        final V docId = (V) (Object)Long.valueOf(KeyBuilder.decodeLong(kbuf.array(),
                docIdOffset));

        // Decode field when present
        final int fieldId;
        if (fieldsEnabled) {
            fieldId = KeyBuilder.decodeShort(kbuf.array(), kbuf.limit()
                    - Bytes.SIZEOF_INT);
        } else {
            fieldId = -1;
        }
        
        final int termWeightOffset = docIdOffset - Bytes.SIZEOF_BYTE;
        
        final byte termWeightCompact = kbuf.getByte(termWeightOffset);
        
        /*
         * See: http://lucene.apache.org/core/old_versioned_docs/versions/3_0_2/api/all/org/apache/lucene/search/Similarity.html
         * 
         * For more information on the round-trip of normalized term weight.
         */
        final double termWeight = 
        	org.apache.lucene.search.Similarity.decodeNorm(termWeightCompact);

        if (keyOnly) {

            return new ReadOnlyTermDocKey(docId, fieldId, termWeight);
            
        }
        
//        final int termFreq;
//        final double termWeight;
//        try {
//
//            final DataInputBuffer dis = tuple.getValueStream();
//
//            termFreq = dis.readShort();
//
//            if(doublePrecision)
//                termWeight = dis.readDouble();
//            else
//                termWeight = dis.readFloat();
//            
//        } catch (IOException ex) {
//            
//            throw new RuntimeException(ex);
//
//        }
//
        return new ReadOnlyTermDocRecord<V>(null/* token */, docId, fieldId,
                /* termFreq, */ termWeight);

    }

    /**
     * The initial version.
     */
    private static final transient byte VERSION0 = 0;

    private static final transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        final byte version = in.readByte();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException("unknown version=" + version);
        }
        this.fieldsEnabled = in.readBoolean();
//        this.doublePrecision = in.readBoolean();

    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeByte(VERSION);
        out.writeBoolean(fieldsEnabled);
//        out.writeBoolean(doublePrecision);
    }

}
