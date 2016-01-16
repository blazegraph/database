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

package com.bigdata.rdf.lexicon;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.LongPacker;
import com.bigdata.io.ShortPacker;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.search.FullTextIndexTupleSerializer;
import com.bigdata.search.ITermDocKey;
import com.bigdata.search.ITermDocRecord;
import com.bigdata.search.ITermDocVal;
import com.bigdata.search.ReadOnlyTermDocKey;
import com.bigdata.search.ReadOnlyTermDocRecord;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

/**
 * Replaces the {@link FullTextIndexTupleSerializer} to support {@link IV}s as
 * document identifiers.
 * <p>
 * Since {@link IV}s have a variable length encoding we have to indicate the
 * length of the {@link IV} either in the key or the value of the {@link ITuple}
 * . I've put this information into the value side of the tuple in order to keep
 * the key format simpler.
 * <p>
 * Note: The RDF database does not make use of the "field" concept in the keys
 * of the full text index. The fieldId will always be reported as -1.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFFullTextIndexTupleSerializer extends
        DefaultTupleSerializer<ITermDocKey, ITermDocVal> {

    final private static transient Logger log = Logger
            .getLogger(RDFFullTextIndexTupleSerializer.class);

//    private boolean doublePrecision;
    
    static private final transient int NO_FIELD = -1;

//    public boolean isDoublePrecision() {
//        return doublePrecision;
//    }

    /**
     * Used to serialize the values for the tuples in the index.
     * <p>
     * Note: While this object is not thread-safe, the mutable B+Tree is
     * restricted to a single writer so it does not have to be thread-safe.
     */
    final transient private DataOutputBuffer buf = new DataOutputBuffer(24);

    /**
     * De-serialization constructor.
     */
    public RDFFullTextIndexTupleSerializer() {
    }

    /**
     * @param keyBuilderFactory
     *            This factory governs the Unicode collation order that will be
     *            imposed on the indexed tokens.
     * @param leafKeysCoder
     *            The coder used for the leaf keys (prefix coding is fine).
     * @param leafValsCoder
     *            The coder used for the leaf values (custom coding may provide
     *            tighter representations of the {@link ITermDocVal} s in the
     *            index entries).
     * @param fieldsEnabled
     *            When <code>true</code> the <code>fieldId</code> will be
     *            included as a component in the generated key. When
     *            <code>false</code> it will not be present in the generated
     *            key.
     */
    public RDFFullTextIndexTupleSerializer(//
            final IKeyBuilderFactory keyBuilderFactory,//
            final IRabaCoder leafKeysCoder, //
            final IRabaCoder leafValsCoder,//
            final boolean fieldsEnabled//
//            final boolean doublePrecision//
    ) {

        super(keyBuilderFactory, leafKeysCoder, leafValsCoder);

//        this.doublePrecision = doublePrecision;

    }

    @Override
    public byte[] serializeKey(final Object obj) {

        final ITermDocKey entry = (ITermDocKey) obj;

        final String termText = entry.getToken();
        
        final double termWeight = entry.getLocalTermWeight();
        
        /*
         * See: http://lucene.apache.org/core/old_versioned_docs/versions/3_0_2/api/all/org/apache/lucene/search/Similarity.html
         * 
         * For more information on the round-trip of normalized term weight.
         */
        final byte termWeightCompact =
        	org.apache.lucene.search.Similarity.encodeNorm((float) termWeight);
        
        final IV docId = (IV)entry.getDocId();

        final IKeyBuilder keyBuilder = getKeyBuilder();

        keyBuilder.reset();

        // the token text (or its successor as desired).
        keyBuilder
                .appendText(termText, true/* unicode */, false/* successor */);

        keyBuilder.append(termWeightCompact);

        IVUtility.encode(keyBuilder, docId);

        final byte[] key = keyBuilder.getKey();

        if (log.isDebugEnabled()) {

            log.debug("{" + termText + "," + docId + "}, key="
                    + BytesUtil.toString(key));

        }

        return key;

    }

    @Override
    public byte[] serializeVal(final ITermDocVal obj) {

        final ITermDocVal val = (ITermDocVal) obj;

        if (log.isDebugEnabled()) {
            log.debug(val);
        }

        buf.reset();

//        final int termFreq = val.termFreq();
//
//        final double localTermWeight = val.getLocalTermWeight();

        final int byteLen = 
        	((IV) ((ITermDocRecord) obj).getDocId()).byteLength();
        
        if (byteLen > Short.MAX_VALUE) {
        	
        	throw new IllegalArgumentException("cannot serialize IVs longer than Short.MAX_VALUE");
        	
        }
        
        // The byte length of the document identifier IV.
        buf.packShort((short) byteLen);
        
        // The term frequency
//        buf.packLong(termFreq);
//        buf.putShort(termFreq > Short.MAX_VALUE ? Short.MAX_VALUE
//                : (short) termFreq);
//
//        // The term weight
//        if (doublePrecision)
//            buf.putDouble(localTermWeight);
//        else
//            buf.putFloat((float) localTermWeight);

        return buf.toByteArray();

    }

    @Override
    public ITermDocKey deserializeKey(final ITuple tuple) {

        return deserialize(tuple, true/* keyOnly */);

    }

    @Override
    public ITermDocRecord deserialize(final ITuple tuple) {

        return (ITermDocRecord) deserialize(tuple, false/* keyOnly */);

    }

    protected ITermDocKey deserialize(final ITuple tuple, final boolean keyOnly) {

        final ByteArrayBuffer kbuf = tuple.getKeyBuffer();

        // The byte length of the docId IV.
        final int byteLength;
        try {
//            byteLength = LongPacker.unpackInt((DataInput) tuple
//                    .getValueStream());
            byteLength = ShortPacker.unpackShort((DataInput) tuple
            		.getValueStream());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        
        final int docIdOffset = kbuf.limit() - byteLength;

        // Decode the IV.
        final IV docId = (IV) IVUtility.decodeFromOffset(kbuf.array(),
                docIdOffset);

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

            return new ReadOnlyTermDocKey(docId, NO_FIELD, termWeight);

        }

//        final int termFreq;
//        final double termWeight;
//        try {
//
//            final DataInputBuffer dis = tuple.getValueStream();
//
//            // skip the byte length of the IV.
//            LongPacker.unpackInt((DataInput) dis);
//            
//            termFreq = dis.readShort();
//            termFreq = LongPacker.unpackInt((DataInput) dis);

//            if (doublePrecision)
//                termWeight = dis.readDouble();
//            else
//                termWeight = dis.readFloat();
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }

        return new ReadOnlyTermDocRecord(null/* token */, docId, NO_FIELD,
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
//        this.doublePrecision = in.readBoolean();

    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeByte(VERSION);
//        out.writeBoolean(doublePrecision);
    }

}
