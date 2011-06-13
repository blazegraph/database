/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 10, 2011
 */

package com.bigdata.search;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderExtension;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rawstore.Bytes;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultRecordBuilder<V extends Comparable<V>> implements
        IRecordBuilder<V> {

    final private static transient Logger log = Logger
            .getLogger(DefaultRecordBuilder.class);

    private final boolean fieldsEnabled;
    private final boolean doublePrecision;
    private final IKeyBuilderExtension<V> docIdFactory;

    /**
     * @param fieldsEnabled
     *            When <code>true</code> the <code>fieldId</code> will be
     *            included as a component in the generated key. When
     *            <code>false</code> it will not be present in the generated
     *            key.
     * @param doublePrecision
     *            When <code>true</code>, the term weight will be serialized
     *            using double precision.
     */
    public DefaultRecordBuilder(final boolean fieldsEnabled,
            final boolean doublePrecision,
            final IKeyBuilderExtension<V> docIdFactory) {

        if (docIdFactory == null)
            throw new IllegalArgumentException();

        this.fieldsEnabled = fieldsEnabled;

        this.doublePrecision = doublePrecision;
        
        this.docIdFactory = docIdFactory;

    }

    public byte[] getKey(final IKeyBuilder keyBuilder, final String termText,
            final boolean successor, final V docId, final int fieldId) {

        keyBuilder.reset();

        // the token text (or its successor as desired).
        keyBuilder.appendText(termText, true/* unicode */, successor);

        docIdFactory.encode(keyBuilder, docId);

        if (fieldsEnabled)
            keyBuilder.append(fieldId);

        final byte[] key = keyBuilder.getKey();

        if (log.isDebugEnabled()) {

            log.debug("{" + termText + "," + docId
                    + (fieldsEnabled ? "," + fieldId : "") + "}, successor="
                    + (successor ? "true " : "false") + ", key="
                    + BytesUtil.toString(key));

        }

        return key;

    }

    /**
     * {@inheritDoc}
     * 
     * TODO optionally record the token position metadata (sequence of token
     *       positions in the source) and the token offset (character offsets
     *       for the inclusive start and exclusive end of each token).
     * 
     * TODO value compression: code "position" as delta from last position in
     *       the same field or from 0 if the first token of a new document; code
     *       "offsets" as deltas - the maximum offset between tokens will be
     *       quite small (it depends on the #of stopwords) so use a nibble
     *       format for this.
     */
    public byte[] getValue(final ByteArrayBuffer buf,
            final ITermMetadata metadata) {

        if (log.isDebugEnabled()) {
            log.debug(metadata);
        }

        buf.reset();

        final int termFreq = metadata.termFreq();

        final double localTermWeight = metadata.getLocalTermWeight();

        // The term frequency
        buf.putShort(termFreq > Short.MAX_VALUE ? Short.MAX_VALUE
                : (short) termFreq);

        // The term weight
        if (doublePrecision)
            buf.putDouble(localTermWeight);
        else
            buf.putFloat((float) localTermWeight);

        return buf.toByteArray();

    }

    public V getDocId(final ITuple<?> tuple) {

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

        return docIdFactory.decode(kbuf.array(), docIdOffset);

    }

    public ITermMetadata decodeValue(final ITuple<?> tuple) {

        try {
            final DataInputBuffer dis = tuple.getValueStream();

            final int termFreq = dis.readShort();

            final double termWeight;
            if(doublePrecision)
                termWeight = dis.readDouble();
            else
                termWeight = dis.readFloat();
            
            return new ReadOnlyTermMetadata(termFreq, termWeight);

        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    private static class ReadOnlyTermMetadata implements ITermMetadata {

        private final int termFreq;
        private final double termWeight;
        
        /**
         * @param termFreq
         * @param termWeight
         */
        public ReadOnlyTermMetadata(final int termFreq, final double termWeight) {
            this.termFreq = termFreq;
            this.termWeight = termWeight;
        }

        public void add() {
            throw new UnsupportedOperationException();
        }

        public double getLocalTermWeight() {
            return termWeight;
        }

        public void setLocalTermWeight(double d) {
            throw new UnsupportedOperationException();
        }

        public int termFreq() {
            return termFreq;
        }
        
    }
    
}
