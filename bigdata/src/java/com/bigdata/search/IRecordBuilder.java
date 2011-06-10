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

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.io.ByteArrayBuffer;

/**
 * Interface for encoding and decoding the keys and values of the full text
 * index.
 * 
 * @param <V>
 *            The generic type of the document identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME This could be further abstracted as an
 *          {@link ITupleSerializer} if we only defined an object for the key of
 *          the tuple. We already have an object for the value - it is the
 *          {@link ITermMetadata}. We could easily create an
 *          IDocumentTokenMetadata to capture the other hand of the information
 *          (the string value of the token, the document identifier, and the
 *          optional field identifier).
 */
public interface IRecordBuilder<V extends Comparable<V>> {

    /**
     * Create a key for a term.
     * 
     * @param keyBuilder
     *            Used to construct the key.
     * @param token
     *            The token whose key will be formed.
     * @param successor
     *            When <code>true</code> the successor of the token's text will
     *            be encoded into the key. This is useful when forming the
     *            <i>toKey</i> in a search.
     * @param docId
     *            The document identifier - use {@link Long#MIN_VALUE} when
     *            forming a search key.
     * @param fieldId
     *            The field identifier - use {@link Integer#MIN_VALUE} when
     *            forming a search key.
     * 
     * @return The key.
     */
    byte[] getKey(IKeyBuilder keyBuilder, String termText, boolean successor,
            V docId, int fieldId);

    /**
     * Generate the key for the inclusive lower bound of a scan of the full text
     * index searching for the specified termText (or a prefix thereof).
     * 
     * @param keyBuilder
     * @param termText
     * @return
     */
    byte[] getFromKey(IKeyBuilder keyBuilder, String termText);

    /**
     * Generate the key for the exclusive upper bound of a scan of the full text
     * index searching for the specified termText (or a prefix thereof).
     * 
     * @param keyBuilder
     * @param termText
     * @return
     */
    byte[] getToKey(IKeyBuilder keyBuilder, String termText);
    
    /**
     * Return the byte[] that is the encoded value for per-{token,docId,fieldId}
     * entry in the index.
     * 
     * @param buf
     *            The buffer to be used (the buffer will be reset
     *            automatically).
     * @param metadata
     *            Metadata about the term.
     * 
     * @return The encoded value.
     */
    byte[] getValue(ByteArrayBuffer buf, ITermMetadata metadata);

    /**
     * Decode the document identifier from the tuple.
     * 
     * @param tuple
     *            The tuple.
     * 
     * @return The document identifier.
     */
    V getDocId(ITuple<?> tuple);

    /**
     * Decode the value associated with the tuple.
     * 
     * @param tuple
     *            The tuple.
     *            
     * @return The decoded value.
     */
    ITermMetadata decodeValue(ITuple<?> tuple);

}
