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
package com.bigdata.rdf.internal.impl.literal;

import java.util.Arrays;

import org.openrdf.model.Literal;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.InlineLiteralIV;
import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.BytesUtil;

/**
 * An inline literal IV composed of an array of other inline literal IVs. This
 * IV is meant to be used with LiteralExtensionIV and URIExtensionIV as the
 * delegate for an inline literal extension or an inline URI. This IV is not
 * directly materializable into a Literal outside the context of a literal
 * extension factory or an inline URI handler.
 * <p>
 * Encoded as flags, DTE.ARRAY, array length, array.  The array length is
 * encoded using a single byte, so the maximum array length is 256 elements
 * (empty arrays are not allowed).
 * 
 * @see {@link LiteralExtensionIV}
 * @see {@link URIExtensionIV}
 * @see {@link InlineURIHandler}
 * @see {@link IExtension}
 * 
 * @author mikepersonick
 */
public class LiteralArrayIV extends AbstractLiteralIV<BigdataLiteral, Object[]> {

    private static final long serialVersionUID = 9136542087440805253L;

    /**
     * The inline literal array.
     */
    private final InlineLiteralIV<?,?>[] ivs;
    
    /**
     * Only used for compareTo() and byteLength().  Encoding takes place in
     * AbstractIV, decoding in IVUtility.
     */
    private transient byte[] key;
    
    /**
     * Cached hash code.
     */
    private transient int hashCode = 0;
    
    /**
     * Construct an instance using the supplied inline literal IVs.  The array
     * must not be empty and must not be more than 256 elements (using one
     * byte to encode the array length).
     * 
     * @param ivs
     *          the inline literal IVs
     */
    public LiteralArrayIV(final InlineLiteralIV<?,?>... ivs) {
        super(DTE.Extension);
        
        // only using one byte for the array length
        if (ivs == null || ivs.length == 0 || ivs.length > 256) {
            throw new IllegalArgumentException();
        }
        
        this.ivs = ivs;
    }
    
    @Override 
    public DTEExtension getDTEX() {
        return DTEExtension.ARRAY;
    }
    
    public InlineLiteralIV<?,?>[] getIVs() {
        return ivs;
    }
    
    private byte[] key() {
        if (key == null) {
            key = super.encode(KeyBuilder.newInstance()).getKey();
        }
        return key;
    }
    
    @Override
    public int byteLength() {
        return key().length;
    }

    @Override
    public LiteralArrayIV clone(boolean clearCache) {
        return new LiteralArrayIV(this.ivs);
    }

    @Override
    public int _compareTo(IV o) {
        if (!(o instanceof LiteralArrayIV)) {
            throw new IllegalArgumentException();
        }
        final LiteralArrayIV iv = (LiteralArrayIV) o;
        return BytesUtil.compareBytes(key(), iv.key());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(ivs);
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LiteralArrayIV other = (LiteralArrayIV) obj;
        if (!Arrays.equals(ivs, other.ivs))
            return false;
        return true;
    }
    
    /**
     * Implement {@link Literal#getLabel()} for logging.  Superclass uses 
     * inline value.
     */
    @Override
    public String getLabel() {
        return "LiteralArrayIV["+ivs.length+"]";
    }

    /**
     * We could theoretically get all the inline values from the inline IVs
     * and return them here.
     */
    @Override
    public Object[] getInlineValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * This IV cannot be materialized on its own.  It can only be used
     * within the context of a {@link URIExtensionIV} or 
     * {@link LiteralExtensionIV} as the delegate in cases where the extension
     * mechanism needs an array of inline IVs to represent its URI or Literal
     * respectively.
     */
    @Override
    public BigdataLiteral asValue(LexiconRelation lex) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

}
