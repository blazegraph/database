/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.UUID;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;


/**
 * Helper class for {@link IV}s.
 */
public class IVUtility {

    public static boolean equals(IV iv1, IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2) {
            return true;
        }
        
        // one of them is null
        if (iv1 == null || iv2 == null) {
            return false;
        }
        
        // only possibility left if that neither are null
        return iv1.equals(iv2);
        
    }
    
    public static int compareTo(IV iv1, IV iv2) {
        
        // same IV or both null
        if (iv1 == iv2)
            return 0;
        
        // one of them is null
        if (iv1 == null)
            return -1;
        
        if (iv2 == null)
            return 1;
        
        // only possibility left if that neither are null
        return iv1.compareTo(iv2);
        
    }

    /**
     * Decode an {@link IV} from a byte[].
     * 
     * @param key
     *            The byte[].
     * @return The {@link IV}.
     * 
     *         FIXME handle all of the inline value types.
     * 
     *         FIXME Construct the InternalValue objects using factory if we
     *         will have to scope how the RDF Value is represented to the
     *         lexicon relation with which it is associated?
     */
    public static IV decode(final byte[] key) {

        return decodeFromOffset(key, 0);
        
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param numTerms
     *            The number of terms to decode.
     * @return The set of {@link IV}s.
     * 
     *         FIXME handle all of the inline value types.
     * 
     *         FIXME Construct the InternalValue objects using factory if we
     *         will have to scope how the RDF Value is represented to the
     *         lexicon relation with which it is associated?
     */
    public static IV[] decode(final byte[] key, final int numTerms) {

        if (numTerms <= 0)
            return new IV[0];
        
        final IV[] ivs = new IV[numTerms];
        
        int offset = 0;
        
        for (int i = 0; i < numTerms; i++) {

            if (offset >= key.length)
                throw new IllegalArgumentException(
                        "key is not long enough to decode " 
                        + numTerms + " terms.");
            
            ivs[i] = decodeFromOffset(key, offset);
            
            offset += ivs[i].byteLength();
            
        }
        
        return ivs;
        
    }
    
    /**
     * Decodes all {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @return The set of {@link IV}s.
     * 
     *         FIXME handle all of the inline value types.
     * 
     *         FIXME Construct the InternalValue objects using factory if we
     *         will have to scope how the RDF Value is represented to the
     *         lexicon relation with which it is associated?
     */
    public static IV[] decodeAll(final byte[] key) {

        final ArrayList<IV> ivs = new ArrayList<IV>();
        
        int offset = 0;
        
        while (offset < key.length) {

            final IV iv = decodeFromOffset(key, offset);
            
            ivs.add(iv);
            
            offset += iv.byteLength();
        }
        
        return ivs.toArray(new IV[ivs.size()]);
        
    }
    
    private static IV decodeFromOffset(final byte[] key, final int offset) {

        final byte flags = KeyBuilder.decodeByte(key[offset]);
            
        /*
         * FIXME iNull does not work yet
         */
        if (AbstractInternalValue.isNull(flags))
            return null;
    
        
        /*
         * Handle a term identifier (versus an inline value).
         */
        if (!AbstractInternalValue.isInline(flags)) {
    
            // decode the term identifier.
            final long termId = KeyBuilder.decodeLong(key, offset+1);

            /*
             * FIXME this is here for now until 
             * {@link AbstractInternalValue#isNull(byte)} works.
             */
            if (termId == TermId.NULL)
                return null;
            else
                return new TermId(flags, termId);

        }

        /*
         * Handle an inline value.
         */
        // The value type (URI, Literal, BNode, SID)
        final VTE vte = AbstractInternalValue.getInternalValueTypeEnum(flags);

        // The data type
        final DTE dte = AbstractInternalValue.getInternalDataTypeEnum(flags);

        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[offset+1]);
            if (x == 0) {
                return XSDBooleanInternalValue.FALSE;
            } else {
                return XSDBooleanInternalValue.TRUE;
            }
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[offset+1]);
            return new XSDByteInternalValue<BigdataLiteral>(x);
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, offset+1);
            return new XSDShortInternalValue<BigdataLiteral>(x);
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, offset+1);
            return new XSDIntInternalValue<BigdataLiteral>(x);
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, offset+1);
            return new XSDLongInternalValue<BigdataLiteral>(x);
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, offset+1);
            return new XSDFloatInternalValue<BigdataLiteral>(x);
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, offset+1);
            return new XSDDoubleInternalValue<BigdataLiteral>(x);
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, offset+1);
            return new UUIDInternalValue<BigdataLiteral>(x);
        }
        case XSDInteger: {
            final byte[] b = KeyBuilder.decodeBigInteger2(offset+1, key);
            final BigInteger x = new BigInteger(b);
            return new XSDIntegerInternalValue<BigdataLiteral>(x);
        }
            // case XSDDecimal:
            // keyBuilder.append(t.decimalValue());
            // break;
            // case XSDUnsignedByte:
            // keyBuilder.appendUnsigned(t.byteValue());
            // break;
            // case XSDUnsignedShort:
            // keyBuilder.appendUnsigned(t.shortValue());
            // break;
            // case XSDUnsignedInt:
            // keyBuilder.appendUnsigned(t.intValue());
            // break;
            // case XSDUnsignedLong:
            // keyBuilder.appendUnsigned(t.longValue());
            // break;
        default:
            throw new UnsupportedOperationException("vte=" + vte + ", dte="
                    + dte);
        }

    }
    
}
