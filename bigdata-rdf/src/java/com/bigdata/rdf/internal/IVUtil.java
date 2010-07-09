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
import java.util.UUID;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;


/**
 * Helper class for {@link IV}s.
 */
public class IVUtil {

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
     * @param offset
     *            The starting offset.
     * @return The {@link IV}.
     * 
     *         FIXME handle all of the inline value types.
     * 
     *         FIXME Construct the InternalValue objects using factory if we
     *         will have to scope how the RDF Value is represented to the
     *         lexicon relation with which it is associated?
     */
    public static IV<?, ?> decode(final byte[] key, final int offset) {

        final byte flags = KeyBuilder.decodeByte(key[offset]);

        final int offset1 = offset + 1;

        if (!AbstractInternalValue.isInline(flags)) {

            /*
             * Handle a term identifier (versus an inline value).
             */

            // decode the term identifier.
            final long termId = KeyBuilder.decodeLong(key, offset1);
//            offset += Bytes.SIZEOF_LONG;

            return new TermId(flags, termId);

        }

        /*
         * Handle an inline value.
         */
        // The value type (URI, Literal, BNode, SID)
        final VTE vte = AbstractInternalValue.getInternalValueTypeEnum(flags);

        // The data type
        final DTE dte = AbstractInternalValue.getInternalDataTypeEnum(flags);

        final IV<?, ?> v;
        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[offset1]);
            if (x == 0) {
                v = XSDBooleanInternalValue.FALSE;
            } else {
                v = XSDBooleanInternalValue.TRUE;
            }
            break;
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[offset1]);
            v = new XSDByteInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, offset1);
            v = new XSDShortInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, offset1);
            v = new XSDIntInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, offset1);
            v = new XSDLongInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, offset1);
            v = new XSDFloatInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, offset1);
            v = new XSDDoubleInternalValue<BigdataLiteral>(x);
            break;
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, offset1);
            v = new UUIDInternalValue<BigdataLiteral>(x);
            break;
        }
        case XSDInteger: {
            final byte[] b = KeyBuilder.decodeBigInteger2(offset1, key);
            final BigInteger x = new BigInteger(b);
            v = new XSDIntegerInternalValue<BigdataLiteral>(x);
            break;
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

        return v;
        
    }

}
