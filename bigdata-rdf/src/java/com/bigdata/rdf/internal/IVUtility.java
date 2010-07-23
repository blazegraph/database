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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.UUID;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.constraints.AbstractInlineConstraint;
import com.bigdata.rdf.internal.constraints.InlineGT;
import com.bigdata.rdf.model.BigdataBNode;
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
    
    public static int compare(IV iv1, IV iv2) {
        
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
     * This method attempts to compare two inline internal values based on
     * their numerical representation, which is different from the natural
     * sort ordering of IVs (the natural sort ordering considers the datatype).
     * <p>
     * So for example, if we have two IVs representing 2.1d and 1l, this method
     * will attempt to compare 2.1 to 1 ignoring the datatype. This is useful
     * for SPARQL filters.
     */
    public static int numericalCompare(IV iv1, IV iv2) {

        if (!iv1.isInline() || !iv2.isInline())
            throw new IllegalArgumentException("not inline terms");
        
        if (!iv1.isLiteral() || !iv2.isLiteral())
            throw new IllegalArgumentException("not literals");
        
        final DTE dte1 = iv1.getDTE();
        final DTE dte2 = iv2.getDTE();

        final int numBooleans = 
                (dte1 == DTE.XSDBoolean ? 1 : 0) +
                (dte2 == DTE.XSDBoolean ? 1 : 0);
        
        // we can do two booleans
        if (numBooleans == 1)
            throw new IllegalArgumentException("only one boolean");

        // we can do two signed numerics
        if (numBooleans == 0)
        if (!dte1.isNumeric() || !dte2.isNumeric())
            throw new IllegalArgumentException("not signed numerics");
        
        // we can use the natural ordering if they have the same DTE
        // this will naturally take care of two booleans or two numerics of the
        // same datatype
        if (dte1 == dte2)
            return iv1.compareTo(iv2);
        
        // otherwise we need to try to convert them into comparable numbers
        final AbstractLiteralIV num1 = 
            (AbstractLiteralIV) iv1; 
        final AbstractLiteralIV num2 = 
            (AbstractLiteralIV) iv2; 
        
        // if one's a BigDecimal we should use the BigDecimal comparator for both
        if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
            return num1.decimalValue().compareTo(num2.decimalValue());
        }
        
        // same for BigInteger
        if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
            return num1.integerValue().compareTo(num2.integerValue());
        }
        
        // fixed length numerics
        if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
            // non-BigDecimal floating points - use doubles
            return Double.compare(num1.doubleValue(), num2.doubleValue());
        } else {
            // non-BigInteger integers - use longs
            final long a = num1.longValue();
            final long b = num2.longValue();
            return a == b ? 0 : a < b ? -1 : 1;
        }
        
    }
    
    /**
     * Used to test whether a given value constant can be used in an inline
     * filter or not.  If so, we can use one of the inline constraints
     * {@link InlineGT}, {@link AbstractInlineConstraint}, {@link LT}, {@link LE}, which in turn defer
     * to the numerical comparison operator {@link #numericalCompare(IV, IV)}. 
     */
    public static final boolean canNumericalCompare(final IV iv) {
        
        // inline boolean or inline signed numeric
        return iv.isInline() && iv.isLiteral() &&
                (iv.getDTE() == DTE.XSDBoolean || iv.isNumeric());
        
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
            
            offset += ivs[i] == null 
                    ? NullIV.INSTANCE.byteLength() : ivs[i].byteLength();
            
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
            
            offset += iv == null
                    ? NullIV.INSTANCE.byteLength() : iv.byteLength();
            
        }
        
        return ivs.toArray(new IV[ivs.size()]);
        
    }
    
    private static IV decodeFromOffset(final byte[] key, final int offset) {

        int o = offset;
        
        final byte flags = KeyBuilder.decodeByte(key[o++]);
            
        /*
         * FIXME iNull does not work yet
         */
        if (AbstractIV.isNull(flags))
            return null;
    
        
        /*
         * Handle a term identifier (versus an inline value).
         */
        if (!AbstractIV.isInline(flags)) {
    
            // decode the term identifier.
            final long termId = KeyBuilder.decodeLong(key, o);

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
        final VTE vte = AbstractIV.getInternalValueTypeEnum(flags);

        // The data type
        final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
        
        final boolean isExtension = AbstractIV.isExtension(flags);
        
        final TermId datatype;
        if (isExtension) {
            datatype = new TermId(VTE.URI, KeyBuilder.decodeLong(key, o));
            o += Bytes.SIZEOF_LONG;
        } else {
            datatype = null;
        }

        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = (x == 0) ? 
                    XSDBooleanIV.FALSE : XSDBooleanIV.TRUE;
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDByteIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDShortIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            if (vte == VTE.LITERAL) {
                final AbstractLiteralIV iv = new XSDIntIV<BigdataLiteral>(x);
                return isExtension ? new ExtensionIV(iv, datatype) : iv;
            } else {
                return new NumericBNodeIV<BigdataBNode>(x);
            }
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDLongIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, o);
            final AbstractLiteralIV iv = new XSDFloatIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, o);
            final AbstractLiteralIV iv = new XSDDoubleIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDInteger: {
            final BigInteger x = KeyBuilder.decodeBigInteger(o, key);
            final AbstractLiteralIV iv = new XSDIntegerIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case XSDDecimal: {
            final BigDecimal x = KeyBuilder.decodeBigDecimal(o, key);
            final AbstractLiteralIV iv = new XSDDecimalIV<BigdataLiteral>(x);
            return isExtension ? new ExtensionIV(iv, datatype) : iv; 
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            if (vte == VTE.LITERAL) {
                final AbstractLiteralIV iv = new UUIDLiteralIV<BigdataLiteral>(x);
                return isExtension ? new ExtensionIV(iv, datatype) : iv;
            } else {
                return new UUIDBNodeIV<BigdataBNode>(x);
            }
        }
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
    
    /**
     * Decode an IV from its string representation as encoded by
     * {@link TermId#toString()} and 
     * {@link AbstractInlineIV#toString()}.
     * 
     * @param s
     *          the string representation
     * @return
     *          the IV
     */
    public static final IV fromString(String s) {
        if (s.startsWith("TermId")) {
            char type = s.charAt(s.length()-2);
            long tid = Long.valueOf(s.substring(7, s.length()-2));
            return new TermId(VTE.valueOf(type), tid);
        } else {
            final String type = s.substring(0, s.indexOf('(')); 
            final String val = s.substring(s.indexOf('('), s.length()-1);
            final DTE dte = Enum.valueOf(DTE.class, type);
            switch (dte) {
            case XSDBoolean: {
                final boolean b = Boolean.valueOf(val);
                if (b) {
                    return XSDBooleanIV.TRUE;
                } else {
                    return XSDBooleanIV.FALSE;
                }
            }
            case XSDByte: {
                final byte x = Byte.valueOf(val);
                return new XSDByteIV<BigdataLiteral>(x);
            }
            case XSDShort: {
                final short x = Short.valueOf(val);
                return new XSDShortIV<BigdataLiteral>(x);
            }
            case XSDInt: {
                final int x = Integer.valueOf(val);
                return new XSDIntIV<BigdataLiteral>(x);
            }
            case XSDLong: {
                final long x = Long.valueOf(val);
                return new XSDLongIV<BigdataLiteral>(x);
            }
            case XSDFloat: {
                final float x = Float.valueOf(val);
                return new XSDFloatIV<BigdataLiteral>(x);
            }
            case XSDDouble: {
                final double x = Double.valueOf(val);
                return new XSDDoubleIV<BigdataLiteral>(x);
            }
            case UUID: {
                final UUID x = UUID.fromString(val);
                return new UUIDLiteralIV<BigdataLiteral>(x);
            }
            case XSDInteger: {
                final BigInteger x = new BigInteger(val);
                return new XSDIntegerIV<BigdataLiteral>(x);
            }
            case XSDDecimal: {
                final BigDecimal x = new BigDecimal(val);
                return new XSDDecimalIV<BigdataLiteral>(x);
            }
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
                throw new UnsupportedOperationException("dte=" + dte);
            }
        }
    }
    
}
