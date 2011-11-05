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
/* Portions Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.NumericBOp.NumericOp;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PartlyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.PartlyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * Helper class for {@link IV}s.
 */
/*
 * Note: There are a huge number of warnings in this class, all of which are
 * related to related to IV type parameters. I've taken the liberty to suppress
 * them all.
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class IVUtility {

//    private static final transient Logger log = Logger.getLogger(IVUtility.class);

    public static boolean equals(final IV iv1, final IV iv2) {
        
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
    
    /**
     * This provides a dumb comparison across IVs.
     */
    public static int compare(final IV iv1, final IV iv2) {
        
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
    
    public static IV literalMath(final Literal l1, final Literal l2, 
            final MathOp op)
    {
        final URI dt1 = l1.getDatatype();
        final URI dt2 = l2.getDatatype();
    
        // Only numeric value can be used in math expressions
        if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1)) {
            throw new IllegalArgumentException("Not a number: " + l1);
        }
        if (dt2 == null || !XMLDatatypeUtil.isNumericDatatype(dt2)) {
            throw new IllegalArgumentException("Not a number: " + l2);
        }
    
        // Determine most specific datatype that the arguments have in common,
        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
        // per the SPARQL/XPATH spec
        URI commonDatatype;
    
        if (dt1.equals(XMLSchema.DOUBLE) || dt2.equals(XMLSchema.DOUBLE)) {
            commonDatatype = XMLSchema.DOUBLE;
        } else if (dt1.equals(XMLSchema.FLOAT) || dt2.equals(XMLSchema.FLOAT)) {
            commonDatatype = XMLSchema.FLOAT;
        } else if (dt1.equals(XMLSchema.DECIMAL) || dt2.equals(XMLSchema.DECIMAL)) {
            commonDatatype = XMLSchema.DECIMAL;
        } else if (op == MathOp.DIVIDE) {
            // Result of integer divide is decimal and requires the arguments to
            // be handled as such, see for details:
            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
            commonDatatype = XMLSchema.DECIMAL;
        } else {
            commonDatatype = XMLSchema.INTEGER;
        }
    
        // Note: Java already handles cases like divide-by-zero appropriately
        // for floats and doubles, see:
        // http://www.particle.kth.se/~lindsey/JavaCourse/Book/Part1/Tech/
        // Chapter02/floatingPt2.html
    
        try {
            if (commonDatatype.equals(XMLSchema.DOUBLE)) {
                double left = l1.doubleValue();
                double right = l2.doubleValue();
                return IVUtility.numericalMath(left, right, op);
            }
            else if (commonDatatype.equals(XMLSchema.FLOAT)) {
                float left = l1.floatValue();
                float right = l2.floatValue();
                return IVUtility.numericalMath(left, right, op);
            }
            else if (commonDatatype.equals(XMLSchema.DECIMAL)) {
                BigDecimal left = l1.decimalValue();
                BigDecimal right = l2.decimalValue();
                return IVUtility.numericalMath(left, right, op);
            }
            else { // XMLSchema.INTEGER
                BigInteger left = l1.integerValue();
                BigInteger right = l2.integerValue();
                return IVUtility.numericalMath(left, right, op);
            }
        } catch (NumberFormatException e) {
            throw new SparqlTypeErrorException();
        } catch (ArithmeticException e) {
            throw new SparqlTypeErrorException();
        }
        
    }
    
    public static final IV numericalMath(final Literal l1, final IV iv2, 
            final MathOp op) {
        
        final URI dt1 = l1.getDatatype();
    
        // Only numeric value can be used in math expressions
        if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1)) {
            throw new IllegalArgumentException("Not a number: " + l1);
        }
    
        if (!iv2.isInline())
            throw new IllegalArgumentException(
                    "right term is not inline: left=" + l1 + ", right=" + iv2);
        
        if (!iv2.isLiteral())
            throw new IllegalArgumentException(
                    "right term is not literal: left=" + l1 + ", right=" + iv2);

        final DTE dte2 = iv2.getDTE();

        if (!dte2.isNumeric())
            throw new IllegalArgumentException(
                    "right term is not numeric: left=" + l1 + ", right=" + iv2);

        final AbstractLiteralIV<BigdataLiteral, ?> num2 = (AbstractLiteralIV<BigdataLiteral, ?>) iv2;
        
        // Determine most specific datatype that the arguments have in common,
        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
        // per the SPARQL/XPATH spec

        if (dte2 == DTE.XSDDouble || dt1.equals(XMLSchema.DOUBLE)) {
            return numericalMath(l1.doubleValue(), num2.doubleValue(), op);
        } else if (dte2 == DTE.XSDFloat || dt1.equals(XMLSchema.FLOAT)) {
            return numericalMath(l1.floatValue(), num2.floatValue(), op);
        } else if (dte2 == DTE.XSDDecimal || dt1.equals(XMLSchema.DECIMAL)) {
            return numericalMath(l1.decimalValue(), num2.decimalValue(), op);
        } else if (op == MathOp.DIVIDE) {
            // Result of integer divide is decimal and requires the arguments to
            // be handled as such, see for details:
            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
            return numericalMath(l1.decimalValue(), num2.decimalValue(), op);
        } else {
            return numericalMath(l1.integerValue(), num2.integerValue(), op);
        }

    }
    
    public static final IV numericalMath(final Literal l1, final Literal l2,
            final MathOp op) {

        final URI dt1 = l1.getDatatype();

        // Only numeric value can be used in math expressions
        if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1)) {
            throw new IllegalArgumentException("Not a number: " + l1);
        }

        final URI dt2 = l2.getDatatype();

        // Only numeric value can be used in math expressions
        if (dt2 == null || !XMLDatatypeUtil.isNumericDatatype(dt2)) {
            throw new IllegalArgumentException("Not a number: " + l2);
        }

        // Determine most specific datatype that the arguments have in common,
        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
        // per the SPARQL/XPATH spec

        if (dt2.equals(XMLSchema.DOUBLE)|| dt1.equals(XMLSchema.DOUBLE)) {
            return numericalMath(l1.doubleValue(), l2.doubleValue(), op);
        } else if ( dt2.equals(XMLSchema.FLOAT)|| dt1.equals(XMLSchema.FLOAT)) {
            return numericalMath(l1.floatValue(), l2.floatValue(), op);
        } else if (dt2.equals(XMLSchema.DECIMAL)|| dt1.equals(XMLSchema.DECIMAL)) {
            return numericalMath(l1.decimalValue(), l2.decimalValue(), op);
        } else if (op == MathOp.DIVIDE) {
            // Result of integer divide is decimal and requires the arguments to
            // be handled as such, see for details:
            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
            return numericalMath(l1.decimalValue(), l2.decimalValue(), op);
        } else {
            return numericalMath(l1.integerValue(), l2.integerValue(), op);
        }

    }

    public static final IV numericalMath(final IV iv1, final IV iv2, 
            final MathOp op) {
        
        if (!iv1.isInline())
            throw new IllegalArgumentException(
                    "left term is not inline: left=" + iv1 + ", right=" + iv2);

        if (!iv2.isInline())
            throw new IllegalArgumentException(
                    "right term is not inline: left=" + iv1 + ", right=" + iv2);
        
        if (!iv1.isLiteral())
            throw new IllegalArgumentException(
                    "left term is not literal: left=" + iv1 + ", right=" + iv2);

        if (!iv2.isLiteral())
            throw new IllegalArgumentException(
                    "right term is not literal: left=" + iv1 + ", right=" + iv2);

        final DTE dte1 = iv1.getDTE();
        final DTE dte2 = iv2.getDTE();

        if (!dte1.isNumeric())
            throw new IllegalArgumentException(
                    "right term is not numeric: left=" + iv1 + ", right=" + iv2);

        if (!dte2.isNumeric())
            throw new IllegalArgumentException(
                    "left term is not numeric: left=" + iv1 + ", right=" + iv2);

        final Literal num1 = (Literal) iv1; 
        final Literal num2 = (Literal) iv2; 
        
        // Determine most specific datatype that the arguments have in common,
        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
        // per the SPARQL/XPATH spec
        
        if (dte1 == DTE.XSDDouble || dte2 == DTE.XSDDouble) {
            return numericalMath(num1.doubleValue(), num2.doubleValue(), op);
        } else if (dte1 == DTE.XSDFloat || dte2 == DTE.XSDFloat) {
            return numericalMath(num1.floatValue(), num2.floatValue(), op);
        } if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
        } if (op == MathOp.DIVIDE) {
            // Result of integer divide is decimal and requires the arguments to
            // be handled as such, see for details:
            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
        } else {
            return numericalMath(num1.integerValue(), num2.integerValue(), op);
        }
            
//        // if one's a BigDecimal we should use the BigDecimal comparator for both
//        if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
//            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
//        }
//        
//        // same for BigInteger
//        if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
//            return numericalMath(num1.integerValue(), num2.integerValue(), op);
//        }
//        
//        // fixed length numerics
//        if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
//            // non-BigDecimal floating points
//            if (dte1 == DTE.XSDFloat || dte2 == DTE.XSDFloat)
//                return numericalMath(num1.floatValue(), num2.floatValue(), op);
//            else
//                return numericalMath(num1.doubleValue(), num2.doubleValue(), op);
//        } else {
//            // non-BigInteger integers
//            if (dte1 == DTE.XSDInt && dte2 == DTE.XSDInt)
//                return numericalMath(num1.intValue(), num2.intValue(), op);
//            else
//                return numericalMath(num1.longValue(), num2.longValue(), op);
//        }
        
    }
    
    /**
     * The XPath numeric functions: abs, ceiling, floor, and round.
     * 
     * @param iv1
     *            The operand.
     * @param op
     *            The operation.

     * @return The result.
     * 
     * @see XPathMathFunctions
     */
    public static final IV numericalFunc(final IV iv1, final NumericOp op) {

        if (!iv1.isInline())
            throw new IllegalArgumentException(
                    "left term is not inline: left=" + iv1 );


        if (!iv1.isLiteral())
            throw new IllegalArgumentException(
                    "left term is not literal: left=" + iv1 );


        final DTE dte1 = iv1.getDTE();

        if (!dte1.isNumeric())
            throw new IllegalArgumentException(
                    "left term is not numeric: left=" + iv1 );

        final AbstractLiteralIV num1 = (AbstractLiteralIV) iv1;

        /*
         * FIXME These xpath functions have very custom semantics. They need to
         * be lifted out of this class and put into their own static methods
         * with their own test suites.
         */
//        switch (op) {
//        case ABS:
//            return XPathMathFunctions.abs(iv1);
//        case CEIL:
//            return XPathMathFunctions.ceiling(iv1);
//        case FLOOR:
//            return XPathMathFunctions.floor(iv1);
//        case ROUND:
//            return XPathMathFunctions.round(iv1);
//        default:
//            throw new UnsupportedOperationException(op.toString());
//        }
        
        // if one's a BigDecimal we should use the BigDecimal comparator for
        // both
        if (dte1 == DTE.XSDDecimal) {
            return numericalFunc(num1.decimalValue(), op);
        } else if (dte1 == DTE.XSDInteger) {
            return numericalFunc(num1.integerValue(), op);
        } else if (dte1.isFloatingPointNumeric()) {
            return numericalFunc(num1.floatValue(), op);
        } else if (dte1.equals(DTE.XSDInt)) {
            return numericalFunc(num1.intValue(), op);
        } else if (dte1.equals(DTE.XSDDouble)) {
            return numericalFunc(num1.doubleValue(), op);
        } else {
            return numericalFunc(num1.longValue(), op);
        }
    }
    
    @Deprecated
    private static final IV numericalFunc(final BigDecimal left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDDecimalIV(left.abs());
        case CEIL:
            return new XSDNumericIV(Math.ceil(left.doubleValue()));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left.doubleValue()));
        case ROUND:
            return new XSDDecimalIV(left.round(MathContext.UNLIMITED));
        default:
            throw new UnsupportedOperationException();
        }
    }
    
    @Deprecated
    private static final IV numericalFunc(final BigInteger left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDIntegerIV(left.abs());
        case CEIL:
          return new XSDNumericIV(Math.ceil(left.doubleValue()));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left.doubleValue()));
        case ROUND:
            return new XSDNumericIV(Math.round(left.doubleValue()));
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Deprecated
    private static final IV numericalFunc(final float left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDNumericIV(Math.abs(left));
        case CEIL:
            return new XSDNumericIV(Math.ceil(left));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left));
        case ROUND:
            return new XSDNumericIV(Math.round(left));
        default:
            throw new UnsupportedOperationException();
        }
    }
    
    @Deprecated
    private static final IV numericalFunc(final int left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDNumericIV(Math.abs(left));
        case CEIL:
            return new XSDNumericIV(Math.ceil(left));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left));
        case ROUND:
            return new XSDNumericIV(Math.round(left));
        default:
            throw new UnsupportedOperationException();
        }
    }
    
    @Deprecated
    private static final IV numericalFunc(final long left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDNumericIV(Math.abs(left));
        case CEIL:
            return new XSDNumericIV(Math.ceil(left));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left));
        case ROUND:
            return new XSDNumericIV(Math.round(left));
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Deprecated
    private static final IV numericalFunc(final double left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDNumericIV(Math.abs(left));
        case CEIL:
            return new XSDNumericIV(Math.ceil(left));
        case FLOOR:
            return new XSDNumericIV(Math.floor(left));
        case ROUND:
            return new XSDNumericIV(Math.round(left));
        default:
            throw new UnsupportedOperationException();
        }
    }
    
    private static final IV numericalMath(final BigDecimal left, 
            final BigDecimal right, final MathOp op) {
        
        switch(op) {
        case PLUS:
            return new XSDDecimalIV(left.add(right));
        case MINUS:
            return new XSDDecimalIV(left.subtract(right));
        case MULTIPLY:
            return new XSDDecimalIV(left.multiply(right));
        case DIVIDE:
            /*
             * Note: Change per mroycsi.  Reverts to a half-rounding mode iff
             * an exact quotient can not be represented.
             */
            try {
                // try for exact quotient
                return new XSDDecimalIV(left.divide(right));
            } catch (ArithmeticException ae) {
                // half-rounding mode.
                return new XSDDecimalIV(left.divide(right, 20,
                        RoundingMode.HALF_UP));
            }
//            return new XSDDecimalIV(left.divide(right, RoundingMode.HALF_UP));
        case MIN:
            return new XSDDecimalIV(left.compareTo(right) < 0 ? left : right);
        case MAX:
            return new XSDDecimalIV(left.compareTo(right) > 0 ? left : right);
        default:
            throw new UnsupportedOperationException();
        }
        
    }
    
    private static final IV numericalMath(final BigInteger left, 
            final BigInteger right, final MathOp op) {
        
        switch(op) {
        case PLUS:
            return new XSDIntegerIV(left.add(right));
        case MINUS:
            return new XSDIntegerIV(left.subtract(right));
        case MULTIPLY:
            return new XSDIntegerIV(left.multiply(right));
        case DIVIDE:
            return new XSDIntegerIV(left.divide(right));
        case MIN:
            return new XSDIntegerIV(left.compareTo(right) < 0 ? left : right);
        case MAX:
            return new XSDIntegerIV(left.compareTo(right) > 0 ? left : right);
        default:
            throw new UnsupportedOperationException();
        }
        
    }
    
    private static final IV numericalMath(final float left, 
            final float right, final MathOp op) {
        
        switch(op) {
        case PLUS:
            return new XSDNumericIV(left+right);
        case MINUS:
            return new XSDNumericIV(left-right);
        case MULTIPLY:
            return new XSDNumericIV(left*right);
        case DIVIDE:
            return new XSDNumericIV(left/right);
        case MIN:
            return new XSDNumericIV(Math.min(left,right));
        case MAX:
            return new XSDNumericIV(Math.max(left,right));
        default:
            throw new UnsupportedOperationException();
        }
        
    }
    
    private static final IV numericalMath(final double left, 
            final double right, final MathOp op) {
        
        switch(op) {
        case PLUS:
            return new XSDNumericIV(left+right);
        case MINUS:
            return new XSDNumericIV(left-right);
        case MULTIPLY:
            return new XSDNumericIV(left*right);
        case DIVIDE:
            return new XSDNumericIV(left/right);
        case MIN:
            return new XSDNumericIV(Math.min(left,right));
        case MAX:
            return new XSDNumericIV(Math.max(left,right));
        default:
            throw new UnsupportedOperationException();
        }
        
    }
    
    /**
     * Encode an RDF value into a key for one of the statement indices.  Handles
     * null {@link IV} references gracefully.
     * 
     * @param keyBuilder
     *            The key builder.
     * @param iv
     *            The internal value (can be <code>null</code>).
     * 
     * @return The key builder.
     */
    public static IKeyBuilder encode(final IKeyBuilder keyBuilder, final IV iv) {

        if (iv == null) {

            TermId.NullIV.encode(keyBuilder);
            
        } else {
            
            iv.encode(keyBuilder);
            
        }

        return keyBuilder;
        
    }
    
    /**
     * Decode an {@link IV} from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The {@link IV}.
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
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int numTerms) {

        return decode(key, 0 /* offset */, numTerms);
        
    }
    
    /**
     * Decodes up to numTerms {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @param offset
     *            The offset into the byte[] key.
     * @param numTerms
     *            The number of terms to decode.
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decode(final byte[] key, final int offset, 
            final int numTerms) {
            
        if (numTerms <= 0)
            return new IV[0];
        
        final IV[] ivs = new IV[numTerms];
        
        int o = offset;
        
        for (int i = 0; i < numTerms; i++) {

            if (o >= key.length)
                throw new IllegalArgumentException(
                        "key is not long enough to decode " 
                        + numTerms + " terms.");
            
            ivs[i] = decodeFromOffset(key, o);
            
            o += ivs[i] == null 
                    ? TermId.NullIV.byteLength() : ivs[i].byteLength();
            
        }

        return ivs;
        
    }
    
    /**
     * Decodes all {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     *            
     * @return The set of {@link IV}s.
     */
    public static IV[] decodeAll(final byte[] key) {

        return decodeAll(key, 0/* fromOffset */, key.length/* toOffset */);

    }

    /**
     * Decodes all {@link IV}s from a slice of a byte[].
     * 
     * @param key
     *            The byte[].
     * @param fromOffset
     *            The offset of the first encoded {@link IV} in the byte[].
     * @param toOffset
     *            The offset of the first byte which will not be decoded.
     * 
     * @return The set of {@link IV}s.
     */
    public static IV[] decodeAll(final byte[] key, final int fromOffset,
            final int toOffset) {

        final List<IV> ivs = new LinkedList<IV>();
        
        int offset = fromOffset;
        
        while (offset < toOffset) {

            final IV iv = decodeFromOffset(key, offset);
            
            ivs.add(iv);
            
            offset += iv == null
                    ? TermId.NullIV.byteLength() : iv.byteLength();
            
        }
        
        return ivs.toArray(new IV[ivs.size()]);
        
    }

    /**
     * Decode one {@link IV}.
     * 
     * @param key
     *            The unsigned byte[] key.
     * @param offset
     *            The offset.
     *            
     * @return The {@link IV} decoded from that offset.
     */
    public static IV decodeFromOffset(final byte[] key, final int offset) {

        int o = offset;
        
        final byte flags = KeyBuilder.decodeByte(key[o++]);

        /*
         * Handle an IV which is not 100% inline.
         */
        if (!AbstractIV.isInline(flags)) {

            if (AbstractIV.isExtension(flags)) {

                /*
                 * Handle non-inline URI or Literal. 
                 */

                final byte extensionByte = KeyBuilder.decodeByte(key[o++]); 

                if (extensionByte < 0) {

                    // Decode the extension IV.
                    final IV extensionIV = IVUtility.decodeFromOffset(key, o);

                    // skip over the extension IV.
                    o += extensionIV.byteLength();

                    // Decode the inline component.
                    final AbstractLiteralIV delegate = (AbstractLiteralIV) 
                    		IVUtility.decodeFromOffset(key, o);

                    // TODO Should really be switch((int)extensionByte).
                    switch (AbstractIV.getInternalValueTypeEnum(flags)) {
                    case URI:
                        return new PartlyInlineURIIV<BigdataURI>(delegate,
                                extensionIV);
                    case LITERAL:
                        return new PartlyInlineTypedLiteralIV<BigdataLiteral>(delegate,
                                extensionIV);
                    default:
                        throw new AssertionError();
                    }
                    
                } else {
                    
                    /*
                     * Handle a BlobIV.
                     * 
                     * Note: This MUST be consistent with
                     * TermsIndexHelper#makeKey() and BlobIV.
                     */

                    final int hashCode = KeyBuilder.decodeInt(key, o);
                    
                    o += BlobsIndexHelper.SIZEOF_HASH;
                    
                    final short counter = KeyBuilder.decodeShort(key, o);
                    
                    o += BlobsIndexHelper.SIZEOF_COUNTER;
                    
                    final BlobIV<?> iv = new BlobIV(flags, hashCode, counter);

                    return iv;
                    
                }
                
            } else {

                /*
                 * Handle a TermId, including a NullIV.
                 */ 
                
                // decode the term identifier.
                final long termId = KeyBuilder.decodeLong(key, o);

                if (termId == TermId.NULL)
                    return null;
                else
                    return new TermId(flags, termId);
                
            }

        }

        /*
         * Handle an inline value. 
         */

        // The value type (URI, Literal, BNode, SID)
        final VTE vte = AbstractIV.getInternalValueTypeEnum(flags);

        switch (vte) {
        case STATEMENT: {
            /*
             * Handle inline sids.
             */
            // spo is directly decodable from key
            final ISPO spo = SPOKeyOrder.SPO.decodeKey(key, o);
            // all spos that have a sid are explicit
            spo.setStatementType(StatementEnum.Explicit);
            spo.setStatementIdentifier(true);
            // create a sid iv and return it
            return new SidIV(spo);
        }
        case BNODE:
            return decodeInlineBNode(flags,key,o);
        case URI:
            return decodeInlineURI(flags,key,o);
        case LITERAL:
            return decodeInlineLiteral(flags, key, o);
        default:
            throw new AssertionError();
        }
        
    }

    /**
     * Decode an inline blank node from an offset.
     * 
     * @param flags
     *            The flags.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     *            
     * @return The decoded {@link IV}.
     */
    static private IV decodeInlineBNode(final byte flags, final byte[] key,
            final int o) {
        
        // The data type
        final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
        switch (dte) {
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            return new NumericBNodeIV<BigdataBNode>(x);
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            return new UUIDBNodeIV<BigdataBNode>(x);
        }
        case XSDString: {
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nbytes;
            try {
                nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o,
                        key.length - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FullyInlineUnicodeBNodeIV<BigdataBNode>(str1,
                    1/* flags */+ nbytes);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }
    }

    /**
     * Decode an inline URI from a byte offset.
     * 
     * @param flags
     *            The flags byte.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     * 
     * @return The decoded {@link IV}.
     */
    static private IV decodeInlineURI(final byte flags, final byte[] key,
            final int o) {

        // The data type
        final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
        switch (dte) {
        case XSDByte: {
            final byte x = key[o];//KeyBuilder.decodeByte(key[o]);
            return new VocabURIByteIV<BigdataURI>(x);
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            return new VocabURIShortIV<BigdataURI>(x);
        }
        case XSDString: {
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nbytes;
            try {
                nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FullyInlineURIIV<BigdataURI>(new URIImpl(str1),
                    1/* flags */+ nbytes);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }

    }

    /**
     * Decode an inline literal from an offset.
     * 
     * @param flags
     *            The flags byte.
     * @param key
     *            The key.
     * @param o
     *            The offset.
     */
    static private IV decodeInlineLiteral(final byte flags, final byte[] key,
            int o) {

        // The data type
        final DTE dte = AbstractIV.getInternalDataTypeEnum(flags);
        
        final boolean isExtension = AbstractIV.isExtension(flags);
        
        final IV datatype; 
        if (isExtension) {
            datatype = decodeFromOffset(key, o);
            o += datatype.byteLength();
        } else {
            datatype = null;
        }

        switch (dte) {
        case XSDBoolean: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = (x == 0) ? 
                    XSDBooleanIV.FALSE : XSDBooleanIV.TRUE;
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDFloat: {
            final float x = KeyBuilder.decodeFloat(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDDouble: {
            final double x = KeyBuilder.decodeDouble(key, o);
            final AbstractLiteralIV iv = new XSDNumericIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDInteger: {
            final BigInteger x = KeyBuilder.decodeBigInteger(o, key);
            final AbstractLiteralIV iv = new XSDIntegerIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDDecimal: {
            final BigDecimal x = KeyBuilder.decodeBigDecimal(o, key);
            final AbstractLiteralIV iv = new XSDDecimalIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case UUID: {
            final UUID x = KeyBuilder.decodeUUID(key, o);
            final AbstractLiteralIV iv = new UUIDLiteralIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDUnsignedByte: {
            final byte x = KeyBuilder.decodeByte(key[o]);
            final AbstractLiteralIV iv = new XSDUnsignedByteIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDUnsignedShort: {
            final short x = KeyBuilder.decodeShort(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedShortIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDUnsignedInt: {
            final int x = KeyBuilder.decodeInt(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedIntIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv;
        }
        case XSDUnsignedLong: {
            final long x = KeyBuilder.decodeLong(key, o);
            final AbstractLiteralIV iv = new XSDUnsignedLongIV<BigdataLiteral>(x);
            return isExtension ? new LiteralExtensionIV(iv, datatype) : iv; 
        }
        case XSDString: {
            if(isExtension) {
            // decode the termCode
            final byte termCode = key[o++];
            assert termCode == ITermIndexCodes.TERM_CODE_LIT : "termCode="
                    + termCode;
            // decode buffer.
            final StringBuilder sb = new StringBuilder();
            // inline string value
            final String str1;
            // #of bytes read.
            final int nread;
            try {
                nread = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                        - o), sb);
                str1 = sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Note: The 'delegate' will be an InlineLiteralIV w/o a datatype.
            final FullyInlineTypedLiteralIV<BigdataLiteral> iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(
                    str1, null/* languageCode */, null/* datatype */,
                    1/* flags */+ 1/* termCode */+ nread);
            return isExtension ? new LiteralExtensionIV<BigdataLiteral>(iv, datatype)
                    : iv;
            }
            return decodeInlineUnicodeLiteral(key,o);
        }
        default:
            throw new UnsupportedOperationException("dte=" + dte);
        }

    }

    /**
     * Decode an inline literal which is represented as a one or two compressed
     * Unicode values.
     * 
     * @param key
     *            The key.
     * @param offset
     *            The offset into the key.
     *            
     * @return The decoded {@link IV}.
     */
    static private FullyInlineTypedLiteralIV<BigdataLiteral> decodeInlineUnicodeLiteral(
            final byte[] key, final int offset) {

        int o = offset;

        /*
         * Fully inline literal.
         */

        // decode the termCode
        final byte termCode = key[o++];
        // figure out the #of string values which were inlined.
        final int nstrings;
        final String str1, str2; 
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            nstrings = 1;
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            nstrings = 2;
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            nstrings = 2;
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        // #of bytes read (not including the flags and termCode).
        int nread = 0;
        // decode buffer.
        final StringBuilder sb = new StringBuilder();
        // first inline string value
        try {
            final int nbytes = IVUnicode.decode(new ByteArrayInputStream(key, o, key.length
                    - o), sb);
            str1 = sb.toString();
            nread += nbytes;
            o += nbytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // second inline string value
        if (nstrings == 2) {
            sb.setLength(0); // reset buffer.
            try {
                final int nbytes = IVUnicode.decode(new ByteArrayInputStream(key,
                        o, key.length - o), sb);
                str2 = sb.toString();
                nread += nbytes;
                o += nbytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            str2 = null;
        }
        final int byteLength = 1/* flags */+ 1/* termCode */+ nread;
        final FullyInlineTypedLiteralIV<BigdataLiteral> iv;
        switch (termCode) {
        case ITermIndexCodes.TERM_CODE_LIT:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str1,//
                    null, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_LCL:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str2,//
                    str1, // language
                    null, // datatype
                    byteLength//
                    );
            break;
        case ITermIndexCodes.TERM_CODE_DTL:
            iv = new FullyInlineTypedLiteralIV<BigdataLiteral>(//
                    str2,//
                    null, // language
                    new URIImpl(str1), // datatype
                    byteLength//
                    );
            break;
        default:
            throw new AssertionError("termCode=" + termCode);
        }
        return iv;
    }

    /**
     * Decode an IV from its string representation as encoded by
     * {@link BlobIV#toString()} and {@link AbstractInlineIV#toString()} (this
     * is used by the prototype IRIS integration.)
     * 
     * @param s
     *            the string representation
     * @return the IV
     */
    public static final IV fromString(final String s) {
        if (s.startsWith("TermIV")) {
            return TermId.fromString(s);
        } else if (s.startsWith("BlobIV")) {
                return BlobIV.fromString(s);
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
                return new XSDNumericIV<BigdataLiteral>(x);
            }
            case XSDShort: {
                final short x = Short.valueOf(val);
                return new XSDNumericIV<BigdataLiteral>(x);
            }
            case XSDInt: {
                final int x = Integer.valueOf(val);
                return new XSDNumericIV<BigdataLiteral>(x);
            }
            case XSDLong: {
                final long x = Long.valueOf(val);
                return new XSDNumericIV<BigdataLiteral>(x);
            }
            case XSDFloat: {
                final float x = Float.valueOf(val);
                return new XSDNumericIV<BigdataLiteral>(x);
            }
            case XSDDouble: {
                final double x = Double.valueOf(val);
                return new XSDNumericIV<BigdataLiteral>(x);
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
