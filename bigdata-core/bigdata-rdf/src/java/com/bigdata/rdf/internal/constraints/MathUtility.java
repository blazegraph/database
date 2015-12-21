package com.bigdata.rdf.internal.constraints;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XPathMathFunctions;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.NumericBOp.NumericOp;
import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.NumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValueFactory;

public class MathUtility implements IMathOpHandler {

    public static boolean checkNumericDatatype(final Literal... args) {
    	for (Literal lit : args) {
    		final URI dt = lit.getDatatype();

    		if (dt == null)
    		    return false;
    		
    		boolean isNumeric = false;
    		isNumeric |= XMLDatatypeUtil.isNumericDatatype(dt);
    		isNumeric |= dt.equals(CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
    		
    		if (!isNumeric)
    			return false;
    	}
    	return true;
    }

    @Override
    public boolean canInvokeMathOp(final Literal... args) {
        return checkNumericDatatype(args);
    }

    @Override
    public NumericIV doMathOp(final Literal l1, final IV iv1,
            final Literal l2, final IV iv2,
            final MathOp op,
            final BigdataValueFactory vf)
    {
        return literalMath(l1, l2, op);
    }


    public static NumericIV literalMath(final Literal l1, final Literal l2,
                        final MathOp op)
    {
    	if (!checkNumericDatatype(l1, l2))
    		throw new IllegalArgumentException("Not numbers: " + l1 + ", " + l2);

    	final URI dt1 = l1.getDatatype();
        final URI dt2 = l2.getDatatype();

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
                return numericalMath(left, right, op);
            }
            else if (commonDatatype.equals(XMLSchema.FLOAT)) {
                float left = l1.floatValue();
                float right = l2.floatValue();
                return numericalMath(left, right, op);
            }
            else if (commonDatatype.equals(XMLSchema.DECIMAL)) {
                BigDecimal left = l1.decimalValue();
                BigDecimal right = l2.decimalValue();
                return numericalMath(left, right, op);
            }
            else { // XMLSchema.INTEGER
                BigInteger left = l1.integerValue();
                BigInteger right = l2.integerValue();
                return numericalMath(left, right, op);
            }
        } catch (NumberFormatException e) {
            throw new SparqlTypeErrorException();
        } catch (ArithmeticException e) {
            throw new SparqlTypeErrorException();
        }

    }

//    public static final IV numericalMath(final Literal l1, final IV iv2,
//            final MathOp op) {
//
//        final URI dt1 = l1.getDatatype();
//
//        // Only numeric value can be used in math expressions
//        if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1)) {
//            throw new IllegalArgumentException("Not a number: " + l1);
//        }
//
//        if (!iv2.isInline())
//            throw new IllegalArgumentException(
//                    "right term is not inline: left=" + l1 + ", right=" + iv2);
//
//        if (!iv2.isLiteral())
//            throw new IllegalArgumentException(
//                    "right term is not literal: left=" + l1 + ", right=" + iv2);
//
//        final DTE dte2 = iv2.getDTE();
//
//        if (!dte2.isNumeric())
//            throw new IllegalArgumentException(
//                    "right term is not numeric: left=" + l1 + ", right=" + iv2);
//
//        final AbstractLiteralIV<BigdataLiteral, ?> num2 = (AbstractLiteralIV<BigdataLiteral, ?>) iv2;
//
//        // Determine most specific datatype that the arguments have in common,
//        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
//        // per the SPARQL/XPATH spec
//
//        if (dte2 == DTE.XSDDouble || dt1.equals(XMLSchema.DOUBLE)) {
//            return numericalMath(l1.doubleValue(), num2.doubleValue(), op);
//        } else if (dte2 == DTE.XSDFloat || dt1.equals(XMLSchema.FLOAT)) {
//            return numericalMath(l1.floatValue(), num2.floatValue(), op);
//        } else if (dte2 == DTE.XSDDecimal || dt1.equals(XMLSchema.DECIMAL)) {
//            return numericalMath(l1.decimalValue(), num2.decimalValue(), op);
//        } else if (op == MathOp.DIVIDE) {
//            // Result of integer divide is decimal and requires the arguments to
//            // be handled as such, see for details:
//            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
//            return numericalMath(l1.decimalValue(), num2.decimalValue(), op);
//        } else {
//            return numericalMath(l1.integerValue(), num2.integerValue(), op);
//        }
//
//    }
//
//    public static final IV numericalMath(final Literal l1, final Literal l2,
//            final MathOp op) {
//
//        final URI dt1 = l1.getDatatype();
//
//        // Only numeric value can be used in math expressions
//        if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1)) {
//            throw new IllegalArgumentException("Not a number: " + l1);
//        }
//
//        final URI dt2 = l2.getDatatype();
//
//        // Only numeric value can be used in math expressions
//        if (dt2 == null || !XMLDatatypeUtil.isNumericDatatype(dt2)) {
//            throw new IllegalArgumentException("Not a number: " + l2);
//        }
//
//        // Determine most specific datatype that the arguments have in common,
//        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
//        // per the SPARQL/XPATH spec
//
//        if (dt2.equals(XMLSchema.DOUBLE)|| dt1.equals(XMLSchema.DOUBLE)) {
//            return numericalMath(l1.doubleValue(), l2.doubleValue(), op);
//        } else if ( dt2.equals(XMLSchema.FLOAT)|| dt1.equals(XMLSchema.FLOAT)) {
//            return numericalMath(l1.floatValue(), l2.floatValue(), op);
//        } else if (dt2.equals(XMLSchema.DECIMAL)|| dt1.equals(XMLSchema.DECIMAL)) {
//            return numericalMath(l1.decimalValue(), l2.decimalValue(), op);
//        } else if (op == MathOp.DIVIDE) {
//            // Result of integer divide is decimal and requires the arguments to
//            // be handled as such, see for details:
//            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
//            return numericalMath(l1.decimalValue(), l2.decimalValue(), op);
//        } else {
//            return numericalMath(l1.integerValue(), l2.integerValue(), op);
//        }
//
//    }
//
//    public static final IV numericalMath(final IV iv1, final IV iv2,
//            final MathOp op) {
//
//        if (!iv1.isInline())
//            throw new IllegalArgumentException(
//                    "left term is not inline: left=" + iv1 + ", right=" + iv2);
//
//        if (!iv2.isInline())
//            throw new IllegalArgumentException(
//                    "right term is not inline: left=" + iv1 + ", right=" + iv2);
//
//        if (!iv1.isLiteral())
//            throw new IllegalArgumentException(
//                    "left term is not literal: left=" + iv1 + ", right=" + iv2);
//
//        if (!iv2.isLiteral())
//            throw new IllegalArgumentException(
//                    "right term is not literal: left=" + iv1 + ", right=" + iv2);
//
//        final DTE dte1 = iv1.getDTE();
//        final DTE dte2 = iv2.getDTE();
//
//        if (!dte1.isNumeric())
//            throw new IllegalArgumentException(
//                    "right term is not numeric: left=" + iv1 + ", right=" + iv2);
//
//        if (!dte2.isNumeric())
//            throw new IllegalArgumentException(
//                    "left term is not numeric: left=" + iv1 + ", right=" + iv2);
//
//        final Literal num1 = (Literal) iv1;
//        final Literal num2 = (Literal) iv2;
//
//        // Determine most specific datatype that the arguments have in common,
//        // choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
//        // per the SPARQL/XPATH spec
//
//        if (dte1 == DTE.XSDDouble || dte2 == DTE.XSDDouble) {
//            return numericalMath(num1.doubleValue(), num2.doubleValue(), op);
//        } else if (dte1 == DTE.XSDFloat || dte2 == DTE.XSDFloat) {
//            return numericalMath(num1.floatValue(), num2.floatValue(), op);
//        } if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
//            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
//        } if (op == MathOp.DIVIDE) {
//            // Result of integer divide is decimal and requires the arguments to
//            // be handled as such, see for details:
//            // http://www.w3.org/TR/xpath-functions/#func-numeric-divide
//            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
//        } else {
//            return numericalMath(num1.integerValue(), num2.integerValue(), op);
//        }
//
////        // if one's a BigDecimal we should use the BigDecimal comparator for both
////        if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
////            return numericalMath(num1.decimalValue(), num2.decimalValue(), op);
////        }
////
////        // same for BigInteger
////        if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
////            return numericalMath(num1.integerValue(), num2.integerValue(), op);
////        }
////
////        // fixed length numerics
////        if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
////            // non-BigDecimal floating points
////            if (dte1 == DTE.XSDFloat || dte2 == DTE.XSDFloat)
////                return numericalMath(num1.floatValue(), num2.floatValue(), op);
////            else
////                return numericalMath(num1.doubleValue(), num2.doubleValue(), op);
////        } else {
////            // non-BigInteger integers
////            if (dte1 == DTE.XSDInt && dte2 == DTE.XSDInt)
////                return numericalMath(num1.intValue(), num2.intValue(), op);
////            else
////                return numericalMath(num1.longValue(), num2.longValue(), op);
////        }
//
//    }

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
    public final static NumericIV numericalFunc(final Literal lit, final NumericOp op) {

    	if (!checkNumericDatatype(lit))
    		throw new IllegalArgumentException("not numeric: " + lit);

    	final URI dte1 = lit.getDatatype();

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

        if (dte1.equals(XMLSchema.DECIMAL)) {
            return numericalFunc(lit.decimalValue(), op);
        } else if (dte1.equals(XMLSchema.INTEGER)) {
            return numericalFunc(lit.integerValue(), op);
        } else if (dte1.equals(XMLSchema.FLOAT)) {
            return numericalFunc(lit.floatValue(), op);
        } else if (dte1.equals(XMLSchema.INT)) {
            return numericalFunc(lit.intValue(), op);
        } else if (dte1.equals(XMLSchema.DOUBLE)) {
            return numericalFunc(lit.doubleValue(), op);
        } else {
            return numericalFunc(lit.longValue(), op);
        }
    }

    @Deprecated
    private static final NumericIV numericalFunc(final BigDecimal left, final NumericOp op) {
        switch(op) {
        case ABS:
            return new XSDDecimalIV(left.abs());
        case CEIL:
            return new XSDDecimalIV(new BigDecimal(Math.round(Math.ceil(left.doubleValue()))));
        case FLOOR:
            return new XSDDecimalIV(new BigDecimal(Math.round(Math.floor(left.doubleValue()))));
        case ROUND:
            return new XSDDecimalIV(new BigDecimal(Math.round(left.doubleValue())));
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Deprecated
    private static final NumericIV numericalFunc(final BigInteger left, final NumericOp op) {
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
    private static final NumericIV numericalFunc(final float left, final NumericOp op) {
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
    private static final NumericIV numericalFunc(final int left, final NumericOp op) {
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
    private static final NumericIV numericalFunc(final long left, final NumericOp op) {
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
    private static final NumericIV numericalFunc(final double left, final NumericOp op) {
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

    private static final NumericIV numericalMath(final BigDecimal left,
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

    private static final NumericIV numericalMath(final BigInteger left,
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

    private static final NumericIV numericalMath(final float left,
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

    private static final NumericIV numericalMath(final double left,
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

}
