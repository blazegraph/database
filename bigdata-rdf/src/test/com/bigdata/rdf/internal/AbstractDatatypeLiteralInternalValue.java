package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Abstract base class for RDF datatype literals adds primitive data type
 * value access methods.
 * <p>
 * {@inheritDoc}
 * 
 * @todo What are the SPARQL semantics for casting among these datatypes?
 *       They should probably be reflected here since that is the real use
 *       case. I believe that those casts also require failing a solution if
 *       the cast is not legal, in which case these methods might not be all
 *       that useful.
 *       <p>
 *       Also see BigdataLiteralImpl and XMLDatatypeUtil. It handles the
 *       conversions by reparsing, but there is no reason to do that here
 *       since we have the canonical point in the value space.
 * 
 * @see http://www.w3.org/TR/rdf-sparql-query/#FunctionMapping, The casting
 *      rules for SPARQL
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 */
abstract public class AbstractDatatypeLiteralInternalValue<V extends BigdataLiteral, T>
        extends AbstractLiteralInternalValue<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = 5962615541158537189L;

    protected AbstractDatatypeLiteralInternalValue(final InternalDataTypeEnum dte) {

        super(dte);

    }

    final public boolean isInline() {
        return true;
    }

    final public boolean isTermId() {
        return false;
    }

    final public boolean isNull() {
        return false;
    }

    final public long getTermId() {
        throw new UnsupportedOperationException();
    }

    /** Return the <code>boolean</code> value of <i>this</i> value. */
    abstract public boolean booleanValue();

    /**
     * Return the <code>byte</code> value of <i>this</i> value.
     * <p>
     * Note: Java lacks unsigned data types. For safety, operations on
     * unsigned XSD data types should be conducted after a widening
     * conversion. For example, operations on <code>xsd:unsignedByte</code>
     * should be performed using {@link #shortValue()}.
     */
    abstract public byte byteValue();

    /**
     * Return the <code>short</code> value of <i>this</i> value.
     * <p>
     * Note: Java lacks unsigned data types. For safety, operations on
     * unsigned XSD data types should be conducted after a widening
     * conversion. For example, operations on <code>xsd:unsignedShort</code>
     * should be performed using {@link #intValue()}.
     */
    abstract public short shortValue();

    /**
     * Return the <code>int</code> value of <i>this</i> value.
     * <p>
     * Note: Java lacks unsigned data types. For safety, operations on
     * unsigned XSD data types should be conducted after a widening
     * conversion. For example, operations on <code>xsd:unsignedInt</code>
     * should be performed using {@link #longValue()}.
     */
    abstract public int intValue();

    /**
     * Return the <code>long</code> value of <i>this</i> value.
     * <p>
     * Note: Java lacks unsigned data types. For safety, operations on
     * unsigned XSD data types should be conducted after a widening
     * conversion. For example, operations on <code>xsd:unsignedLong</code>
     * should be performed using {@link #integerValue()}.
     */
    abstract public long longValue();

    /** Return the <code>float</code> value of <i>this</i> value. */
    abstract public float floatValue();

    /** Return the <code>double</code> value of <i>this</i> value. */
    abstract public double doubleValue();

    /** Return the {@link BigInteger} value of <i>this</i> value. */
    abstract public BigInteger integerValue();

    /** Return the {@link BigDecimal} value of <i>this</i> value. */
    abstract public BigDecimal decimalValue();

}