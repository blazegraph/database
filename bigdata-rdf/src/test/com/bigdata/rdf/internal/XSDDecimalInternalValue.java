package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:integer</code>. */
public class XSDDecimalInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, BigDecimal> {
    
    private final BigDecimal value;

    public XSDDecimalInternalValue(final BigDecimal value) {
        
        super(DTE.XSDDecimal);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public BigDecimal getInlineValue() {

        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        // @todo factory should cache the XSD URIs.
        return (V) f.createLiteral(value.toString(),//
                f.createURI(DTE.XSDInteger.getDatatype()));
    }

    @Override
    final public long longValue() {
        return value.longValue();
    }

    @Override
    public boolean booleanValue() {
        return value.equals(BigInteger.ZERO) ? false : true;
    }

    @Override
    public byte byteValue() {
        return value.byteValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public float floatValue() {
        return value.floatValue();
    }

    @Override
    public int intValue() {
        return value.intValue();
    }

    @Override
    public short shortValue() {
        return value.shortValue();
    }
    
    @Override
    public String stringValue() {
        return value.toString();
    }

    @Override
    public BigDecimal decimalValue() {
        return value;
    }

    @Override
    public BigInteger integerValue() {
        return value.toBigInteger();
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof XSDDecimalInternalValue) {
            return this.value.equals(((XSDDecimalInternalValue) o).value);
        }
        return false;
    }

    /**
     * Return the hash code of the {@link BigInteger}.
     */
    public int hashCode() {
        return value.hashCode();
    }

}