package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:integer</code>. */
public class XSDIntegerInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, BigInteger> {
    
    private final BigInteger value;

    public XSDIntegerInternalValue(final BigInteger value) {
        
        super(InternalDataTypeEnum.XSDInteger);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public BigInteger getInlineValue() {

        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        // @todo factory should cache the XSD URIs.
        return (V) f.createLiteral(value.toString(),//
                f.createURI(InternalDataTypeEnum.XSDInteger.getDatatype()));
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
        return new BigDecimal(value);
    }

    @Override
    public BigInteger integerValue() {
        return value;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof XSDIntegerInternalValue) {
            return this.value.equals(((XSDIntegerInternalValue) o).value);
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