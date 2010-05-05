package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:double</code>. */
public class XSDDoubleInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Double> {

    private final double value;

    public XSDDoubleInternalValue(final double value) {
        
        super(InternalDataTypeEnum.XSDDouble);
        
        this.value = value;
        
    }

    final public Double getInlineValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        return (V) f.createLiteral(value);
    }

    @Override
    final public float floatValue() {
        return (float) value;
    }

    @Override
    public boolean booleanValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf((long) value);
    }

    @Override
    public String stringValue() {
        return Double.toString(value);
    }
    
    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDDoubleInternalValue) {
            return this.value == ((XSDDoubleInternalValue) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the double value.
     * 
     * @see Double#hashCode()
     */
    public int hashCode() {

        final long t = Double.doubleToLongBits(value);

        return (int) (t ^ (t >>> 32));

    }

}