package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:short</code>. */
public class XSDShortInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Short> {

    private final short value;

    public XSDShortInternalValue(final short value) {
        
        super(DTE.XSDShort);
        
        this.value = value;
        
    }

    final public Short getInlineValue() {
        
        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        return (V) f.createLiteral(value);
    }

    @Override
    final public long longValue() {
        return (long) value;
    }

    @Override
    public boolean booleanValue() {
        return value == 0 ? false : true;
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public double doubleValue() {
        return (double) value;
    }

    @Override
    public float floatValue() {
        return (float) value;
    }

    @Override
    public int intValue() {
        return (int)value;
    }

    @Override
    public short shortValue() {
        return value;
    }
    
    @Override
    public String stringValue() {
        return Short.toString(value);
    }

    @Override
    public BigDecimal decimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public BigInteger integerValue() {
        return BigInteger.valueOf(value);
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDShortInternalValue) {
            return this.value == ((XSDShortInternalValue) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the short value.
     * 
     * @see Short#hashCode()
     */
    public int hashCode() {
        return (int) value;
    }

}