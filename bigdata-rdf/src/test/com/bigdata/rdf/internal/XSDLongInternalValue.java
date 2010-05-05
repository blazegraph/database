package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:long</code>. */
public class XSDLongInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Long> {

    /**
     * 
     */
    private static final long serialVersionUID = -6972910330385311194L;
    
    private final long value;

    public XSDLongInternalValue(final long value) {
        
        super(InternalDataTypeEnum.XSDLong);
        
        this.value = value;
        
    }

    final public Long getInlineValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        return (V) f.createLiteral(value);
    }

    @Override
    final public long longValue() {
        return value;
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
        return (int) value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }
    
    @Override
    public String stringValue() {
        return Long.toString(value);
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
        if(o instanceof XSDLongInternalValue) {
            return this.value == ((XSDLongInternalValue) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the long value.
     */
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

}