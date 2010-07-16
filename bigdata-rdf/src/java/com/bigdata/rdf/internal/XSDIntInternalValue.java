package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:int</code>. */
public class XSDIntInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Integer> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final int value;

    public XSDIntInternalValue(final int value) {
        
        super(DTE.XSDInt);
        
        this.value = value;
        
    }

    final public Integer getInlineValue() {
        
        return value;
        
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        final V v = (V) f.createLiteral(value);
        v.setIV(this);
        return v;
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
        return value;
    }

    @Override
    public short shortValue() {
        return (short) value;
    }
    
    @Override
    public String stringValue() {
        return Integer.toString(value);
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
        if(o instanceof XSDIntInternalValue<?>) {
            return this.value == ((XSDIntInternalValue<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the int value.
     * 
     * @see Integer#hashCode()
     */
    public int hashCode() {
        return value;
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_INT;
    }

    @Override
    protected int _compareTo(IV o) {
         
        final int value2 = ((XSDIntInternalValue) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    
}