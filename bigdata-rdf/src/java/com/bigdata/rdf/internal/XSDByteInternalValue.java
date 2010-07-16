package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:byte</code>. */
public class XSDByteInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Byte> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final byte value;

    public XSDByteInternalValue(final byte value) {
        
        super(DTE.XSDByte);
        
        this.value = value;
        
    }

    final public Byte getInlineValue() {
        
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
        return value;
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
        return (short) value;
    }
    
    @Override
    public String stringValue() {
        return Byte.toString(value);
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
        if(o instanceof XSDByteInternalValue<?>) {
            return this.value == ((XSDByteInternalValue<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the byte value.
     * 
     * @see Byte#hashCode()
     */
    public int hashCode() {
        return (int) value;
    }

    public int byteLength() {
        return 1 + 1;
    }

    @Override
    protected int _compareTo(IV o) {
         
        final byte value2 = ((XSDByteInternalValue) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }
    
}