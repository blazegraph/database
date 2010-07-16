package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/** Implementation for inline <code>xsd:float</code>. */
public class XSDFloatInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, Float> {

    /**
     * 
     */
    private static final long serialVersionUID = 2274203835967555711L;

    private final float value;

    public XSDFloatInternalValue(final float value) {
        
        super(DTE.XSDFloat);
        
        this.value = value;
        
    }

    final public Float getInlineValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        final V v = (V) f.createLiteral(value);
        v.setIV(this);
        return v;
    }

    @Override
    final public float floatValue() {
        return value;
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
        return (double) value;
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
        return Float.toString(value);
    }
    
    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDFloatInternalValue<?>) {
            return this.value == ((XSDFloatInternalValue<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the float value.
     * 
     * @see Float#hashCode()
     */
    public int hashCode() {

        return Float.floatToIntBits(value);
        
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_FLOAT;
    }
    
    @Override
    protected int _compareTo(IV o) {
         
        final float value2 = ((XSDFloatInternalValue) o).value;
        
        return value == value2 ? 0 : value < value2 ? -1 : 1;
        
    }


}