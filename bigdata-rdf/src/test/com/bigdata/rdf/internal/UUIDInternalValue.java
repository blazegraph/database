package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Implementation for inline {@link UUID}s (there is no corresponding XML
 * Schema Datatype).
 */
public class UUIDInternalValue<V extends BigdataLiteral> extends
        AbstractDatatypeLiteralInternalValue<V, UUID> {
    
    private final UUID value;

    public UUIDInternalValue(final UUID value) {
        
        super(InternalDataTypeEnum.UUID);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public UUID getInlineValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f) {
        return (V) f.createLiteral(value.toString(), //
                f.createURI(InternalDataTypeEnum.UUID.getDatatype()));
    }

    @Override
    final public long longValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean booleanValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float floatValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int intValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short shortValue() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String stringValue() {
        return value.toString();
    }

    @Override
    public BigDecimal decimalValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigInteger integerValue() {
        throw new UnsupportedOperationException();
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof UUIDInternalValue) {
            return this.value.equals(((UUIDInternalValue) o).value);
        }
        return false;
    }
    
    /**
     * Return the hash code of the {@link UUID}.
     */
    public int hashCode() {
        return value.hashCode();
    }

}