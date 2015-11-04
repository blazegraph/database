package com.bigdata.rdf.internal.impl.literal;

import java.util.Arrays;

import org.openrdf.model.Literal;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.InlineLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.BytesUtil;

public class LiteralArrayIV extends AbstractLiteralIV<BigdataLiteral, Object[]> {

    private static final long serialVersionUID = 9136542087440805253L;

    /**
     * The inline literal array.
     */
    private final InlineLiteralIV<?,?>[] ivs;
    
    private transient byte[] key;
    
    private transient int hashCode = 0;
    
    public LiteralArrayIV(final InlineLiteralIV<?,?>... ivs) {
        super(DTE.Extension);
        
        // only using one byte for the array length
        if (ivs.length >= Byte.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        
        this.ivs = ivs;
    }
    
    @Override 
    public DTEExtension getDTEX() {
        return DTEExtension.ARRAY;
    }
    
    public InlineLiteralIV<?,?>[] getIVs() {
        return ivs;
    }
    
    private byte[] key() {
        if (key == null) {
            key = super.encode(KeyBuilder.newInstance()).getKey();
        }
        return key;
    }
    
    @Override
    public int byteLength() {
        return key().length;
    }

    @Override
    public LiteralArrayIV clone(boolean clearCache) {
        return new LiteralArrayIV(this.ivs);
    }

    @Override
    public int _compareTo(IV o) {
        if (!(o instanceof LiteralArrayIV)) {
            throw new IllegalArgumentException();
        }
        final LiteralArrayIV iv = (LiteralArrayIV) o;
        return BytesUtil.compareBytes(key(), iv.key());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(ivs);
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LiteralArrayIV other = (LiteralArrayIV) obj;
        if (!Arrays.equals(ivs, other.ivs))
            return false;
        return true;
    }
    
    /**
     * Implement {@link Literal#getLabel()}.
     */
    @Override
    public String getLabel() {
        return "LiteralArrayIV["+ivs.length+"]";
    }

    @Override
    public Object[] getInlineValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * This IV cannot be materialized on its own.  It can only be used
     * within the context of a {@link URIExtensionIV} or 
     * {@link LiteralExtensionIV} as the delegate in cases where the extension
     * mechanism needs an array of inline IVs to represent its URI or Literal
     * respectively.
     */
    @Override
    public BigdataLiteral asValue(LexiconRelation lex) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

}
