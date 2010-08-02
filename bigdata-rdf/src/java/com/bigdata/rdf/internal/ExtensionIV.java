package com.bigdata.rdf.internal;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

public class ExtensionIV<V extends BigdataLiteral> 
    extends AbstractInlineIV<V, Object> { 

    /**
     * 
     */
    private static final long serialVersionUID = 8267554196603121194L;

    private final AbstractLiteralIV delegate;
    
    private final TermId datatype;
    
    public ExtensionIV(final AbstractLiteralIV delegate, 
            final TermId datatype) {
        super(VTE.LITERAL, true, delegate.getDTE());
        
        this.delegate = delegate;
        this.datatype = datatype;
    }
    
    public AbstractLiteralIV getDelegate() {
        return delegate;
    }
    
    @Override
    public String stringValue() {
        return delegate.stringValue();
    }
    
    public Object getInlineValue() {
        return delegate.getInlineValue();
    }
    
    @Override
    public TermId getExtensionDatatype() {
        return datatype;
    }
    
    /**
     * Return the hash code of the long epoch value.
     */
    public int hashCode() {
        return delegate.hashCode();
    }

    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof ExtensionIV) {
            return this.delegate.equals(((ExtensionIV) o).delegate) &&
                this.datatype.equals(((ExtensionIV) o).datatype);
        }
        return false;
    }
    
    protected int _compareTo(final IV o) {
        
        int ret = datatype._compareTo(((ExtensionIV) o).datatype);
        
        if (ret != 0)
            return ret;
        
        return delegate._compareTo(((ExtensionIV) o).delegate);
        
    }
    
    /**
     * Return the normal length of the delegate plus 8 bytes for the term ID
     * of the extension datatype.
     */
    public int byteLength() {
        return delegate.byteLength() + Bytes.SIZEOF_LONG;
    }
    
    /**
     * Defer to the {@link ILexiconConfiguration} which has specific knowledge
     * of how to generate an RDF value from this general purpose extension IV.
     */
    @SuppressWarnings("unchecked")
    public V asValue(final BigdataValueFactory f, 
            final ILexiconConfiguration config)
            throws UnsupportedOperationException {
        return (V) config.asValue(this, f);
    }
    
}
