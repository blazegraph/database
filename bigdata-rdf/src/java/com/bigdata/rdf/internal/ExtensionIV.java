package com.bigdata.rdf.internal;

import org.openrdf.model.Literal;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Class provides support for datatype {@link Literal}s for which an
 * {@link IExtension} was registered. An {@link ExtensionIV}
 * <strong>always</strong> has the <em>inline</em> and <em>extension</em> bits
 * set. An instance of this class bundles together an inline value of some
 * primitive data type declared by {@link DTE} with the {@link IV} of the
 * datatype URI for the datatype literal. {@link ExtensionIV} are fully inline
 * since the datatype URI can be materialized by the {@link IExtension} while
 * {@link DTE} identifies the value space and the point in the value space is
 * directly inline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 */
public class ExtensionIV<V extends BigdataLiteral> 
    extends AbstractInlineIV<V, Object> { 

    /**
     * 
     */
    private static final long serialVersionUID = 8267554196603121194L;

    private final AbstractLiteralIV delegate;
    
    private final AbstractIV datatype;
    
    public ExtensionIV(final AbstractLiteralIV delegate, final IV datatype) {
        
        super(VTE.LITERAL, true, delegate.getDTE());
        
        if (datatype == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
        this.datatype = (AbstractIV) datatype;
        
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
    public IV getExtensionIV() {
        return datatype;
    }
    
    /**
     * Return the hash code of the long epoch value.
     */
    public int hashCode() {
        return delegate.hashCode();
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof ExtensionIV<?>) {
            return this.delegate.equals(((ExtensionIV<?>) o).delegate)
                    && this.datatype.equals(((ExtensionIV<?>) o).datatype);
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
     * Return the length of the datatype IV plus the length of the delegate IV.
     */
    public int byteLength() {

        return datatype.byteLength() + delegate.byteLength();
        
    }

	/**
	 * Defer to the {@link ILexiconConfiguration} which has specific knowledge
	 * of how to generate an RDF value from this general purpose extension IV.
	 * <p>
	 * {@inheritDoc}
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" })
	public V asValue(final LexiconRelation lex) {

		V v = getValueCache();
		
		if (v == null) {
			
			final BigdataValueFactory f = lex.getValueFactory();
			
			final ILexiconConfiguration config = lex.getLexiconConfiguration();

			v = setValue((V) config.asValue(this, f));

			v.setIV(this);

		}

		return v;
		
    }
    
}
