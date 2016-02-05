package com.bigdata.rdf.internal.impl.uri;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractInlineExtensionIV;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Class provides support for fully inline {@link URI}s for which a
 * {@link Vocabulary} item was registered for the {@link URI} <em>namespace</em>
 * . An {@link URIExtensionIV} <strong>always</strong> has the <em>inline</em>
 * and <em>extension</em> bits set. {@link URIExtensionIV} are fully inline
 * since the <code>namespace</code> can be materialized from the
 * {@link Vocabulary} and the <code>localName</code> is directly inline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <V>
 */
public class URIExtensionIV<V extends BigdataURI> 
    	extends AbstractInlineExtensionIV<V, Object> 
		implements URI { 

    /**
     * 
     */
    private static final long serialVersionUID = 8267554196603121194L;

    /**
     * The namespace.
     */
    private final AbstractInlineIV<BigdataURI, ?> namespaceIV;

    /**
     * The localName.
     */
    private final AbstractLiteralIV<BigdataLiteral, ?> delegateIV;

    /**
     * {@inheritDoc}
     * <p>
     * Note: The extensionIV and delegateIV are NOT cloned. The rationale is
     * that we are only cloning to break the hard reference from the {@link IV}
     * to to cached value. If that needs to be done for the extensionIV and
     * delegateIV, then it will be done separately for those objects when they
     * are inserted into the termsCache.
     */
    @Override
    public IV<V, Object> clone(final boolean clearCache) {

        final URIExtensionIV<V> tmp = new URIExtensionIV<V>(delegateIV,
                namespaceIV);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    /**
     * 
     * @param delegateIV
     *            The {@link IV} which represents the localName.
     * @param namespaceIV
     *            The {@link IV} which represents the namespace. This MUST be a
     *            fully inline {@link IV} declared by the {@link Vocabulary}.
     */
    @SuppressWarnings("unchecked")
    public URIExtensionIV(
    		final AbstractLiteralIV<BigdataLiteral, ?> delegateIV, 
    		final IV<?,?> namespaceIV) {
        
        super(VTE.URI, true/* extension */, delegateIV.getDTE());

        if (namespaceIV == null)
            throw new IllegalArgumentException();

        if (!namespaceIV.isInline())
            throw new IllegalArgumentException();

        this.delegateIV = delegateIV;

        this.namespaceIV = (AbstractInlineIV<BigdataURI, ?>) namespaceIV;

    }
    
    /**
     * The namespace IV does need materialization, although it will not need
     * to go to the index to get the value (it just needs access to the lexicon's
     * vocabulary).
     */
    @Override
    public boolean needsMaterialization() {
    	return delegateIV.needsMaterialization() 
    	            || namespaceIV.needsMaterialization();
    }
    
    public AbstractLiteralIV<BigdataLiteral, ?> getLocalNameIV() {
        return delegateIV;
    }
    
    @Override
    public Object getInlineValue() { // TODO TEST
        return new URIImpl(stringValue());
    }
    
    /**
     * Extension IV is the <code>namespace</code> for the {@link URI}.
     */
    @Override
    public IV<BigdataURI, ?> getExtensionIV() {
        return namespaceIV;
    }
    
    /**
     * 
     */
    public int hashCode() {// TODO Inspect distribution.
        return namespaceIV.hashCode() ^ delegateIV.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof URIExtensionIV<?>) {
            return this.delegateIV.equals(((URIExtensionIV<?>) o).delegateIV)
                    && this.namespaceIV.equals(((URIExtensionIV<?>) o).namespaceIV);
        }
        return false;
    }
    
    /*
     * See BLZG-1591. Note that namespaceIV is not being materialized
     * separately. This fix does not change that.  It instead it uses
     * the cached value directly.
     */
    @Override
    public String toString() {
    	if (this.namespaceIV != null && this.delegateIV != null )
    		return this.namespaceIV.toString() + ":" + this.delegateIV.toString();
    	else 
    		return getValue().stringValue();
    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public int _compareTo(final IV o) {

        int ret = namespaceIV.compareTo(((URIExtensionIV<?>) o).namespaceIV);

        if (ret != 0)
            return ret;

        return delegateIV._compareTo(((URIExtensionIV<?>) o).delegateIV);

    }

    /**
     * Return the length of the namespace IV plus the length of the localName
     * IV.
     */
    @Override
    public int byteLength() {

        return 1/* flags */+ namespaceIV.byteLength() + delegateIV.byteLength();
        
    }

	/**
	 * Defer to the {@link ILexiconConfiguration} which has specific knowledge
	 * of how to generate an RDF value from this general purpose extension IV.
	 * <p>
	 * {@inheritDoc}
	 */
    @Override
	@SuppressWarnings( "unchecked" )
	public V asValue(final LexiconRelation lex) {

		V v = getValueCache();
		
		if (v == null) {
			
			final BigdataValueFactory f = lex.getValueFactory();
			
//			final ILexiconConfiguration config = lex.getLexiconConfiguration();
//
//            v = setValue((V) config.asValueFromVocab(this));

			final URI namespace = namespaceIV.asValue(lex);
			
			final String localName = lex.getLexiconConfiguration()
					.getInlineURILocalNameFromDelegate(namespace, delegateIV);
			
			v = setValue((V) f.createURI(namespace.stringValue(), localName));
			
			v.setIV(this);

		}

		return v;
		
    }

	////////////////////////
	// OpenRDF URI methods
	////////////////////////
	
	@Override
	public String stringValue() {
        return getNamespace() + getLocalName();
	}

    @Override
    public String getNamespace() {
        return namespaceIV.getValue().stringValue();
    }

    @Override
    public String getLocalName() {
        return delegateIV.getInlineValue().toString();
    }
    
}
