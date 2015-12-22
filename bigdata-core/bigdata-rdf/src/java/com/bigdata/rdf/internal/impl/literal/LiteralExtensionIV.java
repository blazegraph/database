package com.bigdata.rdf.internal.impl.literal;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.InlineLiteralIV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.AbstractInlineExtensionIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;

/**
 * Class provides support for datatype {@link Literal}s for which an
 * {@link IExtension} was registered. An {@link LiteralExtensionIV}
 * <strong>always</strong> has the <em>inline</em> and <em>extension</em> bits
 * set. An instance of this class bundles together an inline value of some
 * primitive data type declared by {@link DTE} with the {@link IV} of the
 * datatype URI for the datatype literal. {@link LiteralExtensionIV} are fully inline
 * since the datatype URI can be materialized by the {@link IExtension} while
 * {@link DTE} identifies the value space and the point in the value space is
 * directly inline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 */
public class LiteralExtensionIV<V extends BigdataLiteral> 
    	extends AbstractInlineExtensionIV<V, Object> 
		implements Literal, InlineLiteralIV<V, Object> { 

    /**
     * 
     */
    private static final long serialVersionUID = 8267554196603121194L;
    
    private final AbstractLiteralIV<BigdataLiteral, ?> delegate;
    
    private final IV<BigdataURI, ?> datatype;

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

        final LiteralExtensionIV<V> tmp = new LiteralExtensionIV<V>(delegate,
                datatype);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public LiteralExtensionIV(
    		final AbstractLiteralIV<BigdataLiteral, ?> delegate, 
    		final IV<BigdataURI, ?> datatype) {
        
        super(VTE.LITERAL, true/*extension*/, delegate.getDTE());
        
        if (datatype == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
        this.datatype = (AbstractIV<BigdataURI, ?>) datatype;
        
    }
    
    /**
     * Even though Literal extension IVs are fully inline (no data in the 
     * lexicon indices), we do need materialization to answer the openrdf
     * Literal interface correctly. We cannot properly interpret what the
     * delegate IV means without the materialized value.
     */
    @Override
    public boolean needsMaterialization() {
    	return true;
    }
    
    public AbstractLiteralIV<BigdataLiteral, ?> getDelegate() {
        return delegate;
    }
    
    @Override
    public Object getInlineValue() {
        return delegate.getInlineValue();
    }

    @Override
    public DTEExtension getDTEX() {
        return delegate.getDTEX();
    }
    
    /**
     * Extension IV is the datatype for this literal.
     */
    @Override
    public IV<BigdataURI, ?> getExtensionIV() {
        return datatype;
    }
    
    /**
     * Return the hash code of the long epoch value.
     */
    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof LiteralExtensionIV<?>) {
            return this.delegate.equals(((LiteralExtensionIV<?>) o).delegate)
                    && this.datatype.equals(((LiteralExtensionIV<?>) o).datatype);
        }
        return false;
    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public int _compareTo(final IV o) {

        int ret = datatype.compareTo(((LiteralExtensionIV<?>) o).datatype);

        if (ret != 0)
            return ret;

        return delegate._compareTo(((LiteralExtensionIV<?>) o).delegate);

    }

    /**
     * Return the length of the datatype IV plus the length of the delegate IV.
     */
    @Override
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
			
//			final BigdataValueFactory f = lex.getValueFactory();
			
			final ILexiconConfiguration config = lex.getLexiconConfiguration();

			v = setValue((V) config.asValue(this));//, f));

			v.setIV(this);

		}

		return v;
		
    }

	@Override
	public String stringValue() {
		return getValue().stringValue();
	}

	@Override
	public boolean booleanValue() {
		return getValue().booleanValue();
	}

	@Override
	public byte byteValue() {
		return getValue().byteValue();
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return getValue().calendarValue();
	}

	@Override
	public BigDecimal decimalValue() {
		return getValue().decimalValue();
	}

	@Override
	public double doubleValue() {
		return getValue().doubleValue();
	}

	@Override
	public float floatValue() {
		return getValue().floatValue();
	}

	@Override
	public URI getDatatype() {
		return getValue().getDatatype();
	}

	@Override
	public String getLabel() {
		return getValue().getLabel();
	}

	@Override
	public String getLanguage() {
		return getValue().getLanguage();
	}

	@Override
	public int intValue() {
		return getValue().intValue();
	}

	@Override
	public BigInteger integerValue() {
		return getValue().integerValue();
	}

	@Override
	public long longValue() {
		return getValue().longValue();
	}

	@Override
	public short shortValue() {
		return getValue().shortValue();
	}

    @Override
    public String toString() {
        return "LiteralExtensionIV [delegate=" + delegate + ", datatype="
                + datatype + "]";
    }
    
}
