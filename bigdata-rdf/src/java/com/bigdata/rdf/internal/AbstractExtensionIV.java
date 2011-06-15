package com.bigdata.rdf.internal;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Class <strong>always</strong> has the <em>extension</em> bit set but is NOT
 * 100% "inline". An instance of this class bundles together an value of some
 * primitive data type declared by {@link DTE} with the "extension" {@link IV}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <V>
 * @param <T>
 */
abstract public class AbstractExtensionIV<V extends BigdataValue, T> extends
        AbstractNonInlineIV<V, T> {

    private final AbstractIV delegate;
    
    private final IV extensionIV;
    
    protected AbstractExtensionIV(final VTE vte,
            final AbstractIV delegate, final IV extensionIv) {

        super(vte, true/* extension */, delegate.getDTE());
        
        if (extensionIv == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
        this.extensionIV = extensionIv;
        
    }
    
    public AbstractIV getDelegate() {

        return delegate;
        
    }
    
    @Override
    public IV getExtensionIV() {

        return extensionIV;
        
    }
    
    final public int hashCode() {
        return delegate.hashCode();
    }

    final public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof AbstractExtensionIV<?, ?>) {
            return this.delegate
                    .equals(((AbstractExtensionIV<?, ?>) o).delegate)
                    && this.extensionIV
                            .equals(((AbstractExtensionIV<?, ?>) o).extensionIV);
        }
        return false;
    }

    final protected int _compareTo(final IV o) {

//        int ret = extensionIv._compareTo(((AbstractExtensionIV) o).extensionIv);

    	final int ret = extensionIV
				.compareTo(((AbstractExtensionIV) o).extensionIV);

        if (ret != 0)
            return ret;

        return delegate._compareTo(((AbstractExtensionIV) o).delegate);

    }

	/**
	 * Return one (for the flags) plus the length of the extension IV plus the
	 * length of the delegate IV.
	 */
    final public int byteLength() {

		return 1/* flags */+ extensionIV.byteLength() + delegate.byteLength();
        
    }

}
