package com.bigdata.rdf.internal;

import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataURI;

/**
 * A {@link URI} modeled as a namespace {@link IV} plus an inline Unicode
 * <code>localName</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 */
public class URINamespaceIV<V extends BigdataURI> extends
        AbstractExtensionIV<V, URI> {

    public URINamespaceIV(final AbstractLiteralIV delegate,
            final TermId datatype) {

        super(VTE.URI, delegate, datatype);

    }

    /**
     * Human readable representation includes the namespace {@link IV} and the
     * <code>localName</code>.
     */
    public String toString() {

        return "URI(namespace" + getExtensionIV()
                + String.valueOf(getVTE().getCharCode()) + ", localName="
                + getDelegate() + ")";

    }

    public long getTermId() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

}
