package com.bigdata.rdf.internal;

import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataLiteral;

/**
 * A {@link Literal} modeled as a datatype {@link IV} plus an inline Unicode
 * <code>label</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <V>
 */
public class LiteralDatatypeIV<V extends BigdataLiteral> extends
        AbstractExtensionIV<V, Literal> {

	public LiteralDatatypeIV(final AbstractIV delegate, final IV datatype) {

        super(VTE.LITERAL, delegate, datatype);

    }

    /**
     * Human readable representation includes the datatype {@link IV} and
     * the <code>label</code>.
     */
    public String toString() {

        return "Literal(namespace" + getExtensionIV()
                + String.valueOf(getVTE().getCharCode()) + ", localName="
                + getDelegate() + ")";

    }

    public long getTermId() throws UnsupportedOperationException {

        throw new UnsupportedOperationException();
        
    }

}
