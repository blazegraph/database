/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.internal;

import java.io.IOException;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for {@link IV}s which CAN NOT be fully materialized
 * from their inline representation.
 */
abstract public class AbstractNonInlineIV<V extends BigdataValue, T> extends
        AbstractIV<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    protected AbstractNonInlineIV(final byte flags) {

        super(flags);

    }

    protected AbstractNonInlineIV(final VTE vte, final boolean extension,
            final DTE dte) {

        super(vte, false/* inline */, extension, dte);

    }
    
    /**
     * Callers must explicitly populate the value cache.
     * <p>
     * {@inheritDoc}
     */
    @Override
    final public V setValue(V v) {
    	
    	return super.setValue(v);
    	
    }
    
    /**
     * Operation is not supported because this {@link IV} type is not 100%
     * inline. You MUST explicitly set the value cache.
     * <p>
     * {@inheritDoc}
     * 
     * @see #setValue(BigdataValue)
     */
    final public V asValue(final LexiconRelation lex) {
    
        throw new UnsupportedOperationException();
        
    }

    /**
     * Operation is not supported because this {@link IV} type is not 100%
     * inline.
     */
    final public T getInlineValue() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Always returns <code>false</code> since the RDF value is not inline.
     */
    @Override
    final public boolean isInline() {

        return false;
        
    }

    /**
     * Override default serialization to send the cached {@link BigdataValue}.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.defaultWriteObject();

        out.writeObject(getValueCache());

    }

    /**
     * Override default serialization to recover the cached {@link BigdataValue}
     * .
     */
    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        in.defaultReadObject();

        final V v = (V) in.readObject();

        if (v != null) {
            // set the value cache.
            setValue(v);
        }

    }

}
