/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.rdf.internal.impl.uri;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IInlineUnicode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUnicode;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Implementation for inline {@link URI}s. All information is inlined. This
 * class is mainly targeted at inlining at fully inlining URIs in scale-out
 * (which can be an attractive option).
 */
public class FullyInlineURIIV<V extends BigdataURI> extends AbstractInlineIV<V, URI>
        implements IInlineUnicode, URI {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final URI uri;
    
    /** The cached byte length of this {@link IV}. */
    private transient int byteLength = 0;

    public IV<V, URI> clone(final boolean clearCache) {

        final FullyInlineURIIV<V> tmp = new FullyInlineURIIV<V>(uri);
        
        // propagate transient state if available.
        tmp.byteLength = byteLength;

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public FullyInlineURIIV(final URI uri) {

        this(uri, 0/* byteLength */);

    }
    
    public FullyInlineURIIV(final URI uri, final int byteLength) {

        super(VTE.URI, DTE.XSDString);

        if (uri == null)
            throw new IllegalArgumentException();

        this.uri = uri;

        this.byteLength = byteLength;
        
    }

    final public URI getInlineValue() {

        return uri;
        
    }

    final public String toString() {

        return uri.stringValue();
        
    }
 
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {
		V v = getValueCache();
		if (v == null) {
            final BigdataValueFactory f = lex.getValueFactory();
            v = (V) f.createURI(uri.stringValue());
            v.setIV(this);
			setValue(v);
		}
		return v;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof FullyInlineURIIV<?>) {
            return uri.stringValue().equals(((FullyInlineURIIV<?>) o).stringValue());
        }
        return false;
    }

    /**
     * Return the hash code of the {@link URI}'s string value (per the openrdf
     * API).
     */
    public int hashCode() {

        return uri.stringValue().hashCode();
        
    }

    public int byteLength() {
        
        if (byteLength == 0) {

            // Cache the byteLength if not yet set.

            byteLength = 1 // flags
                    + IVUnicode.byteLengthUnicode(uri.stringValue())//
                    ;
        }

        return byteLength;
        
    }

    final public void setByteLength(final int byteLength) {

        if (byteLength < 0)
            throw new IllegalArgumentException();
        
        if (this.byteLength != 0 && this.byteLength != byteLength)
            throw new IllegalStateException();
        
        this.byteLength = byteLength;
        
    }

    @Override
    public int _compareTo(final IV o) {

        final FullyInlineURIIV<?> t = (FullyInlineURIIV<?>) o;

        return IVUnicode.IVUnicodeComparator.INSTANCE.compare(
                uri.stringValue(), t.uri.stringValue());
        
//        return uri.stringValue().compareTo(id2);
//        return id == id2 ? 0 : id < id2 ? -1 : 1;
        
    }
    
	/**
	 * Because we this is a fully inlined URI, we do not need the
	 * materialized URI to answer the URI interface methods.
	 */
	@Override
	public boolean needsMaterialization() {
		
		return false;
		
	}

	/**
	 * Implements {@link Value#stringValue()}.
	 */
	@Override
	public String stringValue() {
		
		return uri.stringValue();
		
	}

	/**
	 * Implements {@link URI#getLocalName()}.
	 */
	@Override
	public String getLocalName() {
		
		return uri.getLocalName();
		
	}

	/**
	 * Implements {@link URI#getNamespace()}.
	 */
	@Override
	public String getNamespace() {
		
		return uri.getNamespace();
		
	}

}
