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

import org.openrdf.model.URI;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Implementation for inline {@link URI}s. All information is inlined. This
 * class is mainly targeted at inlining at fully inlining URIs in scale-out
 * (which can be an attractive option).
 */
public class InlineURIIV<V extends BigdataURI> extends AbstractInlineIV<V, URI>
        implements IInlineUnicode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final URI uri;
    
    /** The cached byte length of this {@link IV}. */
    private transient int byteLength = 0;

    public InlineURIIV(final URI uri) {

        this(uri, 0/* byteLength */);

    }
    
    public InlineURIIV(final URI uri, final int byteLength) {

        super(VTE.URI, DTE.XSDString);

        if (uri == null)
            throw new IllegalArgumentException();

        this.uri = uri;

        this.byteLength = byteLength;
        
    }

    final public URI getInlineValue() {

        return uri;
        
    }

    final public String stringValue() {

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
        if (o instanceof InlineURIIV<?>) {
            return uri.stringValue().equals(((InlineURIIV<?>) o).stringValue());
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
                    + IVUtility.byteLengthUnicode(uri.stringValue())//
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
    protected int _compareTo(final IV o) {

        final String id2 = ((InlineURIIV<?>) o).uri.stringValue();
        
        return uri.stringValue().compareTo(id2);
//        return id == id2 ? 0 : id < id2 ? -1 : 1;
        
    }

}
