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
/*
 * Created on June 3rd, 2011
 */
package com.bigdata.rdf.internal;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * A fully inlined representation of a URI based on a <code>short</code> code.
 * The flags byte looks like: <code>VTE=URI, inline=true, extension=false,
 * DTE=XSDShort</code>. It is followed by an <code>unsigned short</code> value
 * which is the index of the URI in the {@link Vocabulary} class for the triple
 * store.
 * 
 * @author thompsonbry
 */
public class URIShortIV<V extends BigdataURI> extends AbstractInlineIV<V, Short> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	final private short value;

	short shortValue() {
		
		return value;
		
	}
	
	public URIShortIV(final short value) {

		super(VTE.URI, DTE.XSDShort);

		this.value = value;

	}

	@SuppressWarnings("unchecked")
	@Override
	protected int _compareTo(final IV o) {

		final short value2 = ((URIShortIV<BigdataURI>) o).value;

		return value == value2 ? 0 : value < value2 ? -1 : 1;

    }

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o instanceof URIShortIV<?>) {
			return this.value == ((URIShortIV<?>) o).value;
		}
        return false;
    }

    /**
     * Return the hash code of the short value.
     */
	final public int hashCode() {
		
		return (int) value;
		
	}

	public V asValue(final LexiconRelation lex)
			throws UnsupportedOperationException {

		/*
		 * FIXME Enable. Consider passing [IV] not [int].
		 * 
		 * FIXME We need a means to obtain the unsigned int from the byte for
		 * this operation. That will give us all 64k possible indices rather
		 * than just 32k.
		 */
//		return lex.getContainer().getVocabulary().get(value);
		
		throw new UnsupportedOperationException();
		
	}

	final public int byteLength() {

		return 3 /* flags(1) + short(2) */;
		
	}

	final public Short getInlineValue() {
	
		return value;
		
	}

	public String stringValue() {
		
		return "Vocab(" + Short.toString(value) + ")";
		
	}

}
