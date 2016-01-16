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
package com.bigdata.rdf.internal.impl.literal;

import java.util.UUID;

import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.Bytes;

/**
 * Implementation for inline {@link UUID}s (there is no corresponding XML
 * Schema Datatype).
 */
public class UUIDLiteralIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, UUID> implements Literal {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 6411134650187983925L;
		
	private final UUID value;

    public IV<V, UUID> clone(final boolean clearCache) {

        final UUIDLiteralIV<V> tmp = new UUIDLiteralIV<V>(value);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public UUIDLiteralIV(final UUID value) {
        
        super(DTE.UUID);

        if (value == null)
            throw new IllegalArgumentException();
        
        this.value = value;
        
    }

    final public UUID getInlineValue() {
        return value;
    }

	@SuppressWarnings("unchecked")
	public V asValue(final LexiconRelation lex) {
	
		V v = getValueCache();
		
		if (v == null) {
			
			final ValueFactory f = lex.getValueFactory();
			
			v = (V) f.createLiteral(value.toString(), DTE.UUID.getDatatypeURI());
			
			v.setIV(this);
			
			setValue(v);
			
		}

		return v;
		
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof UUIDLiteralIV<?>) {
            return this.value.equals(((UUIDLiteralIV<?>) o).value);
        }
        return false;
    }
    
    /**
     * Return the hash code of the {@link UUID}.
     */
    public int hashCode() {
        return value.hashCode();
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_UUID;
    }

    @Override
    public int _compareTo(IV o) {
         
        return value.compareTo(((UUIDLiteralIV) o).value);
        
    }
    
    
}
