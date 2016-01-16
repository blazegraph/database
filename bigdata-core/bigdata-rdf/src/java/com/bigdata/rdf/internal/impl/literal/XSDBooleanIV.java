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

import org.openrdf.model.Literal;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;

/** Implementation for inline <code>xsd:boolean</code>. */
public class XSDBooleanIV<V extends BigdataLiteral> extends
        AbstractLiteralIV<V, Boolean> implements Literal {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    static public transient final XSDBooleanIV<BigdataLiteral> TRUE = 
    	new XSDBooleanIV<BigdataLiteral>(true);

    static public transient final XSDBooleanIV<BigdataLiteral> FALSE = 
    	new XSDBooleanIV<BigdataLiteral>(false);
    
    static public final XSDBooleanIV<BigdataLiteral> valueOf(final boolean b) {
    	return b ? TRUE : FALSE;
    }
    
    private final boolean value;

    @Override
    public IV<V, Boolean> clone(final boolean clearCache) {

        final XSDBooleanIV<V> tmp = new XSDBooleanIV<V>(value);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    public XSDBooleanIV(final boolean value) {
        
        super(DTE.XSDBoolean);
        
        this.value = value;
        
    }

    @Override
    final public Boolean getInlineValue() {

        return value ? Boolean.TRUE : Boolean.FALSE;

    }

    @Override
    @SuppressWarnings("unchecked")
    public V asValue(final LexiconRelation lex) {

    	V v = getValueCache();
    	
    	if( v == null) {
    		
    		v = (V) lex.getValueFactory().createLiteral(value);
    		
    		v.setIV(this);
    		
    		setValue(v);
    		
    	}

    	return v;
    	
    }

    /**
     * Override {@link Literal#booleanValue()}.
     */
    @Override
    public boolean booleanValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if(this==o) return true;
        if(o instanceof XSDBooleanIV<?>) {
            return this.value == ((XSDBooleanIV<?>) o).value;
        }
        return false;
    }
    
    /**
     * Return the hash code of the byte value.
     * 
     * @see Boolean#hashCode()
     */
    @Override
    public int hashCode() {
        return value ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }

    @Override
    public int byteLength() {
        return 1 + 1;
    }

    @Override
    public int _compareTo(IV o) {
         
        final boolean v = ((XSDBooleanIV) o).value;
        
        return (v == value ? 0 : (value ? 1 : -1));
        
    }

}
