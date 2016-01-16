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
package com.bigdata.rdf.internal.impl.bnode;

import org.openrdf.model.BNode;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IInlineUnicode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUnicode;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class for inline RDF blank nodes. Blank nodes MUST use a "short" Unicode ID
 * to be inlined with this class, where "short" is the maximum length configured
 * for the lexicon.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see AbstractTripleStore.Options
 */
public class FullyInlineUnicodeBNodeIV<V extends BigdataBNode> extends
        AbstractBNodeIV<V, String> implements IInlineUnicode {

    private static final long serialVersionUID = 1L;
    
    /** The blank node ID. */
    private final String id;
    
    /** The cached byte length of this {@link IV}. */
    private transient int byteLength = 0;

    public IV<V, String> clone(final boolean clearCache) {

        final FullyInlineUnicodeBNodeIV<V> tmp = new FullyInlineUnicodeBNodeIV<V>(
                id);

        // propagate transient state if available.
        tmp.byteLength = byteLength;

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public FullyInlineUnicodeBNodeIV(final String id) {

        super(DTE.XSDString);

        this.id = id;

    }

    /**
     * 
     * @param id The
     * @param byteLength The byte length of this {@link IV}.
     */
    public FullyInlineUnicodeBNodeIV(final String id, final int byteLength) {

        super(DTE.XSDString);

        this.id = id;

        this.byteLength = byteLength;

    }

    public String toString() {
        return id;
    }

    final public String getInlineValue() {
        return id;
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof FullyInlineUnicodeBNodeIV<?>) {
            return this.id.equals(((FullyInlineUnicodeBNodeIV<?>) o).id);
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int _compareTo(final IV o) {
         
        final FullyInlineUnicodeBNodeIV<?> t = (FullyInlineUnicodeBNodeIV<?>) o;

        return IVUnicode.IVUnicodeComparator.INSTANCE.compare(id, t.id);
        
    }
    
    public int hashCode() {
       
        return id.hashCode();
        
    }

    public int byteLength() {

        if (byteLength == 0) {
        
            byteLength = 1/* flags */+ IVUnicode.byteLengthUnicode(id);
            
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
    
    /**
     * Implements {@link BNode#getID()}.
     */
    @Override
    public String getID() {
        
        return id;

    }

    /**
     * Does not need materialization to answer BNode interface methods.
     */
	@Override
	public boolean needsMaterialization() {
		
		return false;
		
	}

    
}
