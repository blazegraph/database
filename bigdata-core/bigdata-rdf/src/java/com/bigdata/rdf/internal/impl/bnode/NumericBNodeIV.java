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
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.Bytes;

/**
 * Class for inline RDF blank nodes. Blank nodes MUST be based on a numeric
 * value to be inlined with this class.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 * 
 * @see AbstractTripleStore.Options
 */
public class NumericBNodeIV<V extends BigdataBNode> extends
        AbstractBNodeIV<V, Integer> {

    /**
     * 
     */
    private static final long serialVersionUID = -2057725744604560753L;
    
    private final int id;
    
    public int intValue() {
    	
    	return id;
    	
    }
    
    public IV<V, Integer> clone(final boolean clearCache) {

        final NumericBNodeIV<V> tmp = new NumericBNodeIV<V>(id);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }
    
    public NumericBNodeIV(final int id) {

        super(DTE.XSDInt);

        this.id = id;

    }

    final public Integer getInlineValue() {
        
        return Integer.valueOf(id);
        
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof NumericBNodeIV<?>) {
            return this.id == ((NumericBNodeIV<?>) o).id;
        }
        return false;
    }

    @Override
    public int _compareTo(IV o) {
         
        final int id2 = ((NumericBNodeIV) o).id;
        
        return id == id2 ? 0 : id < id2 ? -1 : 1;
        
    }
    
    public int hashCode() {
        return id;
    }

    public int byteLength() {
        return 1 + Bytes.SIZEOF_INT;
    }


    /**
     * Implements {@link BNode#getID()}.
     */
    @Override
    public String getID() {
        
        return 'i' + String.valueOf(id);

    }

    /**
     * Does not need materialization to answer BNode interface methods.
     */
	@Override
	public boolean needsMaterialization() {
		
		return false;
		
	}

}
