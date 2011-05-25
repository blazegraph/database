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

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class for inline RDF blank nodes. Blank nodes MUST use a "short" Unicode ID
 * to be inlined with this class.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see AbstractTripleStore.Options
 */
public class UnicodeBNodeIV<V extends BigdataBNode> extends
        AbstractBNodeIV<V, String> {

    private final String id;
    
    public UnicodeBNodeIV(final String id) {

        super(DTE.XSDUnicode);

        this.id = id;

    }

    @Override
    public String stringValue() {
        return id;
    }

    final public String getInlineValue() {
        return id;
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof UnicodeBNodeIV<?>) {
            return this.id.equals(((UnicodeBNodeIV<?>) o).id);
        }
        return false;
    }

    @Override
    protected int _compareTo(final IV o) {
         
        final String id2 = ((UnicodeBNodeIV) o).id;
        
        return id.compareTo(id2);
//        return id == id2 ? 0 : id < id2 ? -1 : 1;
        
    }
    
    public int hashCode() {
        return id.hashCode();
    }

    public int byteLength() {
        if(true) {
            // FIXME TERMS REFACTOR : must know its length, so compress in ctor?
            throw new UnsupportedOperationException();
        }
        return 1 + Bytes.SIZEOF_INT;
    }
    
}