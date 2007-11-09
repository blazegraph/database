/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.store;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.rdf.spo.ISPOIterator;

/**
 * Wraps the raw iterator that traverses a statement index and exposes each
 * visited statement as a {@link Statement} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SesameStatementIterator implements StatementIterator {

    private final AbstractTripleStore db;
    private final ISPOIterator src;
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator.
     */
    public SesameStatementIterator(AbstractTripleStore db,ISPOIterator src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.src = src;

    }
    
    public boolean hasNext() {
        
        return src.hasNext();
        
    }

    /**
     * Note: Returns instances of {@link StatementWithType}.
     */
    public Statement next() {

        return db.asStatement( src.next() );
        
    }
    
    public void close() {
        
        src.close();
        
    }

}