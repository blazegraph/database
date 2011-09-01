/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.NoSuchElementException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Iterator consumes the solutions from a query and interprets them according to
 * a {@link ConstructNode}. Ground triples in the template are output
 * immediately. Any non-ground triples are output iff they are fully (and
 * validly) bound for a given solution. Blank nodes are scoped to a solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTConstructIterator implements ICloseableIterator<BigdataStatement> {

    private final AST2BOpContext ctx;

    private final QueryRoot queryRoot;

    private final ICloseableIterator<IBindingSet> src;
    
    private boolean open = true;
    
    /**
     * 
     */
    public ASTConstructIterator(final AST2BOpContext ctx,
            final QueryRoot queryRoot, final ICloseableIterator<IBindingSet> src) {
        
        this.ctx = ctx;
        
        this.queryRoot = queryRoot;

        this.src = src;

    }

    @Override
    public boolean hasNext() {
        if (!src.hasNext()) {
            if (open)
                close();
            return false;
        }
        /*
         * FIXME Assemble valid triples now. If none, then recursively try
         * hasNext() again. If some found, then return true and report them from
         * next();
         */
        return false;
    }

    @Override
    public BigdataStatement next() {
        if (!hasNext())
            throw new NoSuchElementException();

        // FIXME report triples already assembled.
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (open) {
            src.close();
        }
    }

}
