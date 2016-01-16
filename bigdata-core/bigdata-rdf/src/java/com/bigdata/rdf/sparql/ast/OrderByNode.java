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
/*
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * AST node models an ORDER BY clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OrderByNode extends SolutionModifierBase implements
        Iterable<OrderByExpr> {

//    private final List<OrderByExpr> orderBy = new LinkedList<OrderByExpr>();

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     */
    public OrderByNode(final OrderByNode op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     */
    public OrderByNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    public OrderByNode() {
    }

    public void addExpr(final OrderByExpr orderBy) {
        
        addArg((BOp) orderBy);
        
    }
    
    public boolean removeOrderBy(final OrderByExpr orderBy) {
        
        return removeArg(orderBy);
        
    }
    
    public int size() {
        
        return arity();
        
    }
    
    public boolean isEmpty() {
     
        return arity() == 0;
        
    }
    
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<OrderByExpr> iterator() {

        return (Iterator) argIterator();

    }

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append("\n");

        sb.append(indent(indent));
        
        sb.append("ORDER BY");
        
        for (OrderByExpr e : this) {
        
            sb.append(" ");
            
            sb.append(e.toString());
            
        }

        return sb.toString();
        
    }

//    @Override
//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof OrderByNode))
//            return false;
//
//        final OrderByNode t = (OrderByNode) o;
//
//        if (!orderBy.equals(t.orderBy))
//            return false;
//
//        return true;
//
//    }

}
