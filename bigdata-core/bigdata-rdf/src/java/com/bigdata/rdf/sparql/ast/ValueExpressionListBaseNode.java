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
 * Base class for AST nodes which model an ordered list of value expressions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ValueExpressionListBaseNode<E extends IValueExpressionNode>
        extends SolutionModifierBase implements Iterable<E> {

//    private final List<E> exprs = new LinkedList<E>();

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     */
    public ValueExpressionListBaseNode(final ValueExpressionListBaseNode op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     */
    public ValueExpressionListBaseNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    public ValueExpressionListBaseNode() {
        super();
    }
    
    public void addExpr(final E e) {

        if (e == null)
            throw new IllegalArgumentException();
        
        addArg((BOp) e);

    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Iterator<E> iterator() {

        return (Iterator) argIterator();

    }

    @SuppressWarnings("unchecked")
    public E getExpr(final int index) {

        return (E) get(index);
        
    }
    
    public int size() {

        return arity();
        
    }

    public boolean isEmpty() {
        
        return size() == 0;
        
    }

//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof ValueExpressionListBaseNode<?>))
//            return false;
//
//        final ValueExpressionListBaseNode<? extends IValueExpressionNode> t = (ValueExpressionListBaseNode<?>) o;
//
//        if (!exprs.equals(t.exprs))
//            return false;
//        
//        return true;
//        
//    }
    
}
