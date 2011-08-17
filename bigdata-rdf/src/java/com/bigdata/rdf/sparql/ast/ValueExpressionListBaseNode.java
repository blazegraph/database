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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for AST nodes which model an ordered list of value expressions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ValueExpressionListBaseNode<E extends IValueExpressionNode>
        extends SolutionModifierBase implements Iterable<E> {

    protected final List<E> exprs = new LinkedList<E>();

    public void addExpr(final E e) {

        if (e == null)
            throw new IllegalArgumentException();
        
        exprs.add(e);

    }

    public Iterator<E> iterator() {

        return exprs.iterator();

    }

    public int size() {

        return exprs.size();
        
    }

    public boolean isEmpty() {
        
        return exprs.isEmpty();
        
    }
    
}
