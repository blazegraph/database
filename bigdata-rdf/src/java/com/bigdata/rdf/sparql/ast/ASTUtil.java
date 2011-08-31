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
 * Created on Aug 31, 2011
 */

package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ModifiableBOpBase;

/**
 * Some utility methods for AST/IV conversions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTUtil {

    /**
     * Convert an {@link IVariable}[] into a {@link VarNode}[].
     */
    static public VarNode[] convert(final IVariable[] a) {

        if (a == null)
            return null;

        final VarNode[] b = new VarNode[a.length];

        for (int i = 0; i < a.length; i++) {

            b[i] = new VarNode(a[i].getName());

        }

        return b;

    }

    /**
     * Convert an {@link VarNode}[] into an {@link IVariable}[].
     */
    static public IVariable[] convert(final VarNode[] a) {

        if (a == null)
            return null;

        final IVariable[] b = new IVariable[a.length];

        for (int i = 0; i < a.length; i++) {

            b[i] = a[i].getValueExpression();

        }

        return b;

    }

    /**
     * Replace a child of a node with another reference (destructive
     * modification). All arguments which point to the oldChild will be replaced
     * by references to the newChild.
     * 
     * @param p
     * @param oldChild
     * @param newChild
     * 
     * @return The #of references which were replaced.
     */
    public static int replaceWith(BOp p, BOp oldChild, BOp newChild) {

        if (!(p instanceof ModifiableBOpBase))
            throw new UnsupportedOperationException();

        final int arity = p.arity();

        int nmods = 0;
        
        for (int i = 0; i < arity; i++) {

            final BOp child = p.get(i);
            
            if(child == oldChild) {
                
                ((ModifiableBOpBase)p).setArg(i, newChild);
                
                nmods++;
                
            }
            
        }

        return nmods;
        
    }

}
