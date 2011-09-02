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
 * Created on Aug 25, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.ModifiableBOpBase;

/**
 * Base class for the AST.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTBase extends ModifiableBOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends ModifiableBOpBase.Annotations {
        
    }

    /**
     * @param op
     */
    public ASTBase(ASTBase op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public ASTBase(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Replace all occurrences of the old value with the new value in both the
     * arguments and annotations of this operator (recursive). A match is
     * identified by reference {@link #equals(Object)}. 
     * 
     * @param oldVal
     *            The old value.
     * @param newVal
     *            The new value.
     * 
     * @return The #of changes made.
     */
    public int replaceAllWith(final BOp oldVal, final BOp newVal) {

        int n = 0;

        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = get(i);

            if (child != null && child.equals(oldVal)) {

                setArg(i, newVal);

                n++;

            }

            if (child instanceof ASTBase) {

                n += ((ASTBase) child).replaceAllWith(oldVal, newVal);

            }

        }

        for (Map.Entry<String, Object> e : annotations().entrySet()) {

            if (e.getValue() instanceof ASTBase) {

                n += ((ASTBase) e.getValue()).replaceAllWith(oldVal, newVal);

            }
            
        }
        
        return n;

    }

}
