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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * "IN" and "NOT IN" operator based on testing of the enumerated value
 * expressions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ComputedIN extends XSDBooleanIVValueExpression {

    private static final long serialVersionUID = 9066209752427789308L;

    public interface Annotations extends XSDBooleanIVValueExpression.Annotations {

        /**
         * <code>true</code> iff this is "NOT IN" rather than "IN".
         */
        String NOT = ComputedIN.class.getName() + ".not";

    }

    private transient boolean not = false;

    @SuppressWarnings("rawtypes")
    public ComputedIN(final boolean not, final IValueExpression<? extends IV>... ise) {

        this(ise, NV.asMap(Annotations.NOT, Boolean.valueOf(not)));
        
    }

    public ComputedIN(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);

        if (getProperty(Annotations.NOT) == null)
            throw new IllegalArgumentException();

        this.not = ((Boolean) getProperty(Annotations.NOT)).booleanValue();

        final IValueExpression<? extends IV> var = get(0);

        if (var == null)
            throw new IllegalArgumentException();

        if (arity() < 2) {
            throw new IllegalArgumentException();
        }

    }

    public ComputedIN(final ComputedIN op) {
        
        super(op);
        
    }

    @SuppressWarnings({ "rawtypes" })
    public boolean accept(final IBindingSet bindingSet) {

        final IV iv = get(0).get(bindingSet);

        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        boolean found = false;

        for (int i = 1; i < arity(); i++) {

            final IV right = get(i).get(bindingSet);

            if (right != null) {

                if (CompareBOp.compare(iv, right, CompareOp.EQ)) {

                    found = true;

                    break;

                }

            }

        }

        return not ? !found : found;

    }

}
