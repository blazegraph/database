/*

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

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Coalesce BOp The COALESCE function form returns the RDF term value of the first expression that evaluates without error. In
 * SPARQL, evaluating an unbound variable raises an error.
 */
public class CoalesceBOp extends IVValueExpression<IV> implements IPassesMaterialization {

    private static final long             serialVersionUID = 7391999162162545704L;

    private static final transient Logger log              = Logger.getLogger(CoalesceBOp.class);

    public CoalesceBOp(final GlobalAnnotations globals, final IValueExpression<? extends IV>... expressions) {

        this(expressions, anns(globals));

    }

    /**
     * Required shallow copy constructor.
     */
    public CoalesceBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        if (args.length <= 0)
            throw new IllegalArgumentException();
        for (BOp b : args) {
            if (b == null) {
                throw new IllegalArgumentException();
            }
        }
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public CoalesceBOp(final CoalesceBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {
        for (int i = 0; i < arity(); i++) {
            try {
                IV result = get(i).get(bs);
                if(result!=null){
                    return result;
                }
            } catch (NotMaterializedException nme) {
                throw nme;
            } catch (Throwable t) {

            }
        }
        throw new SparqlTypeErrorException();
    }

}
