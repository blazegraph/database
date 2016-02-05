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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.error.SparqlTypeErrorException;


/**
 * This class implements 'AS' in a projection as found in both 
 * subselects and bind, e.g.
 * <pre>
 *    { SUBSELECT (Foo(?x) AS ?y) ...
 *      {  ...
 *      }
 *    }
 * </pre>
 * or
 * <pre>
 *    BIND (Bar(?x) AS ?y) 
 * </pre>
 * The bind fails if the expression evaluates to some value that is different from
 * a pre-existing binding for the target variable.
 * If the expr (Foo or Bar above) has an error then the specification is that no binding is done,
 * but the unbound value is a success.
 * 
 * The specification for this operation is found as 
 * <a href="http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#defn_extend">Extend</a>.
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProjectedConstraint extends com.bigdata.bop.constraint.Constraint {

    private static final long serialVersionUID = 1L;
    
    public ProjectedConstraint(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    public ProjectedConstraint(final ProjectedConstraint op) {
        super(op);
    }

    public ProjectedConstraint(ConditionalBind bind) {
        super(new BOp[] { bind }, null);
    }

    @Override
    public boolean accept(IBindingSet bindingSet) {
        try {
            Object result = ((ConditionalBind) get(0)).get(bindingSet);
            return  result != null;
        } catch (SparqlTypeErrorException stee) {
        	// Extend(mu, var, expr) = mu if var not in dom(mu) and expr(mu) is an error (from the spec)
            return true;
        }
    }
}
