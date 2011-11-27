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
 * Created on Nov 27, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSparql11SubqueryOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.QueryHintScope;

/**
 * Query hint indicating whether or not a Sub-Select should be transformed into
 * a named subquery, lifting its evaluation out of the main body of the query
 * and replacing the subquery with an INCLUDE. When <code>true</code>, the
 * subquery will be lifted out. When <code>false</code>, the subquery will not
 * be lifted unless other semantics require that it be lifted out regardless.
 * <p>
 * Note: This sets an AST annotation which is interpreted by the
 * {@link ASTSparql11SubqueryOptimizer}.
 */
final class RunOnceHint extends AbstractBooleanQueryHint {

    protected RunOnceHint() {

        super(SubqueryRoot.Annotations.RUN_ONCE, null/* default */);

    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        if (scope == QueryHintScope.Prior && op instanceof SubqueryRoot) {

            _setAnnotation(context, scope, op, getName(), value);

        }

        throw new QueryHintException(scope, op, getName(), value);

    }

}