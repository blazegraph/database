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
 * Created on Nov 27, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.constraints.RegexBOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * {@link https://jira.blazegraph.com/browse/BLZG-1780}
 * 
 * {@see https://www.w3.org/TR/sparql11-query/#func-regex}  
 * 
 * {@see https://www.w3.org/TR/sparql11-query/#restrictString}
 * 
 * By default, regex is only applied to Literal String values.   Enabling this
 * query hint will attempt to autoconvert non-String literals into their
 * string value.  This is the equivalent of always using the str(...) function.
 * 
 */
final class RegexMatchNonStringHint extends AbstractBooleanQueryHint {

    protected RegexMatchNonStringHint() {
        super(QueryHints.REGEX_MATCH_NON_STRING, QueryHints.DEFAULT_REGEX_MATCH_NON_STRING);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {
    	
		if (op instanceof FilterNode) {

			final IValueExpressionNode n = ((FilterNode) op)
					.getValueExpressionNode();

			assert(n != null);

			@SuppressWarnings("rawtypes")
			final IValueExpression n2 = n.getValueExpression();

			if (n2 != null && n2 instanceof RegexBOp) {
				((RegexBOp)n2).setMatchNonString(value);
			}
		}

    }

}
