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

import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint for turning on/off a distinct filter over the triples extracted
 * from the default graph in quads mode.
 * <p>
 * By default, a DISTINCT filter is applied when evaluating access paths
 * against the default graph, for correctness reasons. The hint (or the 
 * respective system property) can be used to disabled the distinct filter,
 * in order to accelerate default graph queries. This can be done whenever
 * it is known that NO triple occurs in more than one named graph. 
 * 
 * BE CAREFUL: if this condition does not hold and the filter is disabled, 
 * wrong query results (caused by duplicates) will be the consequence.
 */
final class DefaultGraphDistinctFilterHint extends AbstractBooleanQueryHint {

    protected DefaultGraphDistinctFilterHint() {
        super(QueryHints.DEFAULT_GRAPH_DISTINCT_FILTER, QueryHints.DEFAULT_DEFAULT_GRAPH_DISTINCT_FILTER);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        switch (scope) {
        case Query:
            context.defaultGraphDistinctFilter = value;
            return;
        default:
            break;
        }

        throw new QueryHintException(scope, op, getName(), value);

    }

}
