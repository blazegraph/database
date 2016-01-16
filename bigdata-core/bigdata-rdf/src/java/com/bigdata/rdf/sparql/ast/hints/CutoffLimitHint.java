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
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Limit the input into joins by limiting the number of elements read
 * from an access path.  Not exactly a cutoff join, which limits output
 * from the join rather than input into it.
 */
final class CutoffLimitHint extends AbstractLongQueryHint {

    protected CutoffLimitHint() {
        super(QueryHints.CUTOFF_LIMIT, Long.MAX_VALUE);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Long value) {

        if (op instanceof StatementPatternNode) {

            _setQueryHint(context, scope, op, getName(), value);

            return;

        }

    }

}
