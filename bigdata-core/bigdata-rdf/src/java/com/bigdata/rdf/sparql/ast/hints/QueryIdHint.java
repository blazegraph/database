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

import java.util.Properties;
import java.util.UUID;

import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;

/**
 * This just strips the {@link QueryHints#QUERYID} out of the AST. The
 * {@link Bigdata2ASTSPARQLParser} is responsible for traversing the AST and, if
 * it finds the {@link QueryHints#QUERYID}, attaching it to the
 * {@link ASTContainer}'s query hints {@link Properties} object.
 * <P>
 * Note: The timing for interpreting this query hint is critical, which is why
 * it is handled by the {@link Bigdata2ASTSPARQLParser}. We need to know if the
 * QueryID was set long before the {@link ASTQueryHintOptimizer} runs.
 */
final class QueryIdHint extends AbstractQueryHint<UUID> {

    protected QueryIdHint() {

        super(QueryHints.QUERYID, null/* defaultValue */);

    }

    @Override
    public UUID validate(final String value) {

        return UUID.fromString(value);

    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final UUID value) {

        /*
         * NOP.
         */
 
    }

}
