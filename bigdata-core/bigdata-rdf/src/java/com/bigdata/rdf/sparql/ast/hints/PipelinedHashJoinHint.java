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
 * Created on Nov 1, 2015.
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.bop.join.JVMPipelinedHashJoinUtility;
import com.bigdata.bop.join.PipelinedHashIndexAndSolutionSetJoinOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint to enable/disable usage of pipelined hash joins. See
 * {@link PipelinedHashIndexAndSolutionSetJoinOp} and
 * {@link JVMPipelinedHashJoinUtility} for the respective operators.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$ 
 */
final class PipelinedHashJoinHint extends AbstractBooleanQueryHint {

    protected PipelinedHashJoinHint() {

        super(QueryHints.PIPELINED_HASH_JOIN, null/* default */);

    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

       
       switch (scope) {
       case Group:
       case GroupAndSubGroups:
       case Query:
       case SubQuery:
           if (op instanceof JoinGroupNode) {
               _setAnnotation(context, scope, op, getName(), value);
           }
           return;
       case Prior:
       {
          if (op instanceof IGroupMemberNode) {
             _setQueryHint(context, scope, op, getName(), value);          
          }
          return;
       }
       default:
          break;
       }

      throw new QueryHintException(scope, op, getName(), value);


    }

}
