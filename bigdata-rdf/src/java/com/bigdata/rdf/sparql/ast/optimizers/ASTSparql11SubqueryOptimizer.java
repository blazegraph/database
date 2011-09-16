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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

import cutthecrap.utils.striterators.Striterator;

/**
 * Lift {@link SubqueryRoot}s into named subqueries when appropriate.
 * 
 * FIXME We should do this if there are no join variables for the SPARQL 1.1
 * subquery. That will prevent multiple evaluations of the SPARQL 1.1 subquery
 * since the named subquery is run once before the main WHERE clause.
 * <p>
 * This optimizer needs to examine the required statement patterns and decide
 * whether we are better off running the named subquery pipelined after the
 * required statement patterns, using a hash join after the required statement
 * patterns, or running it as a named subquery and then joining in the named
 * solution set (and again, either before or after everything else).
 * 
 * FIXME If a subquery does not share ANY variables which MUST be bound in the
 * parent's context then rewrite the subquery into a named/include pattern so it
 * will run exactly once. (If it does not share any variables at all then it
 * will produce a cross product and, again, we want to run that subquery once.)
 * 
 * @see SubqueryRoot.Annotations#RUN_ONCE
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSparql11SubqueryOptimizer.java 5193 2011-09-15 14:18:56Z
 *          thompsonbry $
 */
public class ASTSparql11SubqueryOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        if(false) {
            
            rewriteSparql11Subqueries(queryRoot);
            
        }

        return queryRoot;
        
    }

    static private void rewriteSparql11Subqueries(final QueryRoot queryRoot){

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Striterator itr2 = new Striterator(
                BOpUtility.postOrderIterator((BOp) queryRoot.getWhereClause()));

        itr2.addTypeFilter(SubqueryRoot.class);

        final List<SubqueryRoot> subqueries  = new ArrayList<SubqueryRoot>();

        while (itr2.hasNext()) {

            subqueries.add((SubqueryRoot)itr2.next());

        }
        
        if (queryRoot.getNamedSubqueries() == null) {
        
            queryRoot.setNamedSubqueries(new NamedSubqueriesNode());
            
        }

        for (SubqueryRoot root : subqueries) {

            final IGroupNode<IGroupMemberNode> parent = root.getParent();

            parent.removeChild(root);

            final String newName = UUID.randomUUID().toString();

            final NamedSubqueryInclude include = new NamedSubqueryInclude(
                    newName);

            parent.addChild(include);

            final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                    root.getQueryType(), newName);

            nsr.setConstruct(root.getConstruct());
            nsr.setGroupBy(root.getGroupBy());
            nsr.setHaving(root.getHaving());
            nsr.setOrderBy(root.getOrderBy());
            nsr.setProjection(root.getProjection());
            nsr.setSlice(root.getSlice());
            nsr.setWhereClause(root.getWhereClause());

            queryRoot.getNamedSubqueries().add(nsr);

        }

    }

}
