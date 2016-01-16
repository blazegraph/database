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
 * Created on Sep 12, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BDS;

/**
 * Rewrites the WHERE clause of each query by lifting out {@link ServiceNode}s
 * into a named subquery. Recursively rewrites the WHERE clause of any named
 * subquery such that there is no more than one {@link ServiceNode} in that
 * subquery by lifting out additional {@link ServiceNode}s into new named
 * subqueries.
 * <p>
 * Note: This rewrite step is useful when it is desirable to run the service
 * once (it is also possible to annotate the {@link ServiceCall} operator as
 * {@link PipelineOp.Annotations#PIPELINED} <code>:= false</code>.
 * <p>
 * By default, if a {@link ServiceNode} appears in any position other than the
 * head of a {@link NamedSubqueryRoot}, then it will be invoked once for each
 * chunk of solutions which flows through that part of the query plan.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTServiceNodeOptimizer.java 6068 2012-03-03 21:34:31Z
 *          thompsonbry $
 * 
 *          FIXME This has been modified to ONLY apply to {@link BD#SEARCH}. It
 *          may well be that we want to take this out entirely.  Our internal
 *          search service runs well when lifted out.  However, it might run
 *          just as well if we specified it as an "at-once" operator.
 */
public class ASTServiceNodeOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTServiceNodeOptimizer.class);
    
    private int nrewrites = 0;
    
    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        /*
         * Lift any ServiceNode out of the main WHERE clause (including any
         * embedded subqueries). We can not have any service invocations run
         * from the main WHERE clause because they will be invoked once for each
         * solution pushed into the query, even if the ServiceNode is the first
         * operator in the query plan. Each such ServiceNode is replaced by a
         * named subquery root and a named subquery include.
         */

        {
        
            @SuppressWarnings("unchecked")
            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                liftOutServiceNodes(queryRoot, context, whereClause, true/* all */);

            }

        }

        /*
         * Examine each named subquery. If there is more than one ServiceNode,
         * or if a ServiceNode is embedded in a subquery, then lift it out into
         * its own named subquery root, replacing it with a named subquery
         * include.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (int i = 0; i < namedSubqueries.size(); i++) {
            
                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                @SuppressWarnings("unchecked")
                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause == null)
                    continue;

                liftOutServiceNodes(queryRoot, context, whereClause, false/* all */);

            }

        }

//        log.error("\nafter rewrite:\n" + queryNode);
        
        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * For each {@link ServiceNode}, create a new {@link NamedSubqueryRoot}
     * which runs that service call replace the {@link ServiceNode} in the
     * original parent with a {@link NamedSubqueryInclude}.
     * 
     * @param parent
     *            The group in which the {@link ServiceNode} appears.
     * @param all
     *            When <code>true</code>, all {@link ServiceNode}s are lifted
     *            out. When <code>false</code>, we will allow the first
     *            {@link ServiceNode} to remain behind. This should be
     *            <code>false</code> only for the top-level of a WHERE clause in
     *            a named subquery. It is always set to <code>true</code> on
     *            recursion into child groups since a {@link ServiceNode} may
     *            not be evaluated in an embedded location without causing
     *            multiple invocations of the SERVICE.
     */
    private void liftOutServiceNodes(//
            final QueryRoot queryRoot,//
            final AST2BOpContext context,//
            final GroupNodeBase<IGroupMemberNode> parent, //
            final boolean all//
            ) {

        boolean first = true;

        final int arity = parent.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = parent.get(i);

            if (child instanceof ServiceNode) {

                final ServiceNode serviceNode = (ServiceNode) child;

                final TermNode serviceRef = serviceNode.getServiceRef();

                if (serviceRef.isConstant()
                        && serviceRef.getValue().equals(BDS.SEARCH)) {

                    if (all || !first) {

                        liftOutServiceNode(queryRoot, context, parent,
                                serviceNode);

                    }

                    first = false;

                }

            }

            if (child instanceof GroupNodeBase<?>) {

                @SuppressWarnings("unchecked")
                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) child;
                
                liftOutServiceNodes(queryRoot, context, childGroup, true/* all */);

            }

        }

    }

    /**
     * Lift out the {@link ServiceNode} into a new {@link NamedSubqueryRoot},
     * replacing it in the parent with a {@link NamedSubqueryInclude}. The
     * result looks as follows, except that the spanned variables in the
     * {@link ServiceNode}s graph pattern are projected rather than using a wild
     * card projection (wild card projections must be rewritten into the
     * projected variables for evaluation so we do not need to reintroduce new
     * wild cards here).
     * 
     * <pre>
     * WITH { SELECT * WHERE { SERVICE <uri> } } AS -anon-service-call-n
     * </pre>
     * 
     * 
     * @param queryRoot
     *            The {@link QueryRoot}.
     * @param parent
     *            The group in which the {@link ServiceNode} appears.
     * @param serviceNode
     *            The {@link ServiceNode}.
     */
    private void liftOutServiceNode(//
            final QueryRoot queryRoot,//
            final AST2BOpContext context,//
            final GroupNodeBase<IGroupMemberNode> parent,//
            final ServiceNode serviceNode//
            ) {

        /*
         * TODO Lift up into caller and pass through (QueryRoot is available
         * from SA)? However, if we start caching the SA then we have to be
         * careful when reusing it across structural modifications.
         */
        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        final String namedSolutionSet = "%-anon-service-call-" + nrewrites++;

        final NamedSubqueryRoot namedSubqueryRoot = new NamedSubqueryRoot(
                QueryType.SELECT, namedSolutionSet);

        {

            {
                
                final ProjectionNode projection = new ProjectionNode();
                
                namedSubqueryRoot.setProjection(projection);
                
                // projection.addProjectionVar(new VarNode("*"));

                final Set<IVariable<?>> varSet = sa.getSpannedVariables(
                        (BOp) serviceNode.getGraphPattern(),
                        new LinkedHashSet<IVariable<?>>());

                for(IVariable<?> var : varSet) {
                
                    projection.addProjectionVar(new VarNode(var.getName()));
                    
                }

            }

            final JoinGroupNode whereClause = new JoinGroupNode(serviceNode);
            namedSubqueryRoot.setWhereClause(whereClause);

            queryRoot.getNamedSubqueriesNotNull().add(namedSubqueryRoot);

        }

        final NamedSubqueryInclude namedSubqueryInclude = new NamedSubqueryInclude(
                namedSolutionSet);

        parent.replaceWith(serviceNode, namedSubqueryInclude);

        if (log.isInfoEnabled()) {
            log.info("\nLifted: " + serviceNode);
            log.info("\nFrom parentGroup:" + parent);
            log.info("\nInto namedSubquery: " + namedSubqueryRoot);
        }

    }

}
