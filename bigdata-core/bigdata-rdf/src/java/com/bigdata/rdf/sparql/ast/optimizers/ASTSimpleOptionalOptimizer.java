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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.internal.constraints.TrueBOp;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.ComputedMaterializationRequirement;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceCallUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BDS;

/**
 * A "simple optional" is an optional sub-group that contains only one statement
 * pattern, no sub-groups of its own, and no filters that require materialized
 * variables based on the optional statement pattern. We can lift these
 * "simple optionals" into the parent group where the join evaluation will be
 * less expensive.
 * <p>
 * Note: When the filter is lifted, it must be attached to the statement pattern
 * node such that toPredicate() puts them onto the predicate since they must run
 * *with* the join for that predicate. (The problem is that ?x != Bar is
 * filtering the optional join, not ?x).
 * 
 * <pre>
 * where {
 *  ?x type Foo . // adds binding for ?x
 *  optional {
 *    ?x p ?y . // adds bindings for ?y if ?x != Bar
 *    filter (?x != Bar) .
 *  }
 * }
 * </pre
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSimpleOptionalOptimizer.java 5197 2011-09-15 19:10:44Z
 *          thompsonbry $
 */
public class ASTSimpleOptionalOptimizer implements IASTOptimizer {

    @SuppressWarnings("unchecked")
    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     
 
        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        // Note: This causes queries such as govtrack/query0021 to run much slower.
//        if (context.mergeJoin) {
//            /*
//             * Do not translate simple optional groups when we expect to do a
//             * merge join.
//             */
//            return queryNode;
//        }
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        /*
         * Collect optional groups.
         * 
         * Note: We can not transform graph patterns inside of SERVICE calls so
         * this explicitly visits the interesting parts of the tree.
         */

        final Collection<JoinGroupNode> optionalGroups = new LinkedList<JoinGroupNode>();

        {

            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    collectOptionalGroups(namedSubquery.getWhereClause(),
                            optionalGroups);

                }

            }

            collectOptionalGroups(queryRoot.getWhereClause(), optionalGroups);

        }

        /*
         * For each optional group, if it qualifies as a simple optional then
         * lift the statement pattern node and and filters into the parent group
         * and mark the statement pattern node as "optional".
         */
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        for(JoinGroupNode group : optionalGroups) {
            
            liftOptionalGroup(sa, group);
            
        }
        
        return new QueryNodeWithBindingSet(queryNode, bindingSets);
        
    }

    /**
     * Collect the optional groups.
     * <p>
     * Note: This will NOT visit stuff inside of SERVICE calls. If those graph
     * patterns get rewritten it has to be by the SERVICE, not us.
     * <p>
     * Note: Do not bother to collect an "optional" unless it has a parent join
     * group node (they all should).
     */
    @SuppressWarnings("unchecked")
    private void collectOptionalGroups(
            final GraphPatternGroup<IGroupMemberNode> group,
            final Collection<JoinGroupNode> optionalGroups) {

        if (group instanceof JoinGroupNode && group.isOptional()
                && group.getParent() != null) {
            
            optionalGroups.add((JoinGroupNode) group);
            
        }
        
        for(IGroupMemberNode child : group) {

            if (child instanceof ServiceNode) {

                final ServiceNode serviceNode = ((ServiceNode) child);
                
                final IVariableOrConstant<?> serviceRef = serviceNode
                        .getServiceRef().getValueExpression();

                if (serviceRef.isVar()) {
                    
                    continue;

                }

                final BigdataURI serviceURI = ServiceCallUtility
                        .getConstantServiceURI(serviceRef);

                if (!BDS.SEARCH.equals(serviceURI)) {

                    /*
                     * Do NOT translate SERVICE nodes (unless they are a well
                     * known bigdata service).
                     */

                    continue;

                }

                final GraphPatternGroup<IGroupMemberNode> graphPattern = serviceNode
                        .getGraphPattern();

                collectOptionalGroups(graphPattern, optionalGroups);
                
            }
            
            if (!(child instanceof GraphPatternGroup<?>))
                continue;

            collectOptionalGroups((GraphPatternGroup<IGroupMemberNode>) child,
                    optionalGroups);
           
        }

    }
    
    /**
     * If the {@link JoinGroupNode} qualifies as a simple optional then lift the
     * statement pattern node and and filters into the parent group and mark the
     * statement pattern node as "optional".
     */
    private void liftOptionalGroup(final StaticAnalysis sa,
            final JoinGroupNode group) {

        // The parent join group.
        final JoinGroupNode p = group.getParentJoinGroup();

        if(p == null) {
            // Can't lift if no parent.
            return;
        }
        
        if(!isSimpleOptional(sa, p, group)) {
        
            // Not a simple optional.
            return;
            
        }

        /*
         * First, get the simple optional statement pattern and also identify
         * any FILTERs to be attached to that statement pattern.
         * 
         * Note: We can lift a filter as long as its materialization
         * requirements would be satisfied in the parent.
         */
        final StatementPatternNode sp;
        final List<FilterNode> filters = new LinkedList<FilterNode>();
        final List<FilterNode> mockFilters = new LinkedList<FilterNode>();
        {
            
            StatementPatternNode tmp = null;

            for(IGroupMemberNode child : group) {
                
                if(child instanceof StatementPatternNode) {

                    tmp = (StatementPatternNode) child;

                } else if (child instanceof FilterNode) {

                    final FilterNode filter = (FilterNode) child;
                    
                    filters.add( filter);

                    final INeedsMaterialization req = filter
                            .getMaterializationRequirement();

                    if (req.getRequirement() == INeedsMaterialization.Requirement.NEVER) {

                        /*
                         * The filter does not have any materialization requirements
                         * so it can definitely be lifted with the statement
                         * pattern.
                         */

                        continue;
                        
                    }

                    if (req instanceof ComputedMaterializationRequirement) {

                        /*
                         * We can lift a filter which only depends on variables
                         * which are "incoming bound" into the parent.
                         * 
                         * Note: In order to do this, the parent join group must
                         * ensure that the variable(s) used by this filter are
                         * materialized before the optional join is run.
                         * 
                         * We achieve that by attaching the appropriate
                         * materialization requirements to a "mock" filter for
                         * the required variable(s) in the parent group. (Make
                         * sure that we use toVE() on the mock filter.)
                         */

                        final IValueExpressionNode ven = BOpUtility
                                .deepCopy((ValueExpressionNode) filter
                                        .getValueExpressionNode());

                        ven.setValueExpression(TrueBOp.INSTANCE);

                        final ComputedMaterializationRequirement mockReq = new ComputedMaterializationRequirement(
                                Requirement.ALWAYS,
                                ((ComputedMaterializationRequirement) req)
                                        .getVarsToMaterialize());

                        final MockFilterNode mockFilter = new MockFilterNode(
                                ven, mockReq);

                        mockFilters.add(mockFilter);
                        
                    }
                    
                } else {

                    /*
                     * This would indicate an error in the logic to identify
                     * which join groups qualify as "simple" optionals.
                     */

                    throw new AssertionError(
                            "Unexpected child for simple optional: group="
                                    + group + ", child=" + child);

                }

            }

            assert tmp != null;
            
            sp = tmp;
            
        }

        /*
         * Set the flag so we know to do an OPTIONAL join for this statement
         * pattern.
         */
        sp.setOptional(true);

        /*
         * Attach any lifted filters.
         */
        if (!filters.isEmpty())
            sp.setAttachedJoinFilters(filters);

        /*
         * Replace the group with the statement pattern node.
         */
        p.replaceWith((BOp) group, (BOp) sp);
       
        /*
         * Add any mock filters used to impose materialization requirements on
         * the parent join group in support of lifted filter(s).
         */
        for(FilterNode mockFilter : mockFilters) {
            
            p.addChild(mockFilter);
            
        }
        
    }
    
    /**
     * Return <code>true</code> iff the <i>group</i> is a "simple optional".
     * 
     * @param p
     *            The parent {@link JoinGroupNode} (never <code>null</code>).
     * @param group
     *            Some candidate {@link JoinGroupNode}.
     */
    private static boolean isSimpleOptional(final StaticAnalysis sa,
            final JoinGroupNode p, final JoinGroupNode group) {

        if (!group.isOptional()) {

            // first, the whole group must be optional
            return false;

        }

        /*
         * Second, make sure we have only one statement pattern, no sub-queries,
         * and no filters that require materialization.
         */
        StatementPatternNode sp = null;

        for (IQueryNode node : group) {

            if (node instanceof StatementPatternNode) {

                if (sp != null) {
                    /*
                     * We already have one statement pattern so this is not a
                     * simple optional.
                     */
                    return false;
                }

                sp = (StatementPatternNode) node;

            } else if (node instanceof FilterNode) {

                /*
                 * The filter must be attached to the optional join for the
                 * statement pattern if the statement pattern is lifted into the
                 * parent group. Therefore we have to examine the
                 * materialization requirements for the filter.
                 */

                final FilterNode filter = (FilterNode) node;

                final INeedsMaterialization req = filter
                        .getMaterializationRequirement();

                if (req.getRequirement() == INeedsMaterialization.Requirement.NEVER) {

                    /*
                     * The filter does not have any materialization requirements
                     * so it can definitely be lifted with the statement
                     * pattern.
                     */

                    continue;
                    
                }

                /*
                 * Note: This is disabled. I am having trouble getting the query
                 * plan to generate the correct materialization operations with
                 * the mock filter node and its mock requirements. Talk this
                 * over with MikeP or wait until I get further into how the
                 * materialization pipeline is generated. Also, it seems that we
                 * would have to use ALWAYS as the materialization requirement
                 * in order to ensure that the variable(s) were materialized
                 * before the optional join. If so, then this might not be worth
                 * the effort as we could (potentially) be doing more work than
                 * if we just ran the optional in the child group.
                 * 
                 * Note: I have decided that this is not worth the candle. The
                 * new optional sub-group hash join code path is much faster and
                 * handles complex optionals just fine.
                 */

                if (false && req instanceof ComputedMaterializationRequirement) {
                    
                    /*
                     * We can lift a filter which only depends on variables
                     * which are "incoming bound" into the parent.
                     * 
                     * Note: In order to do this, the parent join group must
                     * ensure that the variable(s) used by this filter are
                     * materialized before the optional join is run. We can
                     * achieve that by attaching the appropriate materialization
                     * requirements to a "mock" filter for the required
                     * variable(s) in the parent group. (Make sure that we use
                     * toVE() on the mock filter.)
                     */

                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    final Set<IVariable<?>> requiredVars = (Set) ((ComputedMaterializationRequirement)req)
                            .getVarsToMaterialize();

                    final Set<IVariable<?>> incomingBound = sa
                            .getDefinitelyIncomingBindings(group,
                                    new LinkedHashSet<IVariable<?>>());

                    requiredVars.removeAll(incomingBound);

                    if (requiredVars.isEmpty()) {

                        continue;

                    }

                }
                
                /*
                 * There is at least one required variable for this filter which
                 * is not incoming bound to the optional group and hence would
                 * not be definitely bound if the optional statement pattern
                 * were evaluated in the parent group.
                 */

                return false;

            } else {

                /*
                 * Anything else will queer the deal.
                 */

                return false;

            }

        }

        /*
         * If we found one and only one statement pattern and have not tripped
         * any of the other conditions then this is a "simple" optional.
         */

        return sp != null;

    }

    /**
     * Used to impose additional materialization requirements on the required
     * join group in the parent when lifting a filter whose materialization
     * requirements could be satisfied against the parent group.
     */
    final static class MockFilterNode extends FilterNode {

        private static final long serialVersionUID = 1L;

        interface Annotations extends FilterNode.Annotations {
            
            /**
             * The {@link ComputedMaterializationRequirement} for the filter
             * which was lifted onto the optional statement pattern from the
             * simple optional group.
             */
            String REQUIREMENT = "requirement";
            
        }

        /**
         * Shallow copy constructor.
         */
        public MockFilterNode(BOp[] args, Map<String, Object> anns) {

            super(args, anns);
            
        }

        /**
         * Deep copy constructor.
         */
        public MockFilterNode(MockFilterNode op) {

            super(op);
            
        }

        /**
         * @param ve
         */
        public MockFilterNode(final IValueExpressionNode ve,
                final ComputedMaterializationRequirement req) {
         
            super(ve);
            
            setProperty(Annotations.REQUIREMENT, req);
            
        }

        /**
         * Report any variables that are part of the materialization requirement
         * for the original filter.
         */
        @SuppressWarnings("unchecked")
        @Override
        public Set<IVariable<?>> getConsumedVars() {

            return (Set) getMaterializationRequirement().getVarsToMaterialize();

        }
        
        @Override
        final public ComputedMaterializationRequirement getMaterializationRequirement() {

            return (ComputedMaterializationRequirement) getRequiredProperty(Annotations.REQUIREMENT);

        }

        /**
         * Overridden to flag mock filters as such.
         */
        @Override
        public String toString(final int indent) {
           
            return super.toString(indent) + " [mockFilter]";
            
        }

    }

}
