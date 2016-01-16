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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;

/**
 * Base class for static analysis.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StaticAnalysisBase {

//    private static final Logger log = Logger.getLogger(StaticAnalysisBase.class);
    
    protected final QueryRoot queryRoot;
    
    protected final IEvaluationContext evaluationContext;
    
    /**
     * Return the {@link QueryRoot} parameter given to the constructor.
     */
    public QueryRoot getQueryRoot() {
       
        return queryRoot;
        
    }
    
    /**
     * 
     * @param queryRoot
     *            The root of the query. We need to have this on hand in order
     *            to resolve {@link NamedSubqueryInclude}s during static
     *            analysis.
     * @param evaluationContext
	 *            The evaluation context provides access to the
	 *            {@link ISolutionSetStats} and the {@link ISolutionSetManager} for
	 *            named solution sets.
     */
    protected StaticAnalysisBase(final QueryRoot queryRoot,
            final IEvaluationContext evaluationContext) {
        
        if(queryRoot == null)
            throw new IllegalArgumentException();
        
        this.queryRoot = queryRoot;
                
        this.evaluationContext = evaluationContext;

    }
    
    /**
     * Return the distinct variables in the operator tree, including on those on
     * annotations attached to operators. Variables projected by a subquery are
     * included, but not variables within the WHERE clause of the subquery.
     * Variables projected by a {@link NamedSubqueryInclude} are also reported,
     * but not those used within the WHERE clause of the corresponding
     * {@link NamedSubqueryRoot}.
     * 
     * @param op
     *            An operator.
     * @param varSet
     *            The variables are inserted into this {@link Set}.
     * 
     * @return The caller's {@link Set}.
     */
    public Set<IVariable<?>> getSpannedVariables(final BOp op,
            final Set<IVariable<?>> varSet) {

        return getSpannedVariables(op, true/* filters */, varSet);

    }

    /**
     * Return the distinct variables in the operator tree. Variables projected
     * by a subquery are included, but not variables within the WHERE clause of
     * the subquery. Variables projected by a {@link NamedSubqueryInclude} are
     * also reported, but not those used within the WHERE clause of the
     * corresponding {@link NamedSubqueryRoot}.
     * 
     * @param op
     *            An operator.
     * @param filters
     *            When <code>true</code>, variables on {@link FilterNode}s are
     *            also reported.
     * @param varSet
     *            The variables are inserted into this {@link Set}.
     * 
     * @return The caller's {@link Set}.
     * 
     *         TODO Unit tests for different kinds of AST nodes to make sure
     *         that we always get/ignore the variables in filters as appropriate.
     *         For example, an optional {@link StatementPatternNode} can have
     *         filter nodes attached.
     */
    public Set<IVariable<?>> getSpannedVariables(final BOp op,
            final boolean filters, final Set<IVariable<?>> varSet) {

        if (op == null) {

            return varSet;
            
        }
        
//        System.err.println("vars=" + varSet + ", op=" + op.toShortString());
        
        if (op instanceof IVariable<?>) {
         
            varSet.add((IVariable<?>)op);
            
        } else if(op instanceof IConstant<?>) {
            
            final IConstant<?> c = (IConstant<?>)op;
            
            final IVariable<?> var = (IVariable<?> )c.getProperty(Constant.Annotations.VAR);
                
            if( var != null) {
                
                varSet.add(var);
                
            }
            
        } else if (op instanceof ArbitraryLengthPathNode) {
        	
        	varSet.addAll(((ArbitraryLengthPathNode) op).getMaybeProducedBindings());
        	
        	// do not recurse
        	return varSet;
        	
        } else if (op instanceof ZeroLengthPathNode) {
        	
        	varSet.addAll(((ZeroLengthPathNode) op).getProducedBindings());
        	
        	// do not recurse
        	return varSet;

        } else if (op instanceof ServiceNode) {

            // @see http://trac.blazegraph.com/ticket/816
            final ServiceNode serviceNode = (ServiceNode) op;
            
            // Look for the SERVICE URI, it might be a variable as well.
            final TermNode uriRef = serviceNode.getServiceRef();

            if (uriRef instanceof VarNode) {

                varSet.add(((VarNode) uriRef).getValueExpression());

            }

            // pick up anything in the group graph pattern.
            getSpannedVariables(serviceNode.getGraphPattern(), filters, varSet);

            // fall through - look for attached filters.

        } else if (op instanceof BindingsClause) {
    
            final BindingsClause bc = (BindingsClause)op;
            
            varSet.addAll(bc.getDeclaredVariables());
           return varSet;
          
        } else if (op instanceof FilterNode && !filters) {

            // DO NOT RECURSE INTO THE FILTER!
            return varSet;
            
        } else if (op instanceof SubqueryRoot) {

            /*
             * Do not recurse into a subquery, but report any variables
             * projected by that subquery.
             */

            final SubqueryRoot subquery = (SubqueryRoot) op;

            subquery.getProjectedVars(varSet);
//            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;

        } else if (op instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude namedInclude = (NamedSubqueryInclude) op;

            final String name = namedInclude.getName();
            
            final NamedSubqueryRoot subquery = getNamedSubqueryRoot(name);

            if(subquery != null) {
            	
                subquery.getProjectedVars(varSet);
//              addProjectedVariables(subquery, varSet);
            	
            } else {

                final ISolutionSetStats stats = getSolutionSetStats(name);

                /*
                 * Note: This is all variables which are bound in ANY solution.
                 * It MAY include variables which are NOT bound in some
                 * solutions.
                 */

                varSet.addAll(stats.getUsedVars());
            	
            }
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;
            
//        } else {
//            
//            throw new AssertionError("Not handled: " + op);
            
        }
        
        if (filters && op instanceof IJoinNode) {
            /*
             * Join nodes may have attached filters.
             */
            final IJoinNode t = (IJoinNode) op;
            final List<FilterNode> list = t.getAttachedJoinFilters();
            if (list != null) {
                for (FilterNode f : list) {
                    getSpannedVariables(f, filters, varSet);
                }
            }
        }

        /*
         * Recursion.
         */
        final int arity = op.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = op.get(i);
            
            getSpannedVariables(child, filters, varSet);

        }

        return varSet;

    }

    /**
	 * Return the corresponding {@link NamedSubqueryRoot}.
	 * 
	 * @param name
	 *            The name of the solution set.
	 * 
	 * @return The {@link NamedSubqueryRoot} and never <code>null</code>.
	 * 
	 * @throws RuntimeException
	 *             if no {@link NamedSubqueryRoot} was found for the named set
	 *             associated with this {@link NamedSubqueryInclude}.
	 * 
	 * @deprecated Caller's MUST BE CHANGED to look for both a
	 *             {@link NamedSubqueryRoot} and an {@link ISolutionSetStats}
	 *             and then handle these as appropriate. In one case, that means
	 *             static analysis of the {@link NamedSubqueryRoot}. In the
	 *             other, the relevant information are present in pre-computed
	 *             metadata on the {@link ISolutionSetStats}.
	 */
    protected NamedSubqueryRoot getRequiredNamedSubqueryRoot(final String name) {
        
        final NamedSubqueryRoot nsr = getNamedSubqueryRoot(name);
        
        if(nsr == null)
            throw new RuntimeException(
                    "Named subquery does not exist for namedSet: " + name);
        
        return nsr;
        
    }

    /**
	 * Return the corresponding {@link NamedSubqueryRoot}.
	 * <p>
	 * Note: You can not resolve pre-existing named solution sets with this
	 * method, only those which are defined within the scope of a query by a
	 * {@link NamedSubqueryRoot}.
	 * <p>
	 * Note: Typically, callers MUST look for both a {@link NamedSubqueryRoot}
	 * and an {@link ISolutionSetStats} and then handle these as appropriate. In
	 * one case, that means static analysis of the {@link NamedSubqueryRoot}. In
	 * the other, the relevant information are present in pre-computed metadata
	 * on the {@link ISolutionSetStats}.
	 * 
	 * @param name
	 *            The name of the solution set.
	 * 
	 * @return The {@link NamedSubqueryRoot} -or- <code>null</code> if none was
	 *         found.
	 * 
	 * @see #getSolutionSetStats(String)
	 */
    public NamedSubqueryRoot getNamedSubqueryRoot(final String name) {

		final NamedSubqueriesNode namedSubqueries = queryRoot
				.getNamedSubqueries();

		if (namedSubqueries == null)
			return null;

		for (NamedSubqueryRoot namedSubquery : namedSubqueries) {
            
            if(name.equals(namedSubquery.getName()))
                return namedSubquery;
            
        }
        
        return null;

    }

    /**
     * Return the {@link ISolutionSetStats} for the named solution set.
     * <p>
     * Note: This does NOT report on {@link NamedSubqueryRoot}s for the query.
     * It only checks the {@link ISolutionSetManager}.
     * <p>
     * Note: Typically, callers MUST look for both a {@link NamedSubqueryRoot}
     * and an {@link ISolutionSetStats} and then handle these as appropriate. In
     * one case, that means static analysis of the {@link NamedSubqueryRoot}. In
     * the other, the relevant information are present in pre-computed metadata
     * on the {@link ISolutionSetStats}.
     * 
     * @param name
     *            The name of the solution set.
     * 
     * @return The {@link ISolutionSetStats} and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if there is no such pre-existing named solution set.
     * 
     * @see #getNamedSubqueryRoot(String)
     */
    public ISolutionSetStats getSolutionSetStats(final String name) {

        return evaluationContext.getSolutionSetStats(name);
        
    }
    
    /**
     * Add all variables spanned by the operator.
     * <p>
     * <strong>WARNING:</strong> This method does not consider the variable
     * scoping rules. It is safe to use with a FILTER as all variables will be
     * in scope, but it is not safe to use with a {@link SubqueryBase} as only
     * the projected variables will be in scope.
     * 
     * @param bindings
     *            The set to which the variables will be added.
     * @param op
     *            The operator.
     */
    protected void addAll(final Set<IVariable<?>> bindings,
            final IGroupMemberNode op) {

        final Iterator<IVariable<?>> it = BOpUtility
                .getSpannedVariables((BOp) op);

        while (it.hasNext()) {

            bindings.add(it.next());

        }

    }

//    /**
//     * Add all variables on the {@link ProjectionNode} of the subquery to the
//     * set of distinct variables visible within the scope of the parent query.
//     * 
//     * @param subquery
//     * @param varSet
//     */
//    static private void addProjectedVariables(final QueryBase subquery,
//            final Set<IVariable<?>> varSet) {
//        
//        final ProjectionNode proj = subquery.getProjection();
//
//        if (proj.isWildcard()) {
//            /* The subquery's projection should already have been rewritten. */
//            throw new AssertionError();
//        }
//
//        proj.getProjectionVars(varSet);
//        
////        for (IVariable<?> var : proj.getProjectionVars()) {
////
////            varSet.add(var);
////
////        }
//        
//    }

    /**
     * Return <code>true</code> if the {@link FilterNode} is fully bound for the
     * given variables.
     * 
     * @param f
     *            The {@link FilterNode}
     * @param vars
     *            Some collection of variables which are known to be bound.
     * @return <code>true</code> if all variables on which the filter depends
     *         are present in that collection.
     */
    public boolean isFullyBound(final FilterNode f, final Set<IVariable<?>> vars) {

        final Set<IVariable<?>> fvars = getSpannedVariables(f,
                true/* filters */, new LinkedHashSet<IVariable<?>>());

        fvars.removeAll(vars);

        if (fvars.isEmpty()) {

            /*
             * The variables for this filter are all present in the given set of
             * variables.
             */

            return true;

        }

        return false;

    }

    /**
     * Return any variables appearing in the Subject, Predicate, or Object
     * position (the Context position is ignored).
     * 
     * @param sp
     *            The statement pattern.
     * 
     * @return The SPO variables in that statement pattern.
     */
    public static Set<IVariable<?>> getSPOVariables(
            final StatementPatternNode sp) {

        final LinkedHashSet<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        for (int i = 0; i < 3; i++) {

            final TermNode tmp = (TermNode) sp.get(0);

            if (tmp.isVariable()) {

                set.add((IVariable<?>) tmp.getValueExpression());

            }
        }

        return set;

    }

}
