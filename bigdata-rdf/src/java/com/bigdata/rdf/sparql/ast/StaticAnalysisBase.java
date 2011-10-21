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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * Base class for static analysis.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StaticAnalysisBase {

    private static final Logger log = Logger.getLogger(StaticAnalysisBase.class);
    
    protected final QueryRoot queryRoot;
    
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
     */
    protected StaticAnalysisBase(final QueryRoot queryRoot) {
        
        if(queryRoot == null)
            throw new IllegalArgumentException();
        
        this.queryRoot = queryRoot;
        
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

        return getSpannedVariables(op, true/*filters*/, varSet);
        
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
     *         that we always get/ignore the variables in filters as approriate.
     *         For example, an optional {@link StatementPatternNode} can have
     *         filter nodes attached.
     */
    public Set<IVariable<?>> getSpannedVariables(final BOp op,
            final boolean filters, final Set<IVariable<?>> varSet) {

        if (op == null) {

            return varSet;
            
        } else if (op instanceof IVariable<?>) {
         
            varSet.add((IVariable<?>)op);
            
        } else if(op instanceof IConstant<?>) {
            
            final IConstant<?> c = (IConstant<?>)op;
            
            final IVariable<?> var = (IVariable<?> )c.getProperty(Constant.Annotations.VAR);
                
            if( var != null) {
                
                varSet.add(var);
                
            }
            
        } else if (op instanceof FilterNode && !filters) {

            // DO NOT RECURSE INTO THE FILTER!
            return varSet;
            
        } else if (op instanceof SubqueryRoot) {

            /*
             * Do not recurse into a subquery, but report any variables
             * projected by that subquery.
             */

            final SubqueryRoot subquery = (SubqueryRoot) op;

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;

        } else if (op instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude namedInclude = (NamedSubqueryInclude) op;

            final NamedSubqueryRoot subquery = namedInclude
                    .getRequiredNamedSubqueryRoot(queryRoot);

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;
            
        }
        
        /*
         * Recursion.
         */
        if(filters) {
            if(op instanceof StatementPatternNode) {
                /*
                 * Optional statements patterns may have attached filters.
                 */
                final StatementPatternNode t = (StatementPatternNode) op;
                final List<FilterNode> list = t.getFilters();
                if (list != null) {
                    for (FilterNode f : list) {
                        getSpannedVariables(f, filters, varSet);
                    }
                }
            }
        }

        final int arity = op.arity();

        for (int i = 0; i < arity; i++) {

            getSpannedVariables(op.get(i), filters, varSet);

        }

        return varSet;

    }

    /**
     * Add all variables on the {@link ProjectionNode} of the subquery to the
     * set of distinct variables visible within the scope of the parent query.
     * 
     * @param subquery
     * @param varSet
     */
    static private void addProjectedVariables(final SubqueryBase subquery,
            final Set<IVariable<?>> varSet) {
        
        final ProjectionNode proj = subquery.getProjection();

        if (proj.isWildcard()) {
            /* The subquery's projection should already have been rewritten. */
            throw new AssertionError();
        }

        for (IVariable<?> var : proj.getProjectionVars()) {

            varSet.add(var);

        }
        
    }

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

}
