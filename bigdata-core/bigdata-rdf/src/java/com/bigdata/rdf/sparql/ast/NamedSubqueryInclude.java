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
 * Created on Aug 18, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

/**
 * An AST node which provides a reference in an {@link IGroupNode} and indicates
 * that a named solution set should be joined with the solutions in the group.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see NamedSubqueryRoot
 */
public class NamedSubqueryInclude extends
        GroupMemberNodeBase<NamedSubqueryInclude> implements
        IJoinNode, INamedSolutionSet {

    private static final long serialVersionUID = 1L;

    public interface Annotations extends IJoinNode.Annotations,
            INamedSolutionSet.Annotations {

        /**
         * A {@link VarNode}[] specifying the join variables that will be used
         * when the named result set is join with the query. The join variables
         * MUST be bound for a solution to join.
         * <p>
         * Note: This can be different for each context in the query in which a
         * given named result set is included. When there are different join
         * variables for different INCLUDEs, then we need to build a hash index
         * for each set of join variable context that will be consumed within
         * the query.
         * <p>
         * Note: If no join variables are specified, then the join will consider
         * the N x M cross product, filtering for solutions which join. This is
         * very expensive. Whenever possible you should identify one or more
         * variables which must be bound for the join and specify those as the
         * join variables.
         */
        String JOIN_VARS = "joinVars";
        
        /*
         * Note: This is an unfinished concept in support of
         * "Optimize hash joins when there are no source solutions (or only the exogenous bindings)"
         * .
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/405
         */
//        /**
//         * When <code>true</code>, the join variables will be ignored when
//         * performing the join. This option makes it possible to build an index
//         * using join variables, but to evaluate the join without regard to
//         * those join variables.
//         * <p>
//         * The most common use case is used when an INCLUDE appears as the first
//         * {@link IJoinNode} in a query. In this case, the only left solution is
//         * the exogenous solution which, by default, does not have any bindings.
//         * By using a naive join we can scan a hash index built using one or
//         * more join variables, testing each solution in turn against the
//         * exogenous solution. This is efficient since it uses a single pass
//         * over the hash index and correct since it ignores the join variables
//         * for the left solution.
//         */
//        String NAIVE_JOIN = "naiveJoin";
//        
//        boolean DEFAULT_NAIVE_JOIN = false;
        
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public NamedSubqueryInclude(final NamedSubqueryInclude op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public NamedSubqueryInclude(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * @param name
     *            The name of the subquery result set.
     */
    public NamedSubqueryInclude(final String name) {

        setName(name);
        
    }

    @Override
    public String getName() {

        return (String) getProperty(Annotations.NAMED_SET);

    }

    @Override
    public void setName(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        setProperty(Annotations.NAMED_SET, name);

    }

    /**
     * The join variables to be used when the named result set is included into
     * the query.
     */
    public VarNode[] getJoinVars() {

        return (VarNode[]) getProperty(Annotations.JOIN_VARS);

    }

    /**
     * Set the join variables.
     * 
     * @param joinVars
     *            The join variables.
     */
    public void setJoinVars(final VarNode[] joinVars) {

        setProperty(Annotations.JOIN_VARS, joinVars);

    }

    /**
     * Return the ordered set of join variables.
     * 
     * @return The ordered set of join variables and never <code>null</code>.
     */
    @SuppressWarnings("rawtypes")
    public Set<IVariable<?>> getJoinVarSet() {

        final Set<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        final VarNode[] a = getJoinVars();

        if (a != null && a.length > 0) {

            for (IVariable v : ASTUtil.convert(a)) {

                set.add(v);

            }

        }

        return set;

    }
    
    /**
     * Returns <code>false</code>.
     */
    @Override
    final public boolean isOptional() {
        
        return false;
        
    }

    /**
     * Returns <code>false</code>.
     */
    @Override
    final public boolean isMinus() {
     
        return false;
        
    }
    
    @Override
    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    @Override
    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }

    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append("\n");
        
        sb.append(indent(indent));

        sb.append("INCLUDE ").append(getName());

        final VarNode[] joinVars = getJoinVars();

        if (joinVars != null) {

            sb.append(" JOIN ON (");

            boolean first = true;

            for (VarNode var : joinVars) {

                if (!first)
                    sb.append(",");

                sb.append(var);

                first = false;

            }

            sb.append(")");

        }

//        if (getProperty(Annotations.NAIVE_JOIN, Annotations.DEFAULT_NAIVE_JOIN)) {
//
//            sb.append(" [naiveJoin]");
//            
//        }
        
        final List<FilterNode> filters = getAttachedJoinFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }

        return sb.toString();

    }

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {

       final NamedSubqueryRoot nsr = sa.resolveNamedSubqueryInclude(this, sa);
       return nsr==null ?
          new HashSet<IVariable<?>>() : nsr.getRequiredBound(sa);
          
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {

       final NamedSubqueryRoot nsr = sa.resolveNamedSubqueryInclude(this, sa);
       return nsr==null ?
          new HashSet<IVariable<?>>() : nsr.getDesiredBound(sa);
          
    }

}
