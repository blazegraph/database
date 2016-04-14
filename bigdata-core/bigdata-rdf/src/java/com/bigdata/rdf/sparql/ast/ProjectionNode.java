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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import java.util.HashSet;

/**
 * AST node modeling projected value expressions.
 * <p>
 * Note: "*" is modeled using an explicit variable whose name is <code>*</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProjectionNode extends ValueExpressionListBaseNode<AssignmentNode> {

    private static final long serialVersionUID = 1L;

    interface Annotations extends ValueExpressionListBaseNode.Annotations {
    
        /**
         * {@link Boolean} value annotation marks projection of the "distinct"
         * solutions.
         */
        String DISTINCT = "distinct";

        boolean DEFAULT_DISTINCT = false;

        /**
         * {@link Boolean} value annotation marks projection of the "reduced"
         * solutions.
         */
        String REDUCED = "reduced";

        boolean DEFAULT_REDUCED = false;
        
        /**
         * Optional annotation specifies the {@link DescribeModeEnum} that will
         * be used to evaluate a DESCRIBE query. The default is controlled by
         * {@value QueryHints#DEFAULT_DESCRIBE_MODE}.
         * 
         * @see QueryHints#DESCRIBE_MODE
         */
        String DESCRIBE_MODE = "describeMode";

        /**
         * Optional annotation specifies the limit on the #of iterations for an
         * iterative DESCRIBE algorithm.
         * 
         * @see QueryHints#DESCRIBE_ITERATION_LIMIT
         */
        String DESCRIBE_ITERATION_LIMIT = "describeIterationLimit";
        
        /**
         * Optional annotation specifies the limit on the #of statements for an
         * iterative DESCRIBE algorithm.
         * 
         * @see QueryHints#DESCRIBE_STATEMENT_LIMIT
         */
        String DESCRIBE_STATEMENT_LIMIT = "describeStatementLimit";
        
        /** "Black list" for variables that should not be treated as projection
         * variables, eg, auxiliary aliases introduced for flattening aggregates.
         * Essentially, the blacklisted variables won't show in 
         * getProjectionVars().
         */
        String VARS_TO_EXCLUDE_FROM_PROJECTION = "varsToExcludeFromProjection";
        
        Set<IVariable<?>> DEFAULT_VARS_TO_EXCLUDE_FROM_PROJECTION = null;
    }

    public ProjectionNode() {

    }

    /**
     * Deep copy constructor.
     */
    public ProjectionNode(final ProjectionNode op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     */
    public ProjectionNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    public void setDistinct(final boolean distinct) {

        setProperty(Annotations.DISTINCT, distinct);

    }

    public boolean isDistinct() {

        return getProperty(Annotations.DISTINCT, Annotations.DEFAULT_DISTINCT);

    }

    public void setReduced(final boolean reduced) {

        setProperty(Annotations.REDUCED, reduced);

    }

    public boolean isReduced() {

        return getProperty(Annotations.REDUCED, Annotations.DEFAULT_REDUCED);

    }

    public boolean isWildcard() {

        if (isEmpty())
            return false;

        return getExpr(0).getVar().isWildcard();

    }

    /**
     * Return the {@link DescribeModeEnum} that will be used to evaluate a
     * DESCRIBE query.
     * <p>
     * Note: The default is governed by
     * {@value QueryHints#DEFAULT_DESCRIBE_MODE}.
     * 
     * @return The {@link DescribeModeEnum} or <code>null</code> if this has not
     *         been explicitly specified.
     */
    public DescribeModeEnum getDescribeMode() {
        
        return (DescribeModeEnum) getProperty(Annotations.DESCRIBE_MODE);
        
    }

    /**
     * Set the {@link DescribeModeEnum} that will be used to evaluate a DESCRIBE
     * query.
     * <p>
     * Note: The default is governed by
     * {@value QueryHints#DEFAULT_DESCRIBE_MODE}.
     * 
     * @param describeMode
     *            The {@link DescribeModeEnum} or <code>null</code> to use the
     *            default.
     *            
     * @see Annotations#DESCRIBE_MODE
     */
    public void setDescribeMode(final DescribeModeEnum describeMode) {

        setProperty(Annotations.DESCRIBE_MODE, describeMode);

    }
    
    /**
     * Return the optional limit on the #of iterations for a DESCRIBE query.
     * 
     * @return The limit -or- <code>null</code>.
     * 
     * @see Annotations#DESCRIBE_ITERATION_LIMIT
     */
    public Integer getDescribeIterationLimit() {

        return (Integer) getProperty(Annotations.DESCRIBE_ITERATION_LIMIT);

    }

    /**
     * Return the optional limit on the #of statements for a DESCRIBE query.
     * 
     * @return The limit -or- <code>null</code>.
     * 
     * @see Annotations#DESCRIBE_STATEMENT_LIMIT
     */
    public Integer getDescribeStatementLimit() {
        
        return (Integer) getProperty(Annotations.DESCRIBE_STATEMENT_LIMIT);
        
    }

    /**
     * Set the optional limit on the #of iterations for a DESCRIBE query.
     */
    public void setDescribeIterationLimit(final int newValue) {

        setProperty(Annotations.DESCRIBE_ITERATION_LIMIT, newValue);

    }

    /**
     * Set the optional limit on the #of statements for a DESCRIBE query.
     */
    public void setDescribeStatementLimit(final int newValue) {

        setProperty(Annotations.DESCRIBE_STATEMENT_LIMIT, newValue);

    }

    /**
     * Adds a variable to be projected. The variable is modeled as an assignment
     * of itself to itself, so everything in the projection node winds up
     * looking like an assignment.
     * 
     * @param var
     *            The variable.
     */
    public void addProjectionVar(final VarNode var) {

        addExpr(new AssignmentNode(var, var));

    }

    public void addProjectionExpression(final AssignmentNode assignment) {

        addExpr(assignment);

    }
    
    /** Makes a copy of vars the (new) black list for variables that are 
     *  not to be treated as projection variables.
     *  This feature is useful for auxiliary aliases.
     */
    public void setVarsToExcludeFromProjection(final Set<IVariable<?>> vars) {
        setProperty(Annotations.VARS_TO_EXCLUDE_FROM_PROJECTION, 
                new HashSet<IVariable<?>>(vars));
    }
    
    
    /**
     * Return the ordered subset of the value expressions which project a
     * computed value expression which is not a bare variable.
     * 
     * TODO Consistent API for {@link #getAssignmentProjections()} and
     * {@link #getProjectionVars()}.
     */
    public List<AssignmentNode> getAssignmentProjections() {

        final List<AssignmentNode> assignments = new LinkedList<AssignmentNode>();

        for (AssignmentNode n : this) {

            if (n.getValueExpressionNode().equals(n.getVarNode()))
                continue;

            assignments.add(n);

        }

        return assignments;

    }

    /**
     * Return the projected variables.
     */
    public IVariable[] getProjectionVars() {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();

        getProjectionVars(vars);
//        for (AssignmentNode n : this) {
//
//            vars.add(n.getVar());
//
//        }

        return (IVariable[]) vars.toArray(new IVariable[vars.size()]);

    }
    
    /**
     * Return the projected variables.
     * 
     * @param vars
     *            A set into which the projected variables will be added.
     * 
     * @return The caller's set.
     */
    public Set<IVariable<?>> getProjectionVars(final Set<IVariable<?>> vars) {

        for (AssignmentNode n : this) {

            if (!excludeFromProjection(n.getVar())) {
                vars.add(n.getVar());
            }

        }
        
        return vars;

    }

    
    
    /** Checks if the variable is "blacklisted" to be excluded from
     *  projection variables. This feature is useful for auxiliary aliases.
     */
    public boolean excludeFromProjection(final IVariable<?> var) {
        Set<IVariable<?>> vars = 
                (Set<IVariable<?>>) getProperty(Annotations.VARS_TO_EXCLUDE_FROM_PROJECTION,
                Annotations.DEFAULT_VARS_TO_EXCLUDE_FROM_PROJECTION);
        return (vars != null) && vars.contains(var);
    }
    
    /**
     * Collect the variables used by the SELECT EXPRESSIONS for this projection
     * node.
     * <p>
     * Note: This DOES NOT report the variables which are projected OUT of the
     * query. It reports the variables on which those projected variables
     * depend.
     * 
     * @param vars
     *            The variables are inserted into this set.
     *            
     * @return The caller's set.
     */
    public Set<IVariable<?>> getSelectExprVars(final Set<IVariable<?>> vars) {
        
        for(AssignmentNode n : this) {

            final Iterator<IVariable<?>> itr = BOpUtility.getSpannedVariables(n
                    .getValueExpression());

            while (itr.hasNext()) {

                vars.add(itr.next());

            }
            
        }
        
        return vars;
        
    }
    
    /**
     * Return the {@link IValueExpression}s for this {@link ProjectionNode}.
     */
    public IValueExpression[] getValueExpressions() {

        final IValueExpression<?>[] exprs = new IValueExpression[size()];
        
        int i = 0;
        
        for (AssignmentNode n : this) {

            exprs[i++] = new Bind(n.getVar(), n.getValueExpression());

        }
        
        return exprs;
        
    }

    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(indent(indent)).append("SELECT ");

        if (isDistinct())
            sb.append("DISTINCT ");

        if (isReduced())
            sb.append("REDUCED ");

        final DescribeModeEnum describeMode = getDescribeMode();

        final Integer describeIterationLimit = getDescribeIterationLimit();

        final Integer describeStatementLimit = getDescribeStatementLimit();

        if (describeMode != null) {

            sb.append("[describeMode=" + describeMode + "]");

        }

        if (describeIterationLimit != null) {

            sb.append("[describeIterationLimit=" + describeIterationLimit + "]");

        }

        if (describeStatementLimit != null) {

            sb.append("[describeStatement=" + describeStatementLimit + "]");

        }

        if (isWildcard()) {

            sb.append("* ");
            
        } else {

            boolean first = true;

            for (AssignmentNode v : this) {

                if (first) {
                    first = false;
                } else {
                    sb.append(" ");
                }

                sb.append(v);

                if (excludeFromProjection(v.getVar())) {
                    sb.append("[excludeFromProjection]");
                }
            
            }

        }

        return sb.toString();

    }

}
