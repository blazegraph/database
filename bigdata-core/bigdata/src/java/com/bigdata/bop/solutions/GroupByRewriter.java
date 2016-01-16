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
 * Created on Jul 29, 2011
 */

package com.bigdata.bop.solutions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * Utility class simplifies an aggregation operator through a rewrite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Modify to rewrite AVERAGE as SUM()/COUNT() in preparation for a
 *          more distributed / parallel evaluation when the aggregate does not
 *          use DISTINCT and when there are no dependencies among the aggregate
 *          expressions (e.g., SUM(x)+1 is fine, as is SUM(x)+SUM(y), but
 *          SUM(x+AVG(x)) introduces a dependency which prevents us from
 *          optimizing the aggregation using per-group incremental pipelined
 *          evaluation).
 */
public class GroupByRewriter implements IGroupByRewriteState, IVariableFactory,
        Serializable {

    /**
     * Note: This class must be serializable so we may distribute it with the
     * parallel decomposition of an aggregation operator on a cluster, otherwise
     * each time an operator computes the rewrite it will assign new variables
     * and we will be unable to combine the decomposed aggregation results on
     * the query controller.
     */
    private static final long serialVersionUID = 1L;
    
    private final LinkedHashMap<IAggregate<?>,IVariable<?>> aggExpr;
//    private final LinkedHashMap<IValueExpression<?>,ProjectionType> columnProjections;
    private final IValueExpression<?>[] select2;
    private final IConstraint[] having2;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{aggExpr=" + aggExpr);
        sb.append(",select2=" + Arrays.toString(select2));
        sb.append(",having2=" + Arrays.toString(having2));
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * The set of all unique {@link IAggregate} expressions with {@link Bind}s
     * onto anonymous variables. Any internal {@link IAggregate} have been
     * lifted out and will appear before any {@link IAggregate}s which use them.
     */
    public LinkedHashMap<IAggregate<?>,IVariable<?>> getAggExpr() {
        return aggExpr;
    }

    /**
     * A modified version of the original SELECT expression which has the same
     * semantics. However, the modified select expressions DO NOT contain any
     * {@link IAggregate} functions. All {@link IAggregate} functions have been
     * lifted out into {@link #aggExpr}.
     */
    @Override
    public IValueExpression<?>[] getSelect2() {
        return select2;
    }

    /**
     * A modified version of the original HAVING expression which has the same
     * semantics (and <code>null</code> iff the original was <code>null</code>
     * or empty). However, the modified select expressions DO NOT contain any
     * {@link IAggregate} functions. All {@link IAggregate} functions have been
     * lifted out into {@link #aggExpr}.
     */
    @Override
    public IConstraint[] getHaving2() {
        return having2;
    }
    
//    /**
//     * Metadata flags reporting whether the column projection of a value
//     * expression will include all values or only the distinct values. If both
//     * flags are set, then both the projection of all values and the projection
//     * of all distinct values are required.
//     */
//    public static class ProjectionType {
//
//        /**
//         * The column projection of the all observed values is required.
//         */
//        static final int AllValues = 1 << 0;
//
//        /**
//         * The column projection of the observed distinct values is required.
//         */
//        static final int DistinctValues = 1 << 1;
//
//        private int state = 0;
//
//        public ProjectionType() {
//
//        }
//
//        public boolean isAllValues() {
//            return (state & AllValues) != 0;
//        }
//
//        public boolean isDistinctValues() {
//            return (state & DistinctValues) != 0;
//        }
//
//        public void setAllValues() {
//            state |= AllValues;
//        }
//
//        public void setDistinctValues() {
//            state |= DistinctValues;
//        }
//        
//        public Object getColumnProjection() {
//            return proj;
//        }
//        
//        public void setColumnProjection(Object proj) {
//            this.proj = proj;
//        }
//        
//        private Object proj;
//        
//    }
//    
//    /**
//     * The distinct {@link IValueExpression}s whose column projections are used
//     * by the {@link #getAggExpr() aggregate expressions}. The variables appear
//     * in the order in which they are first used by the aggregate expressions.
//     * For example, given
//     * 
//     * <pre>
//     * SELECT SUM(x), SUM(y), SUM(x+y), AVG(x+y), SUM(DISTINCT z)
//     * </pre>
//     * 
//     * this would return <code>[x,y,x+y]</code>.
//     * <p>
//     * Note that the value expression <code>x+y</code> appears more than once as
//     * the inner value expression of different aggregation functions, but it is
//     * only reported once.
//     * 
//     * TODO This is unused and untested.
//     */
//    public LinkedHashMap<IValueExpression<?>, ProjectionType> getColumnProjections() {
//        return columnProjections;
//    }

    /**
     * Special construct creates a distinct instance of each {@link IAggregate}
     * in order to avoid side-effects in the internal state of the
     * {@link IAggregate} functions when evaluated in different contexts (e.g.,
     * a pipelined aggregation subquery).
     * 
     * @param rewrittenState
     */
    public GroupByRewriter(final IGroupByRewriteState rewrittenState) {

        this.select2 = rewrittenState.getSelect2();

        this.having2 = rewrittenState.getHaving2();

        final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr = rewrittenState.getAggExpr();

        this.aggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        for (Map.Entry<IAggregate<?>, IVariable<?>> e : aggExpr.entrySet()) {

            // Note: *clone* the IAggregate function!
            this.aggExpr.put((IAggregate<?>) e.getKey().clone(), e.getValue());

        }
        
    }
    
    public GroupByRewriter(final IGroupByState groupByState) {

        if (groupByState == null)
            throw new IllegalArgumentException();

        this.aggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

//        this.columnProjections = new LinkedHashMap<IValueExpression<?>, ProjectionType>();

        final IValueExpression<?>[] select = groupByState.getSelectClause();

        this.select2 = new IValueExpression[select.length];

        /*
         * Rewrite having clause.
         * 
         * Note: The HAVING clause (if one exists) is rewritten first so the
         * left-to-right dependency will evaluate the aggregates for the HAVING
         * clause before the aggregates for the SELECT clause. This makes it
         * easier to decide whether or not a GROUP will pass its HAVING filter
         * before we project the solution for that group.
         */
        
        final IConstraint[] having = groupByState.getHavingClause();
        
        if (having == null || having.length == 0) {
         
            this.having2 = null;
            
        } else {

            /*
             * Rewrite the HAVING clause.
             */
            
            having2 = new IConstraint[having.length];
            
            for (int i = 0; i < having.length; i++) {

                final IConstraint e = having[i];

                having2[i] = (IConstraint) rewrite(e, this, aggExpr);

            }

        }
        
        /*
         * Rewrite SELECT clause.
         */
        for (int i = 0; i < select.length; i++) {

            final IValueExpression<?> e = select[i];

            select2[i] = rewrite(e, this, aggExpr);

        }

//        /*
//         * Collect the distinct value expressions (the inner expression for the
//         * aggregates) and add some metadata about the types of column projects
//         * which we require for each such value expression.
//         */
//        for (IAggregate<?> e : aggExpr.keySet()) {
//            final IValueExpression<?> valueExpr = e.getExpr();
//            ProjectionType ptype = columnProjections.get(valueExpr);
//            if (ptype == null) {
//                ptype = new ProjectionType();
//                columnProjections.put(valueExpr, ptype);
//            }
//            if (e.isDistinct())
//                ptype.setDistinctValues();
//            else
//                ptype.setAllValues();
//        }
        
    }

    /**
     * Rewrite an {@link IConstraint}.
     * <p>
     * Note: Rewriting a constraint require us to effectively clone the original
     * constraint. I've hacked this in two ways. First, I assume that the inner
     * value expression is <code>e.get(0)</code>. Second, I wrap the rewritten
     * value expression with {@link #newConstraint(IValueExpression)}. That
     * factory is responsible for the type of the returned {@link IConstraint}.
     */
    static public IConstraint rewrite(final IConstraint e,
            final IVariableFactory f,
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr) {

        // tunnel through : assumes arg0 is the inner value expression!
        final IValueExpression<?> oldInnerExpr = (IValueExpression<?>) e.get(0);

        // rewrite the inner value expression.
        final IValueExpression<?> newInnerExpr = rewrite(oldInnerExpr, f,
                aggExpr);

        if (newInnerExpr == oldInnerExpr) {
            // Not rewritten.
            return e;
        }

        return (IConstraint) ((BOpBase) e).setArg(0, newInnerExpr);

    }
    
    /**
     * Rewrite an {@link IValueExpression} from a SELECT or HAVING clause. If a
     * rewrite is performed, then the modified expression is returned. Otherwise
     * the original expression is returned. If an aggregation expression is
     * lifted out by the rewrite, then it is added to <i>aggExpr</i>.
     * 
     * @param e
     *            The {@link IValueExpression}.
     * @param f
     *            A factory for anonymous variables.
     * @param aggExpr
     *            A map associating each unique {@link IAggregate} expression
     *            lifted out of the an {@link IValueExpression} with an
     *            anonymous variable on which the value of the
     *            {@link IAggregate} lifted out the of {@link IValueExpression}
     *            will be bound when that {@link IAggregate} is evaluated.
     * 
     * @return The (possibly rewritten) {@link IValueExpression}.
     */
    public static IValueExpression<?> rewrite(final IValueExpression<?> e,
            final IVariableFactory f,
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr) {

        /*
         * Bind should only be observed at the top-level.
         */

        if (e instanceof IBind<?>) {

            final IValueExpression<?> expr = ((IBind<?>) e).getExpr();
            
            if (expr instanceof IVariableOrConstant<?>) {
            
                return e;
                
            }

        }

        /*
         * Re-write the expression (anything but a top-level bind).
         */

        return rewrite2(e, f, aggExpr);

    }
    
    /**
     * Depth first recursion replaces any {@link IAggregate}s. Depth first
     * recursion is used to lift any embedded {@link IAggregate}s out first.
     * 
     * @param expr
     *            The value expression.
     * @param f
     *            The factory for anonymous variables.
     * @param aggExpr
     *            The map of distinct {@link IAggregate} expressions and the
     *            anonymous variables which they will be bound as when they are
     *            evaluated.
     *            
     * @return The (possibly rewritten) expression.
     */
    static private IValueExpression<?> rewrite2(IValueExpression<?> expr,
            final IVariableFactory f,
            final LinkedHashMap<IAggregate<?>, IVariable<?>> aggExpr) {

        if (expr instanceof IVariableOrConstant<?> || expr.arity() == 0) {
            // Nothing to do.
            return expr;
        }

        /*
         * Depth first recursion for each child argument.
         */
        int index = 0;

        final Iterator<BOp> itr = expr.argIterator();
        
        while (itr.hasNext()) {

            final IValueExpression<?> c = (IValueExpression<?>) itr.next();

            // rewrite child.
            final IValueExpression<?> newC = rewrite(c, f, aggExpr);

            if (newC != c) {

                // copy-on-write for parent iff child was rewritten.
                expr = (IValueExpression<?>) ((BOpBase) expr).setArg(index,
                        newC);

            }

            index++;
            
        }
        
        /*
         * Examine this node and replace it with a reference to an anonymous
         * variable if it is an IAggregate expression.
         */
        
        if (expr instanceof IAggregate<?>) {

            final IAggregate<?> t = (IAggregate<?>) expr;

            IVariable<?> anonVar = aggExpr.get(t);

            if (anonVar == null) {

                /*
                 * This is the first time we have encountered this aggregate
                 * expression.
                 */

                // new anonymous variable.
                anonVar = f.var();
                
                // add to the map.
                aggExpr.put(t, anonVar);

            }
            
            // return the anonymous variable for the aggregate expression.
            return anonVar;
            
        }
        
        return expr;

    }

    /**
     * Return a new anonymous variable (this is overridden by some unit tests in
     * order to have predictable variable names).
     */
    @Override
    public IVariable<?> var() {

        return Var.var();

    }

}
