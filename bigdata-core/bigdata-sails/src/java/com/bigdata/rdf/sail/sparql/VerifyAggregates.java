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
 * Created on Jul 27, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IValueExpressionConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.UnknownFunctionBOp;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import cutthecrap.utils.striterators.Striterator;

/**
 * An object which encapsulates the validation and state of an aggregation
 * operation with an optional GROUP BY clause, SELECT expressions, and an
 * optional HAVING clause. The SELECT expressions MUST be aggregates (if the
 * SELECT expressions do not involve aggregates then you should not be using an
 * aggregation operator to compute the select expressions).
 * <p>
 * Note: This is a port of {@link com.bigdata.bop.solutions.GroupByState} that
 * does not depend on the blazegraph operator model. It was developed as part of
 * BLZG-1176 to decouple the SPARQL parser from the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 *
 * @see https://jira.blazegraph.com/browse/BLZG-1176
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VerifyAggregates {

    private static final Logger log = Logger.getLogger(VerifyAggregates.class);
    
    private final IValueExpression<?>[] select;
    private final IValueExpression<?>[] groupBy;
    private final IConstraint[] having;
    private final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
    private final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
    private final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{select=" + Arrays.toString(select));
        sb.append(",groupBy=" + Arrays.toString(groupBy));
        sb.append(",having=" + Arrays.toString(having));
        sb.append("}");
        return sb.toString();
    }
    
    public VerifyAggregates(final ProjectionNode projection,
            final GroupByNode groupBy, final HavingNode having) {

//        // normalize an empty[] to a null.
        this.groupBy = groupBy == null || groupBy.arity() == 0 ? null : groupBy.getValueExpressions();
        
        // Replacing unresolvedFunctionNodes with specific AggregateBase to be able
        // to check for aggregates in com.bigdata.bop.solutions.GroupByState.isAggregate
        if (projection!=null) {
            final IValueExpression<?>[] exprs = new IValueExpression[projection.arity()];
            int i = 0;
            final Striterator projectionNodes = new Striterator(projection.iterator());
            while(projectionNodes.hasMoreElements()) {
                final AssignmentNode n = (AssignmentNode) projectionNodes.nextElement();
                IValueExpression expr = n.getValueExpression();
                final IValueExpressionNode exprNode = n.getValueExpressionNode();
                if (expr == null && exprNode instanceof FunctionNode) {
                    expr = convertAggregates((FunctionNode)exprNode);
                }
                exprs[i++] = new Bind(n.getVar(), expr);
            }
            select = exprs;
        } else {
            select = null;
        }


        if (projection == null)
            throw new IllegalArgumentException();

        if (projection.arity() == 0)
            throw new IllegalArgumentException();

        if (having!=null) {
            final IConstraint[] exprs2 = new IConstraint[having.arity()];
            int i = 0;
            for (final IValueExpressionNode node : having) {
    
                final IValueExpression<? extends IV> ve = node.getValueExpression();
                if (ve!=null) {
                    exprs2[i] = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                            ve);
                } else if (node instanceof FunctionNode) {
                    
                    final FunctionNode exprNode = (FunctionNode)node;
                    final BOp expr = convertAggregates(exprNode);
                    exprs2[i] = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(new BOp[]{
                            expr}, null);
                    
                    log.debug("Unknown node "+node);
                }
                i++;
    
            }
            this.having = exprs2;
        } else {
            this.having = null;
        }

        
        // true iff any aggregate expression uses DISTINCT.
        final AtomicBoolean anyDistinct = new AtomicBoolean(false);

        // true iff any aggregate expression nests another aggregate expression.
        final AtomicBoolean nestedAggregates = new AtomicBoolean(false);
        
        /*
         * Validate GROUP_BY value expressions.
         * 
         * Note: The GROUP BY clause may include bare variables such as "?x",
         * non-aggregate expressions such as "STR(?x)" and declarations of
         * variables for non-aggregate expressions such as "STR(?x) as strX".
         * However, only bare variables or variables declared using "AS" may
         * appear in the SELECT clause. Those variables are collected in
         * [groupByVars].
         * 
         * Note: Aggregate functions MAY NOT appear in the GROUP_BY clause.
         */
        if (groupBy != null) {

            // Collect top-level variables from GROUP_BY value expressions.
            for (final IValueExpression<?> expr : this.groupBy) {
                if (expr instanceof IVariable<?>) {
                    groupByVars.add((IVariable<?>) expr);
                } else if (expr instanceof IBind<?>) {
                    final IBind<?> bindExpr = (IBind<?>) expr;
                    final IValueExpression<?> e = bindExpr.getExpr();
                    if (isAggregate(e, false/* isSelectClause */,
                            null/* isSelectDependency */, nestedAggregates,
                            anyDistinct)) {
                        throw new IllegalArgumentException(
                                "Aggregate expression not allowed in GROUP_BY: "
                                        + expr);
                    }
                    groupByVars.add(bindExpr.getVar());
                }
            }

        }

        /*
         * Validate SELECT value expressions.
         * 
         * Note: SELECT value expressions must be either variables appearing in
         * the top-level of the GROUP BY value expressions -or- a IBind wrapping
         * an aggregate function.
         * 
         * Note: Certain optimizations are possible when none of the SELECT
         * value expressions use DISTINCT.
         * 
         * Note: Certain optimizations are possible when all of the SELECT value
         * expressions may be computed based on per-group counters.
         */
        {
            // true iff any aggregate expression uses a reference to another
            // aggregate expression in the select clause.
            final AtomicBoolean selectDependency = new AtomicBoolean(false);
            for (final IValueExpression<?> expr : this.select) {
                /*
                 * Each SELECT value expression must be either a top-level
                 * IVariable in the GROUP BY clause or an IBind wrapping a value
                 * expression consisting solely of aggregates (which may of
                 * course wrap bare variables) and constants.
                 */
                if (expr instanceof IVariable<?>) {
                    final IVariable<?> var = (IVariable<?>) expr;
                    if (!groupByVars.contains(var)) {
                        throw new IllegalArgumentException(
                                "Bare variable not declared by GROUP_BY clause: "
                                        + var);
                    }
                    selectVars.add(var);
                } else if (expr instanceof IBind<?>) {
                    /*
                     * Child of IBind must be a valid aggregate expression
                     * consisting solely of aggregates (which may wrap bare
                     * variables declared in the GROUP_BY clause) and constants.
                     * 
                     * Note: Top-level variables already declared in a GROUP_BY
                     * or SELECT clause MAY appear within other value
                     * expressions in the SELECT clause.
                     * 
                     * Note: If any aggregate in the expression uses DISTINCT
                     * then we make a note of that as certain optimizations are
                     * not possible when DISTINCT is used within an aggregate
                     * expression (this is done by isAggregate()).
                     */
                    final IBind<?> bindExpr = (IBind<?>) expr;
                    final IValueExpression<?> e = bindExpr.getExpr();
                    if (!isAggregate(e, true/* isSelectClause */,
                            selectDependency, nestedAggregates, anyDistinct))
                        throw new IllegalArgumentException("Not an aggregate: "
                                + bindExpr);
                    selectVars.add(bindExpr.getVar());
                } else {
                    throw new IllegalArgumentException(
                            "Top-level of SELECT expression must be IVariable or IBind: "
                                    + expr);
                }
            }
        }

        /*
         * HAVING clause.
         * 
         * The having[] may be null or an empty[]. However, any value
         * expressions used within the IConstraint[] must be aggregates (as
         * defined for SELECT expressions).
         */
        /*
         * true iff none of the value expressions in the HAVING clause involve
         * IAggregate functions.
         */
        boolean simpleHaving = true;
        if (having != null) {
            
            for (final IConstraint c : this.having) {

                /*
                 * The constraint must be an aggregate expression.
                 * 
                 * Note: Top-level variables already declared in a GROUP_BY or
                 * SELECT clause MAY appear within value expressions in the
                 * HAVING clause.
                 * 
                 * Note: If any aggregate in the expression uses DISTINCT then
                 * we make a note of that as certain optimizations are not
                 * possible when DISTINCT is used within an aggregate expression
                 * (this is done by isAggregate()).
                 */

                if (!isAggregate(c, false/* isSelectClause */,
                        null/* isSelectDependency */, nestedAggregates,
                        anyDistinct))
                    throw new IllegalArgumentException("Not an aggregate: " + c);

                if (simpleHaving) {
                    /*
                     * Inspect the value expression for each constraint.
                     * Typically the constraint will be a SPARQLConstraint,
                     * which reports the EBV of a value expression. If that
                     * value expression uses an IAggregate function then we set
                     * [simpleHaving := false]. We are done as soon as we have
                     * falsified the "simpleHaving" hypothesis.
                     */
                    final IValueExpression<?> expr = ((IValueExpressionConstraint<?>) c)
                            .getValueExpression();
                    final Iterator<BOp> itr = BOpUtility.preOrderIterator(expr);
                    while (itr.hasNext()) {
                        final BOp t = itr.next();
                        if (t instanceof IAggregate<?>) {
                            simpleHaving = false;
                            break;
                        }
                    }
                }
            }
        }

    }

    private IValueExpression convertAggregates(final BOp exprNode) {
        IValueExpression expr;
        final BOp[] args = new BOp[exprNode.args().size()];
        if (exprNode!=null && exprNode.arity()>0) {
            for (int i=0; i<exprNode.args().size(); i++) {
                final BOp arg = exprNode.args().get(i);
                final IValueExpression newValue = convertAggregates(arg); 
                if (newValue!=null) {
                    args[i] = newValue;
                } else {
                    args[i] = arg;
                }
            }
        }
        if (exprNode instanceof FunctionNode) {
            if ((exprNode instanceof FunctionNode) && FunctionRegistry.isAggregate(((FunctionNode) exprNode).getFunctionURI())) {
                expr = new AggregateBase(args, null) {
                    @Override public void reset() {}
                    @Override public IV done() { return null; }
                };
            } else {
                expr = new UnknownFunctionBOp(args, null);
            }
        } else {
            expr = null;
        }
        return expr;
    }

    /**
     * Return <code>true</code> iff the expression is an aggregate.
     * <p>
     * Aggregates may be built out of constants, references to {@link IVariable}
     * s which are already defined and which are themselves aggregates, and
     * {@link IAggregate} functions. An {@link IVariable} will be an aggregate
     * if it appears as a bare variable in a GROUP_BY clause or if it declared
     * by a prior value expression in a GROUP_BY or SELECT clause. Testing
     * whether or not an {@link IValueExpression} is an aggregate therefore
     * depends on access to the set of known aggregates. The value expressions
     * in the GROUP_BY clause must be processed first (in order) followed by the
     * value expressions in the SELECT clause (in order).
     * <p>
     * An aggregate may use a non-aggregate variable only allowed within an
     * {@link IAggregate} function. For example, given:
     * <code>SUM(?x) as ?y</code>, <code>?x</code> must be a non-aggregate
     * variable and <code>?y</code> will be an aggregate variable.
     * <p>
     * Aggregate variables may be used both inside and outside of an
     * {@link IAggregate} function as long as the variable was declared before
     * it was used. For example, the following are legal:
     * 
     * <pre>
     * SELECT SUM(?x) as ?y, SUM(?x + ?y) as ?z, SUM(?x)+AVG(?x) as ?z2
     * 
     * SELECT SUM(?x) as ?y, SUM(?x + COUNT(?y)) as ?z
     * </pre>
     * 
     * Patterns where an aggregate depends on a prior aggregate prevent certain
     * optimizations, notably you have to evaluate each aggregate in turn rather
     * than evaluating them in parallel over the solutions is a group. If any
     * such patterns are observed in the SELECT clause then this method will set
     * <code>isSelectDependency := true</code> as a side-effect.
     * 
     * @param op
     *            An {@link IValueExpression} or {@link IConstraint}.
     * @param isSelectClause
     *            <code>true</code> if the <i>op</i> appears a SELECT clause.
     * @param isSelectDependency
     *            Set as a side-effect when an {@link IValueExpression}
     *            appearing in a SELECT clause has a dependency on an
     *            {@link IVariable} declared in the GROUP_BY clause or earlier
     *            in the SELECT clause. This argument is optional unless
     *            <i>isSelectClause</i> is <code>true</code>.
     * @param isNestedAggregates
     *            Set as a side-effect when an {@link IValueExpression}
     *            containing an {@link IAggregate} nests another
     *            {@link IAggregate} within it.
     * @param isAnyDistinct
     *            Set as a side-effect if an {@link IAggregate} function is
     *            encountered which reports <code>true</code> for
     *            {@link IAggregate#isDistinct()}.
     * 
     * @return <code>true</code> iff the operator is an aggregate.
     */
    protected boolean isAggregate(final BOp op,
            final boolean isSelectClause,
            final AtomicBoolean isSelectDependency,
            final AtomicBoolean isNestedAggregates,
            final AtomicBoolean isAnyDistinct) {

        if (op == null) {
            // BOp is not prepared yet, but it is not an Aggregate,
            // otherwise dummy aggregate BOp provided
            // by com.bigdata.rdf.sparql.ast.ProjectionNode.getValueExpressions() 
            return false;
        }

        if (op instanceof IConstant && isSelectClause) {
            /*
             * A constant appearing in the root of a SELECT expression is an
             * aggregate.
             */
            return true;
        }

        return isAggregate(op, isSelectClause, isSelectDependency,
                isNestedAggregates, isAnyDistinct, false/* withinAggregateFunction */);

    }

    private boolean isAggregate(final BOp op,
            final boolean isSelectClause,
            final AtomicBoolean isSelectDependency,
            final AtomicBoolean isNestedAggregates,
            final AtomicBoolean isAnyDistinct,
            final boolean withinAggregateFunction) {

        if (op instanceof IAggregate<?> ||
                ((op instanceof FunctionNode) && FunctionRegistry.isAggregate(((FunctionNode) op).getFunctionURI()))
                    ) {
            if(withinAggregateFunction) {
                isNestedAggregates.set(true);
            }
            if (((IAggregate<?>) op).isDistinct()) {
                isAnyDistinct.set(true);
            }
        }
        final boolean aggregationContext = withinAggregateFunction
                || op instanceof IAggregate<?>;
        boolean isAggregate = aggregationContext;
        {
            final BOp t = op;
            if (t instanceof IVariable<?>) {
                final IVariable<?> v = (IVariable<?>) t;
                if (aggregationContext) {
                    /*
                     * Decide if a variable appearing in within an aggregation
                     * context is a reference to a previously observed
                     * aggregate. If not, then we presume it to be a variable in
                     * the detail records and aggregation will (at least logically)
                     * form a column projection of that variable for each group.
                     */
                    if (!groupByVars.contains(v) && !selectVars.contains(v)) {
                        columnVars.add(v);
                    }
                    return false;
                }
                if (groupByVars.contains(v)) {
                    isAggregate = true;
                    return true;
                }
                if (selectVars.contains(v)) {
                    if (isSelectClause)
                        isSelectDependency.set(true);
                    isAggregate = true;
                    return true;
                }
                if(isSelectClause) {
                    /*
                     * Note: This is also thrown when there is a forward
                     * reference to a variable in the select expression which we
                     * have not yet seen.
                     * 
                     * Note: This situation does not arise for the GROUP_BY
                     * clause because it may only reference non-aggregate
                     * variables.
                     * 
                     * Note: This situation does not arise for the HAVING clause
                     * because it can not define new variables using "AS".
                     */
                    throw new IllegalArgumentException(
                        "Non-aggregate variable in select expression: " + v);
                }
            }
        }
        final Iterator<BOp> itr = op.argIterator();
        while (itr.hasNext()) {
            final BOp arg = itr.next();
            if (selectVars.contains(arg)) {
                if (isSelectClause)
                    isSelectDependency.set(true);
                isAggregate = true;
                return true;
            }
            if (log.isTraceEnabled())
                log.trace("op=" + op.getClass()
                        + //
                        ", isSelectClause="
                        + isSelectClause //
                        + ", isSelectDependency="
                        + isSelectDependency //
                        + ", isNestedAggregates="
                        + isNestedAggregates//
                        + ", isAnyDistinct="
                        + isAnyDistinct //
                        + ", withinAggregateFunction="
                        + withinAggregateFunction //
                        + ", aggregationContext=" + aggregationContext //
                        + ", groupByVars=" + groupByVars//
                        + ", selectVars=" + selectVars //
                        + ", arg=" + arg//
                );
            // recursion through child value expression.
            isAggregate |= isAggregate(arg, isSelectClause, isSelectDependency,
                    isNestedAggregates, isAnyDistinct, aggregationContext/* withinAggregateFunction */);
        }

        return isAggregate;

    }

    /**
     * Verify the various conditions that must be met when a query uses GROUP BY
     * or when a query uses aggregates in a PROJECTION.
     * 
     * @param queryBase
     *            The query.
     * 
     * @throws VisitorException
     */
    public static void verifyAggregate(final QueryBase queryBase)
            throws VisitorException {

        /*
         * The following code has some dependencies on whether or not the
         * value expressions have been cached. That is not done until we get
         * into AST2BOpUtility. I have worked some hacks to support this in
         * FunctionRegistry.isAggregate() and StaticAnalysis.isAggregate().
         * However, the code is still hitting some edge cases.
         * 
         * MP: I fixed this by running the ASTSetValueOptimizer earlier in the
         * parsing process - ie. in Bigdata2ASTSPARQLParser.parseQuery2.
         * 
         * There is some commented out code from openrdf that depends on setting
         * a flag for the expression if an AggregationCollector reports at least
         * one aggregation in a projection element. We could do this same thing
         * here but we still need to have the logic to figure out what is an
         * invalid aggregate.
         * 
         * MP: I think the place to go look for reference is Sesame's
         * TupleExprBuilder, especially:
         * 
         * public TupleExpr visit(ASTSelect node, Object data)
         * 
         * And also look at the AggregateCollector.
         */
        
        final ProjectionNode projection = queryBase.getProjection() == null ? null
                : queryBase.getProjection().isEmpty() ? null : queryBase
                        .getProjection();

        final GroupByNode groupBy = queryBase.getGroupBy() == null ? null
                : queryBase.getGroupBy().isEmpty() ? null : queryBase
                        .getGroupBy();

        final HavingNode having = queryBase.getHaving() == null ? null
                : queryBase.getHaving().isEmpty() ? null : queryBase
                        .getHaving();

        // true if this is an aggregation query.
        final boolean isAggregate = StaticAnalysis.isAggregate(projection,
                groupBy, having);

        if (isAggregate) {

            if (projection.isWildcard())
                throw new VisitorException(
                        "Wildcard not allowed with aggregate.");

            try {

                /*
                 * Delegate logic to validate the aggregate query.
                 */

                new VerifyAggregates(projection, groupBy, having);

            } catch (final IllegalArgumentException ex) {

                throw new VisitorException("Bad aggregate", ex);

            }

        }

    }
}
