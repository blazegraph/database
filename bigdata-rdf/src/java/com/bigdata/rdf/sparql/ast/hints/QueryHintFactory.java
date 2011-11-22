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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.bop.solutions.HTreeDistinctBindingSetsOp;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.QueryHintScope;

/**
 * A factory which may be used to register and resolve query hints.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryHintFactory {

    private static ConcurrentMap<String/* name */, IQueryHint<?>> registry = new ConcurrentHashMap<String/* name */, IQueryHint<?>>();

    /**
     * Register an {@link IQueryHint}.
     * 
     * @param The
     *            query hint.
     * 
     * @throws UnsupportedOperationException
     *             if there is already a {@link Factory} registered for that
     *             URI.
     */
    public static final void add(final IQueryHint<?> queryHint) {

        if (registry.putIfAbsent(queryHint.getName(), queryHint) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }

    }

    /**
     * Return the {@link IQueryHint} under that name.
     * 
     * @param name
     *            The name of the {@link IQueryHint}.
     *            
     * @return The {@link IQueryHint} -or- <code>null</code> if there is none
     *         registered for that name.
     */
    public static final IQueryHint<?> get(final String name) {
        
        return registry.get(name);
        
    }
    
    /*
     * Implementations.
     */

    /**
     * Exception thrown when a query hint is invalid/illegal.
     */
    public static final class QueryHintException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public QueryHintException(final QueryHintScope scope, final ASTBase op,
                final String name, final Object value) {

            super("scope=" + scope + ", name=" + name + ", value=" + value
                    + ", op=" + op.getClass().getSimpleName());

        }
        
    }
    
    /**
     * Query hint to run a join first in a join group.
     */
    private static final class RunFirstHint extends AbstractBooleanQueryHint {

        protected RunFirstHint() {

            super(QueryHints.RUN_FIRST, null/* default */);

        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (scope == QueryHintScope.Prior && op instanceof IJoinNode) {

                super.attach(context, scope, op, value);

            }

            throw new QueryHintException(scope, op, getName(), value);

        }

    }

    /**
     * Query hint to run a join last in a join group.
     */
    private static final class RunLastHint extends AbstractBooleanQueryHint {

        protected RunLastHint() {

            super(QueryHints.RUN_LAST, null/* default */);

        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (scope == QueryHintScope.Prior && op instanceof IJoinNode) {

                super.attach(context, scope, op, value);

            }

            throw new QueryHintException(scope, op, getName(), value);

        }

    }
    
    /**
     * The query hint governing the choice of the join order optimizer.
     */
    private static final class JoinOrderOptimizerQueryHint implements
            IQueryHint<QueryOptimizerEnum> {

        @Override
        public String getName() {
            return QueryHints.OPTIMIZER;
        }

        @Override
        public QueryOptimizerEnum getDefault() {
            return QueryOptimizerEnum.Static;
        }

        @Override
        public QueryOptimizerEnum validate(final String value) {
            
            return QueryOptimizerEnum.valueOf(value);
            
        }

        @Override
        public void attach(final AST2BOpContext ctx,
                final QueryHintScope scope, final ASTBase op,
                final QueryOptimizerEnum value) {

            switch (scope) {
            case Group:
            case GroupAndSubGroups:
            case Query:
            case SubQuery:
                // TODO Set annotation to typed value.
                op.setQueryHint(getName(), value.toString());
                return;
            }
            throw new QueryHintException(scope, op, getName(), value);

        }

    }

    /**
     * Base class for query hints.
     */
    private static abstract class AbstractQueryHint<T> implements IQueryHint<T> {

        private final String name;

        private final T defaultValue;

        protected AbstractQueryHint(final String name, final T defaultValue) {
            
            if (name == null)
                throw new IllegalArgumentException();
            
            if (defaultValue == null)
                throw new IllegalArgumentException();
            
            this.name = name;
            
            this.defaultValue = defaultValue;
            
        }

        @Override
        final public String getName() {
            return name;
        }

        @Override
        final public T getDefault() {
            return defaultValue;
        }

        @Override
        public void attach(final AST2BOpContext ctx,
                final QueryHintScope scope, final ASTBase op,
                final T value) {

            // TODO Set annotation to typed value.
            op.setQueryHint(getName(), value.toString());

        }

    }

    /**
     * Base class for {@link Boolean} query hints.
     */
    private static abstract class AbstractBooleanQueryHint extends
            AbstractQueryHint<Boolean> {

        protected AbstractBooleanQueryHint(final String name,
                final Boolean defaultValue) {

            super(name, defaultValue);

        }

        @Override
        public Boolean validate(final String value) {

            return Boolean.valueOf(value);

        }

    }

    /**
     * Base class for {@link Integer} query hints.
     */
    private static abstract class AbstractIntQueryHint extends
            AbstractQueryHint<Integer> {

        protected AbstractIntQueryHint(final String name,
                final Integer defaultValue) {

            super(name, defaultValue);

        }

        @Override
        public Integer validate(final String value) {

            return Integer.valueOf(value);

        }

    }

    /**
     * Base class for {@link Long} query hints.
     */
    private static abstract class AbstractLongQueryHint extends
            AbstractQueryHint<Long> {

        protected AbstractLongQueryHint(final String name,
                final Long defaultValue) {

            super(name, defaultValue);

        }

        @Override
        public Long validate(final String value) {

            return Long.valueOf(value);

        }

    }

    /**
     * Query hint for turning analyic query on/off.
     */
    private static final class AnalyticQueryHint extends
            AbstractBooleanQueryHint {

        protected AnalyticQueryHint() {
            super(QueryHints.ANALYTIC, QueryHints.DEFAULT_ANALYTIC);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            switch (scope) {
            case Query:
                context.nativeHashJoins = value;
                context.nativeDistinctSolutions = value;
                context.nativeDistinctSPO = value;
                return;
            }

            throw new QueryHintException(scope, op, getName(), value);

        }

    }

    /**
     * Query hint for turning the {@link HTreeDistinctBindingSetsOp} on/off.
     */
    private static final class NativeDistinctQueryHint extends
            AbstractBooleanQueryHint {

        protected NativeDistinctQueryHint() {
            super(QueryHints.NATIVE_DISTINCT_SOLUTIONS,
                    QueryHints.DEFAULT_NATIVE_DISTINCT_SOLUTIONS);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (scope == QueryHintScope.Query) {

                context.nativeDistinctSolutions = value;

            }

            if (op instanceof QueryBase) {

                super.attach(context, scope, op, value);

            }

        }

    }

    /**
     * Query hint for turning the {@link NativeDistinctFilter} on/off.
     */
    private static final class NativeDistinctSPOHint extends AbstractBooleanQueryHint {

        protected NativeDistinctSPOHint() {
            super(QueryHints.NATIVE_DISTINCT_SPO,
                    QueryHints.DEFAULT_NATIVE_DISTINCT_SPO);
        }
        
        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (scope == QueryHintScope.Query) {
                context.nativeDistinctSPO = value;
            } else {
                super.attach(context, scope, op, value);
            }

        }

    }
    
    /**
     * Query hint for turning the {@link NativeDistinctFilter} on/off.
     */
    private static final class NativeDistinctSPOThresholdHint extends
            AbstractLongQueryHint {

        protected NativeDistinctSPOThresholdHint() {
            super(QueryHints.NATIVE_DISTINCT_SPO_THRESHOLD,
                    QueryHints.DEFAULT_NATIVE_DISTINCT_SPO_THRESHOLD);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op, final Long value) {

            if (scope == QueryHintScope.Query) {

                context.nativeDistinctSPOThreshold = value;
                
            } else {
                
                super.attach(context, scope, op, value);
                
            }

        }

    }

    /**
     * Query hint for turning the enabling/disabling native hash joins using
     * the {@link HTree}.
     */
    private static final class NativeHashJoinsHint extends AbstractBooleanQueryHint {

        protected NativeHashJoinsHint() {
            super(QueryHints.NATIVE_HASH_JOINS,
                    QueryHints.DEFAULT_NATIVE_HASH_JOINS);
        }
        
        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (scope == QueryHintScope.Query) {

                context.nativeHashJoins = value;
                
                return;
                
            }

            throw new QueryHintException(scope, op, getName(), value);

        }

    }
    
    /**
     * Query hint for turning the enabling/disabling merge joins.
     */
    private static final class MergeJoinHint extends AbstractBooleanQueryHint {

        protected MergeJoinHint() {
            super(QueryHints.MERGE_JOIN, QueryHints.DEFAULT_MERGE_JOIN);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            switch (scope) {
            case Group:
            case GroupAndSubGroups:
            case Query:
            case SubQuery:
                break;
            case Prior:
                throw new QueryHintException(scope, op, getName(), value);
            }

            if (op instanceof JoinGroupNode) {

                super.attach(context, scope, op, value);

            }

        }

    }
    
    /**
     * Query hint for turning the enabling/disabling the use of remote access
     * paths on a cluster.
     * 
     * TODO Should be allowed on a BGP basis, but only supported right now as a
     * query-wide hint.
     */
    private static final class RemoteAPHint extends AbstractBooleanQueryHint {

        protected RemoteAPHint() {
            super(QueryHints.REMOTE_APS, QueryHints.DEFAULT_REMOTE_APS);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            switch (scope) {
            case Query:
                if(op instanceof QueryRoot) {
                    context.remoteAPs = value;
                }
                return;
            default:
                throw new QueryHintException(scope, op, getName(), value);
            }

        }

    }

    /**
     * Query hint for turning the controlling the #of access paths to sample
     * when making a decision between a SCAN+FILTER or PARALLEL SUBQUERY plan
     * for a named or default graph access path.
     */
    private static final class AccessPathSampleLimitHint extends
            AbstractIntQueryHint {

        protected AccessPathSampleLimitHint() {
            super(QueryHints.ACCESS_PATH_SAMPLE_LIMIT,
                    QueryHints.DEFAULT_ACCESS_PATH_SAMPLE_LIMIT);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Integer value) {

            if (op instanceof StatementPatternNode) {

                super.attach(context, scope, op, value);

            }

        }

    }

    /**
     * For named and default graph access paths where access path cost
     * estimation is disabled by setting the {@link #ACCESS_PATH_SAMPLE_LIMIT}
     * to ZERO (0), this query hint determines whether a SCAN + FILTER or
     * PARALLEL SUBQUERY (aka as-bound data set join) approach.
     */
    private static final class AccessPathScanAndFilterHint extends
            AbstractBooleanQueryHint {

        protected AccessPathScanAndFilterHint() {
            super(QueryHints.ACCESS_PATH_SCAN_AND_FILTER,
                    QueryHints.DEFAULT_ACCESS_PATH_SCAN_AND_FILTER);
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final Boolean value) {

            if (op instanceof StatementPatternNode) {

                super.attach(context, scope, op, value);

            }

        }

    }

    /**
     * Specify the {@link UUID} of the query.
     */
    private static final class QueryIdHint extends AbstractQueryHint<UUID> {

        protected QueryIdHint() {
        
            super(QueryHints.QUERYID, null/* defaultValue */);
            
        }

        @Override
        public UUID validate(final String value) {

            return UUID.fromString(value);
            
        }

        @Override
        public void attach(final AST2BOpContext context,
                final QueryHintScope scope, final ASTBase op,
                final UUID value) {

            if (op instanceof QueryRoot) {

                super.attach(context, scope, op, value);

                return;
                
            }
            
            throw new QueryHintException(scope, op, getName(), value);

        }
        
    }
    
    static {

        add(new RunFirstHint());
        add(new RunLastHint());
        add(new JoinOrderOptimizerQueryHint());

        add(new AnalyticQueryHint());
        add(new NativeDistinctQueryHint());
        add(new NativeDistinctSPOHint());
        add(new NativeDistinctSPOThresholdHint());
        add(new NativeHashJoinsHint());
        add(new MergeJoinHint());
        add(new RemoteAPHint());
        add(new AccessPathSampleLimitHint());
        add(new AccessPathScanAndFilterHint());

        add(new QueryIdHint());

    }

}
