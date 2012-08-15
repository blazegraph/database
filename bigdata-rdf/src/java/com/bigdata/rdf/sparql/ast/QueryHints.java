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
 * Created on Jan 2, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.UUID;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.htree.HTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.sparql.ast.hints.QueryHintRegistry;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer;

/**
 * Query hints are directives understood by the SPARQL end point. A query hint
 * appears in the SPARQL query as a "virtual triple". A query hint is declared
 * in a {@link QueryHintScope}, which specifies the parts of the SPARQL query to
 * which it will be applied. A list of the common directives is declared by this
 * interface. (Query hints declared elsewhere are generally for internal use
 * only.) Note that not all query hints are permitted in all scopes.
 * 
 * @see QueryHintScope
 * @see QueryHintRegistry
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QueryHints {

//    /**
//     * The namespace prefix used in SPARQL queries to signify query hints. 
//     */
//    String PREFIX = "BIGDATA_QUERY_HINTS";

    /**
     * The namespace for the bigdata query hints.
     */
    String NAMESPACE = "http://www.bigdata.com/queryHints#";

    /**
     * Specify the join order optimizer. For example, you can disable the query
     * optimizer within some join group using
     * 
     * <pre>
     * hint:Group hint:optimizer "None".
     * </pre>
     * 
     * Disabling the join order optimizer can be useful if you have a query for
     * which the static optimizer is producing a inefficient join ordering. With
     * the query optimizer disabled for that query, the joins will be run in the
     * order given. This makes it possible for you to decide on the right join
     * ordering for that query.
     * 
     * @see QueryOptimizerEnum
     */
    String OPTIMIZER = "optimizer";//QueryHints.class.getName() + ".optimizer";

    QueryOptimizerEnum DEFAULT_OPTIMIZER = QueryOptimizerEnum.Static;

    /**
     * Query hint sets the optimistic threshold for the static join order
     * optimizer.
     */
    String OPTIMISTIC = "optimistic";

    double DEFAULT_OPTIMISTIC = ASTStaticJoinOptimizer.Annotations.DEFAULT_OPTIMISTIC;

//    /**
//     * A label which may be used to tag the instances of some SPARQL query
//     * template in manner which makes sense to the application (default
//     * {@value #DEFAULT_TAG}). The tag is used to aggregate performance
//     * statistics for tagged queries.
//     * 
//     * <pre>
//     * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sparql.ast.QueryHints.tag=Query12&gt;
//     * </pre>
//     * 
//     * @see http://sourceforge.net/apps/trac/bigdata/ticket/207 (Report on Top-N
//     *      queries)
//     * @see http://sourceforge.net/apps/trac/bigdata/ticket/256 (Amortize RTO
//     *      cost)
//     * 
//     * @deprecated This is not currently supported. The feature may or may not
//     *             be re-enabled.
//     */
//    String TAG = QueryHints.class.getName() + ".tag";
//
//    /**
//     * @see #TAG
//     */
//    String DEFAULT_TAG = "";
 
    /**
     * When <code>true</code>, enables all query hints pertaining to analytic
     * query patterns. When <code>false</code>, those features are disabled.
     * <p>
     * Note: This query hint MUST be applied in the {@link QueryHintScope#Query}
     * . Hash indices are often created by one operator and then consumed by
     * another so the same kinds of hash indices MUST be used throughout the
     * query.
     * 
     * <pre>
     * hint:Query hint:analytic "true".
     * </pre>
     * 
     * @see #NATIVE_DISTINCT_SPO
     * @see #NATIVE_DISTINCT_SOLUTIONS
     * @see #NATIVE_HASH_JOINS
     * @see #MERGE_JOIN
     */
    String ANALYTIC = "analytic";//QueryHints.class.getName() + ".analytic";

    boolean DEFAULT_ANALYTIC = false;

    /**
     * When <code>true</code>, will use the version of DISTINCT SOLUTIONS based
     * on the {@link HTree} and the native (C process) heap. When
     * <code>false</code>, use the version based on a JVM collection class. The
     * JVM version does not scale-up as well, but it offers higher concurrency.
     */
    String NATIVE_DISTINCT_SOLUTIONS = "nativeDistinctSolutions";
//            QueryHints.class.getName()+ ".nativeDistinctSolutions";

    boolean DEFAULT_NATIVE_DISTINCT_SOLUTIONS = DEFAULT_ANALYTIC;

    /**
     * When <code>true</code> and the range count of the default graph access
     * path exceeds the {@link #NATIVE_DISTINCT_SPO_THRESHOLD}, will use the
     * version of DISTINCT SPO for a hash join against a DEFAULT GRAPH access
     * path based on the {@link HTree} and the native (C process) heap. When
     * <code>false</code>, use the version based on a JVM collection class. The
     * JVM version does not scale-up as well.
     */
    String NATIVE_DISTINCT_SPO = "nativeDistinctSPO";
//            QueryHints.class.getName()+ ".nativeDistinctSPO";

    boolean DEFAULT_NATIVE_DISTINCT_SPO = DEFAULT_ANALYTIC;

    /**
     * The minimum range count for a default graph access path before the native
     * DISTINCT SPO filter will be used.
     * 
     * @see #NATIVE_DISTINCT_SPO
     */
    String NATIVE_DISTINCT_SPO_THRESHOLD = "nativeDistinctSPOThreshold";
//            QueryHints.class.getName()+ ".nativeDistinctSPOThreshold";

    long DEFAULT_NATIVE_DISTINCT_SPO_THRESHOLD = 100 * Bytes.kilobyte32;
    
    /**
     * When <code>true</code>, use hash index operations based on the
     * {@link HTree} and backed by the native (C process) heap. When
     * <code>false</code>, use hash index operations based on the Java
     * collection classes. The {@link HTree} is more scalable but has higher
     * overhead for small cardinality hash joins.
     * <p>
     * Note: This query hint MUST be applied in the {@link QueryHintScope#Query}
     * . Hash indices are often created by one operator and then consumed by
     * another so the same kinds of hash indices MUST be used throughout the
     * query.
     */
    String NATIVE_HASH_JOINS = "nativeHashJoins";
            //QueryHints.class.getName() + ".nativeHashJoins";

    boolean DEFAULT_NATIVE_HASH_JOINS = DEFAULT_ANALYTIC;

    /**
     * When <code>true</code>, a merge-join pattern will be recognized if it
     * appears in a join group. When <code>false</code>, this can still be
     * selectively enabled using a query hint.
     */
    String MERGE_JOIN = "mergeJoin";//QueryHints.class.getName() + ".mergeJoin";

    boolean DEFAULT_MERGE_JOIN = true;

    /**
     * When <code>true</code>, force the use of REMOTE access paths in scale-out
     * joins. This is intended as a tool when analyzing query patterns in
     * scale-out. It should normally be <code>false</code>.
     */
    String REMOTE_APS = "remoteAPs";//QueryHints.class.getName() + ".remoteAPs";

    /**
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/380#comment:4
     */
    boolean DEFAULT_REMOTE_APS = false;

    /**
     * The #of samples to take when comparing the cost of a SCAN with an IN
     * filter to as-bound evaluation for each graph in the data set (default
     * {@value #DEFAULT_ACCESS_PATH_SAMPLE_LIMIT}). The samples are taken from
     * the data set. Each sample is a graph (aka context) in the data set. The
     * range counts and estimated cost to visit the AP for each of the sampled
     * contexts are combined to estimate the total cost of visiting all of the
     * contexts in the NG or DG access path.
     * <p>
     * When ZERO (0), no cost estimation will be performed and the named graph
     * or default graph join will always use approach specified by the boolean
     * {@link #ACCESS_PATH_SCAN_AND_FILTER}.
     */
    String ACCESS_PATH_SAMPLE_LIMIT = "accessPathSampleLimit";
//            QueryHints.class.getName()+ ".accessPathSampleLimit";

    /**
     * Note: Set to ZERO to disable AP sampling for default and named graphs.
     */
    int DEFAULT_ACCESS_PATH_SAMPLE_LIMIT = 100;
    
    /**
     * For named and default graph access paths where access path cost
     * estimation is disabled by setting the {@link #ACCESS_PATH_SAMPLE_LIMIT}
     * to ZERO (0), this query hint determines whether a SCAN + FILTER or
     * PARALLEL SUBQUERY (aka as-bound data set join) approach.
     */
    String ACCESS_PATH_SCAN_AND_FILTER = "accessPathScanAndFilter";
//            QueryHints.class.getName()+ ".accessPathScanAndFilter";  

    /**
     * Note: To ALWAYS use either SCAN + FILTER or PARALLEL subquery, set
     * {@link #DEFAULT_ACCESS_PATH_SAMPLE_LIMIT} to ZERO (0) and set this to the
     * desired method for named graph and default graph evaluation. Note that
     * you MAY still override this behavior within a given scope using a query
     * hint.
     */
    boolean DEFAULT_ACCESS_PATH_SCAN_AND_FILTER = true;
    
    /**
     * The {@link UUID} to be assigned to the {@link IRunningQuery} (optional).
     * This query hint makes it possible for the application to assign the
     * {@link UUID} under which the query will run. This can be used to locate
     * the {@link IRunningQuery} using its {@link UUID} and gather metadata
     * about the query during its evaluation. The {@link IRunningQuery} may be
     * used to monitor the query or even cancel a query.
     * <p>
     * The {@link UUID} of each query MUST be distinct. When using this query
     * hint the application assumes responsibility for applying
     * {@link UUID#randomUUID()} to generate a unique {@link UUID} for the
     * query. The application may then discover the {@link IRunningQuery} using
     * {@link QueryEngineFactory#getQueryController(com.bigdata.journal.IIndexManager)}
     * and {@link QueryEngine#getQuery(UUID)}.
     * <p>
     * Note: The openrdf iteration interface has a close() method, but this can
     * not be invoked until hasNext() has run and the first solution has been
     * materialized. For queries which use an "at-once" operator, such as ORDER
     * BY, the query will run to completion before hasNext() returns. This means
     * that it is effectively impossible to interrupt a running query which uses
     * an ORDER BY clause from the SAIL. However, applications MAY use this
     * query hint to discovery the {@link IRunningQuery} interface and cancel
     * the query.
     * 
     * <pre>
     * hint:Query hint:queryId "36cff615-aaea-418a-bb47-006699702e45"
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/283
     */
    String QUERYID = "queryId";//QueryHints.class.getName() + ".queryId";

    /**
     * This query hint may be applied to any {@link IJoinNode} and marks a
     * particular join to be run first among in a particular group. Only one
     * "run first" join is permitted in a given group. This query hint is not
     * permitted on optional joins.
     */
    String RUN_FIRST = "runFirst";//QueryHints.class.getName() + ".runFirst";

    /**
     * This query hint may be applied to any {@link IJoinNode} and marks a
     * particular join to be run last among in a particular group. Only one
     * "run last" join is permitted in a given group.
     */
    String RUN_LAST = "runLast";//QueryHints.class.getName() + ".runLast";

    /**
     * Query hint indicating whether or not a Sub-Select should be transformed
     * into a <em>named subquery</em>, lifting its evaluation out of the main
     * body of the query and replacing the subquery with an INCLUDE. This is
     * similar to {@link #AT_ONCE 'at-once'} evaluation, but creates a different
     * query plan by lifting out a named subquery. It is also only supported for
     * a Sub-Select. The {@link #AT_ONCE} query hint can be applied to other
     * things as well.
     * <p>
     * When <code>true</code>, the subquery will be lifted out. When
     * <code>false</code>, the subquery will not be lifted unless other
     * semantics require that it be lifted out regardless.
     * <p>
     * For example, the following may be used to lift out the sub-select in
     * which it appears into a {@link NamedSubqueryRoot}. The lifted expression
     * will be executed exactly once.
     * 
     * <pre>
     * hint:SubQuery hint:runOnce "true" .
     * </pre>
     * 
     * @see #AT_ONCE
     */
    String RUN_ONCE = "runOnce";//QueryHints.class.getName() + ".runOnce";

    /**
     * Query hint indicating whether or not a JOIN (including SERVICE,
     * SUB-SELECT, etc) should be run as an "at-once" operator. All solutions
     * for an "at-once" operator are materialized before the operator is
     * evaluated. It is then evaluated against those materialized solutions
     * exactly once.
     * <p>
     * Note: "At-once" evaluation is a general property of the query engine.
     * This query hint does not change the structure of the query plan, but
     * simply serves as a directive to the query engine that it should buffer
     * all source solutions before running the operator. This is more general
     * purpose than the {@link #RUN_ONCE} query hint.
     * 
     * @see #RUN_ONCE
     * 
     *      TODO "Blocked" evaluation. Blocked evaluation is similar to at-once
     *      evaluation but lacks the strong guarantee of that the operator will
     *      run exactly once. For blocked evaluation, the solutions to be fed to
     *      the operator are buffered up to a memory limit. If that memory limit
     *      is reached, then the buffered solutions are vectored through the
     *      operator. If all solutions can be buffered within the memory limit
     *      then "at-once" and "blocked" evaluation amount to the same thing.
     */
    String AT_ONCE = "atOnce";

    /**
     * The target chunk (aka vector size) for the operator.
     * <p>
     * Note: The vectored query engine will buffer multiple chunks for an
     * operator before the producer(s) (the operator(s) feeding into the
     * annotated operator) must block.
     * 
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    String CHUNK_SIZE = "chunkSize";
    
    /**
     * The maximum parallelism for the operator within the query.
     * 
     * @see PipelineOp.Annotations#MAX_PARALLEL
     */
    String MAX_PARALLEL = "maxParallel";
    
    /**
     * Query hint to use a hash join against the access path for a given
     * predicate. Hash joins should be enabled once it is recognized that
     * the #of as-bound probes of the predicate will approach or exceed the
     * range count of the predicate.
     * <p>
     * Note: {@link HashJoinAnnotations#JOIN_VARS} MUST also be specified
     * for the predicate. The join variable(s) are variables which are (a)
     * bound by the predicate and (b) are known bound in the source
     * solutions. The query planner has the necessary context to figure this
     * out based on the structure of the query plan and the join evaluation
     * order.
     */
    String HASH_JOIN = "hashJoin";

    boolean DEFAULT_HASH_JOIN = false;

    /**
     * Option controls whether or not the bigdata extension to SPARQL Update for
     * named solution sets is enabled.
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
     *      SPARQL Update </a>
     */
    String SOLUTION_SET_CACHE = "solutionSetCache";

    boolean DEFAULT_SOLUTION_SET_CACHE = false;

    /**
     * When <code>true</code> a DESCRIBE cache will be maintained. This can
     * DESCRIBE queries, linked data queries (which are mapped to a DESCRIBE
     * query by the NSS), and potentially accelerate star-joins (if the query
     * plan is rewritten to hit the DESCRIBE cache and obtain the materialized
     * joins from it, but this is best done with a fully materialized and
     * synchronously maintained DESCRIBE cache).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
     *      DESCRIBE CACHE </a>
     */
    String DESCRIBE_CACHE = "describeCache";
    
    boolean DEFAULT_DESCRIBE_CACHE = false;
    
   /**
	 * Option controls whether or not the proposed SPARQL extension for
	 * reification done right is enabled.
	 * 
	 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
	 *      Reification Done Right</a>
	 */
    String REIFICATION_DONE_RIGHT = "reificationDoneRight";

    boolean DEFAULT_REIFICATION_DONE_RIGHT = false;
    
    /**
     * Used to mark a predicate as "range safe" - that is, we can safely
     * apply the range bop to constrain the predicate.  This can only be
     * used currently when there is a single datatype for attribute values.
     */
    String RANGE_SAFE = "rangeSafe";
        
    /**
     * Used to mark a statement pattern with a cutoff limit for how many
     * elements (maximum) should be read from its access path.  This
     * effectively limits the input into the join.
     * 
     * @see {@link Annotations#CUTOFF_LIMIT}.
     */
    String CUTOFF_LIMIT = "cutoffLimit";
        
}