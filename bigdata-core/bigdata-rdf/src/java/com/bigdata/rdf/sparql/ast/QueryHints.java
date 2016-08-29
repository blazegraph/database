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
 * Created on Jan 2, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.UUID;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.engine.IChunkHandler;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.sparql.ast.cache.CacheConnectionFactory;
import com.bigdata.rdf.sparql.ast.hints.QueryHintRegistry;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;
import com.bigdata.rdf.sparql.ast.optimizers.ASTDistinctTermScanOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTFastRangeCountOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinGroupOrderOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinOrderByTypeOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.util.Bytes;
import com.bigdata.util.ClassPathUtil;

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
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" > Clean up
 *      query hints </a>
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
     * The sampling bias for the runtime query optimizer. Dense sampling
     * maximizes index locality but reduces robustness to correlations that do
     * not exist in the head of the access path key range. Random sampling
     * maximizes robustness, but pays a heavy IO cost. Even sampling also
     * increases robustness, but will visit every Nth tuple and pays a heavy IO
     * cost as a result. Thus dense sampling should be much faster but random or
     * even sampling should detect bias that might not otherwise be exposed to
     * the runtime query optimizer.
     * 
     * @see SampleType
     */
    String RTO_SAMPLE_TYPE = "RTO-sampleType";

    SampleType DEFAULT_RTO_SAMPLE_TYPE = SampleType.DENSE;

    /**
     * The limit for sampling a vertex and the initial limit for cutoff join
     * evaluation (default {@value #DEFAULT_RTO_LIMIT}). A larger limit and a
     * random sample will provide a more accurate estimate of the cost of the
     * join paths but are increase the runtime overhead of the RTO optimizer.
     * Smaller value can lead to underflow in the cardinality estimates of the
     * cutoff joins resulting in a longer execution time for the RTO since more
     * paths may be explored or the explored paths must be deepened in order to
     * differentiate their costs. Values corresponding to up to the expected
     * number of triples on an index page should have the same IO cost since
     * there will be a single page read for the vertex and the output of the
     * join will be cutoff once the desired number of join results has been
     * produced.
     */
    String RTO_LIMIT = "RTO-limit";

    int DEFAULT_RTO_LIMIT = 100;

    /**
     * The <i>nedges</i> edges of the join graph having the lowest cardinality
     * will be used to generate the initial join paths (default
     * {@value #DEFAULT_NEDGES}). This must be a positive integer. The edges in
     * the join graph are sorted in order of increasing cardinality and up to
     * <i>nedges</i> of those edges having the lowest cardinality are used to
     * form the initial set of join paths. For each edge selected to form a join
     * path, the starting vertex will be the vertex of that edge having the
     * lower cardinality. If ONE (1), then only those join paths that start with
     * the two vertices having the lowest cardinality will be explored (this was
     * the published behavior for ROX). When greater than ONE, a broader search
     * of the join paths will be carried out.
     */
    String RTO_NEDGES = "RTO-nedges";

    int DEFAULT_RTO_NEDGES = 1;

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
     * The default is <code>false</code>. The default may be
     * overridden using the environment variable named
     * 
     * <pre>
     * com.bigdata.rdf.sparql.ast.QueryHints.analytic
     * </pre>
     * 
     * @see #NATIVE_DISTINCT_SPO
     * @see #NATIVE_DISTINCT_SOLUTIONS
     * @see #NATIVE_HASH_JOINS
     * @see #MERGE_JOIN
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-43" > Add System 
     *      property to enable analytic query mode. </a>
     */
    String ANALYTIC = "analytic";

    boolean DEFAULT_ANALYTIC = Boolean.valueOf(System.getProperty(
            QueryHints.class.getName() + "." + ANALYTIC, "false"));

    /**
     * The maximum amount of native heap memory that may be allocated for a
     * single query when using the analytic query mode -or- ZERO (0L) if no
     * limit should be imposed. When non-zero, queries that exceed this limit
     * will be broken with a memory allocation exception. Together with a limit
     * on the number of concurrent queries, this may be used to limit the amount
     * of native memory consumed by the query engine.
     * <p>
     * The default is ZERO (0) which implies no limit. The default may be
     * overridden using the environment variable named
     * 
     * <pre>
     * com.bigdata.rdf.sparql.ast.QueryHints.analyticMaxMemoryPerQuery
     * </pre>
     * <p>
     * Native memory allocations are made using a {@link DirectBufferPool}. The
     * per-query limit will be rounded up to a multiple of buffers based on the
     * configured buffer capacity. The default is a 1MB buffer, so the
     * granularity of the limit is multiples of 1MB.
     * 
     * @see DirectBufferPool
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     *      memory limit for analytic query mode. </a>
     */
    String ANALYTIC_MAX_MEMORY_PER_QUERY = "analyticMaxMemoryPerQuery";
    
    long DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY = Long.valueOf(System
            .getProperty(QueryHints.class.getName() + "."
                    + ANALYTIC_MAX_MEMORY_PER_QUERY, "0"));
    
    /**
     * Controls where the intermediate solutions output by operators will be
     * stored. Options include the managed object heap, the native heap, or
     * potentially some policy which stores things dynamically depending on the
     * size of the chunk or the total memory burden on the query engine.
     * <p>
     * The effective value of this property is determined by effective value of
     * the system property {@value #QUERY_ENGINE_CHUNK_HANDLER}.
     * 
     * @see BLZG-533 Vector query engine on native heap.
     */
    String QUERY_ENGINE_CHUNK_HANDLER = "queryEngineChunkHandler";

    IChunkHandler DEFAULT_QUERY_ENGINE_CHUNK_HANDLER = 
            ClassPathUtil.classForName(//
                    System.getProperty(QueryHints.class.getName() + "."+QUERY_ENGINE_CHUNK_HANDLER,
                          com.bigdata.bop.engine.ManagedHeapStandloneChunkHandler.class.getName()
//                            com.bigdata.bop.engine.NativeHeapStandloneChunkHandler.class.getName()
                            ), // preferredClassName,
                    null, // defaultClass,
                    IChunkHandler.class, // sharedInterface,
                    IChunkHandler.class.getClassLoader() // classLoader
              );
    
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
     * Query hint for disabling the DISTINCT SPO behavior for a CONSTRUCT QUERY
     * (default {@value #DEFAULT_CONSTRUCT_DISTINCT_SPO}). When disabled, the
     * CONSTRUCT will NOT eliminate duplicate triples from the constructed
     * graph. Note that CONSTRUCT automatically avoids duplicate detection and
     * removal for cases where a CONSTRUCT is already "obviously" distinct. Thus
     * this query hint is only required if you have a very large graph and want
     * to stream the graph out without imposing the distinct SPO filter. You can
     * also use {@link #ANALYTIC} query hint to use the native heap for the
     * DISTINCT SPO filter. Thus this query hint is really only for very large
     * graphs.
     * 
     * @see https://jira.blazegraph.com/browse/BLZG-1341 (performance of dumping
     *      single graph)
     */
    String CONSTRUCT_DISTINCT_SPO = "constructDistinctSPO";
    
    boolean DEFAULT_CONSTRUCT_DISTINCT_SPO = true;
    
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
    String QUERYID = "queryId";

    /**
     * This query hint may be applied to any {@link IJoinNode} and marks a
     * particular join to be run first among in a particular group. Only one
     * "run first" join is permitted in a given group. This query hint is not
     * permitted on optional joins. This hint must be used with
     * {@link QueryHintScope#Prior}.
     */
    String RUN_FIRST = "runFirst";

    /**
     * This query hint may be applied to any {@link IJoinNode} and marks a
     * particular join to be run last among in a particular group. Only one
     * "run last" join is permitted in a given group. This hint must be used
     * with {@link QueryHintScope#Prior}.
     */
    String RUN_LAST = "runLast";

    /**
     * Query hint indicating whether or not a Sub-Select should be transformed
     * into a <em>named subquery</em>, lifting its evaluation out of the main
     * body of the query and replacing the subquery with an INCLUDE. This hint
     * must be used with {@link QueryHintScope#SubQuery}.
     * <p>
     * This is similar to {@link #AT_ONCE 'atOnce'} evaluation, but creates a
     * different query plan by lifting out a named subquery. The
     * {@link #RUN_ONCE} query hint is only supported for
     * {@link QueryHintScope#SubQuery} while {@link #AT_ONCE} query hint can be
     * applied to other things as well.
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
    String RUN_ONCE = "runOnce";

    /**
     * Query hint indicating whether or not a JOIN (including SERVICE,
     * SUB-SELECT, etc) should be run as an "atOnce" operator. All solutions for
     * an "atOnce" operator are materialized before the operator is evaluated.
     * It is then evaluated against those materialized solutions exactly once.
     * <p>
     * Note: "atOnce" evaluation is a general property of the query engine. This
     * query hint does not change the structure of the query plan, but simply
     * serves as a directive to the query engine that it should buffer all
     * source solutions before running the operator. This is more general
     * purpose than the {@link #RUN_ONCE} query hint.
     * <p>
     * This query hint is allowed in any scope. The hint is transferred as an
     * annotation onto all query plan operators generated from the annotated
     * scope.
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
     * Sets the target chunk size (aka vector size) for the output buffer of the operator.
     * <p>
     * This query hint does not change the structure of the query plan, but
     * simply serves as a directive to the query engine that it should allocate
     * an output buffer for the operator that will emit chunks of the indicated
     * target capacity. This query hint is allowed in any scope, but is
     * generally used to effect the behavior of a join group, a subquery, or the
     * entire query.
     * 
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    String CHUNK_SIZE = "chunkSize";
    
    /**
     * The maximum parallelism for the operator within the query.
     * <p>
     * Note: "maxParallel" evaluation is a general property of the query engine.
     * This query hint does not change the structure of the query plan, but
     * simply serves as a directive to the query engine that it should not allow
     * more than the indicated number of parallel instances of the operator to
     * execute concurrently. This query hint is allowed in any scope. The hint is
     * transferred as an annotation onto all query plan operators generated from
     * the annotated scope.
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
     * When <code>true</code> a DESCRIBE cache will be maintained. This can
     * accelerate DESCRIBE queries, linked data queries (which are mapped to a
     * DESCRIBE query by the NSS), and potentially accelerate star-joins (if the
     * query plan is rewritten to hit the DESCRIBE cache and obtain the
     * materialized joins from it, but this is best done with a fully
     * materialized and synchronously maintained DESCRIBE cache).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
     *      DESCRIBE CACHE </a>
     */
    String DESCRIBE_CACHE = "describeCache";
    
    boolean DEFAULT_DESCRIBE_CACHE = false;

    /**
     * FIXME Hack enables the cache feature if the describe cache is enabled.
     * 
     * @see CacheConnectionFactory#getCacheConnection(QueryEngine)
     */
    boolean CACHE_ENABLED = DEFAULT_DESCRIBE_CACHE;

    /**
     * Query hint controls the manner in which a DESCRIBE query is evaluated.
     * 
     * @see DescribeModeEnum
     * @see #DEFAULT_DESCRIBE_MODE
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578">
     *      Concise Bounded Description </a>
     */
    String DESCRIBE_MODE = "describeMode";

    DescribeModeEnum DEFAULT_DESCRIBE_MODE = DescribeModeEnum.SymmetricOneStep;
    
    /**
     * For iterative {@link DescribeModeEnum}s, this property places a limit on
     * the number of iterative expansions that will be performed before the
     * DESCRIBE query is cut off, providing that the limit on the maximum #of
     * statements in the description is also satisfied (the cut off requires
     * that both limits are reached).  May be ZERO (0) for NO limit.
     * 
     * @see #DESCRIBE_MODE
     * @see #DESCRIBE_STATEMENT_LIMIT
     */
    String DESCRIBE_ITERATION_LIMIT = "describeIterationLimit";

    int DEFAULT_DESCRIBE_ITERATION_LIMIT = 5;

    /**
     * For iterative {@link DescribeModeEnum}s, this property places a limit on
     * the number of statements that will be accumulated before the DESCRIBE
     * query is cut off, providing that the limit on the maximum #of iterations
     * in the description is also satisfied (the cut off requires that both
     * limits are reached). May be ZERO (0) for NO limit.
     * 
     * @see #DESCRIBE_MODE
     * @see #DESCRIBE_ITERATION_LIMIT
     */
    String DESCRIBE_STATEMENT_LIMIT = "describeStatementLimit";

    int DEFAULT_DESCRIBE_STATEMENT_LIMIT = 5000;

    /**
	 * Option controls whether or not the proposed SPARQL extension for
	 * reification done right is enabled.
	 * 
	 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
	 *      Reification Done Right</a>
	 */
    String REIFICATION_DONE_RIGHT = "reificationDoneRight";

    boolean DEFAULT_REIFICATION_DONE_RIGHT = true;
    
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
     * @see JoinAnnotations#LIMIT
     */
    String CUTOFF_LIMIT = "cutoffLimit";
 
    /**
     * Used to specify the query plan for FILTER (NOT) EXISTS. There are two
     * basic plans: vectored sub-plan and subquery with LIMIT ONE. Each plan has
     * its advantages.
     * 
     * @see FilterExistsModeEnum
     * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for
     *      FILTER EXISTS </a>
     */
    String FILTER_EXISTS = "filterExists";

    /**
     * Note: The historical behavior up through bigdata release 1.3.1 is
     * {@link FilterExistsModeEnum#VectoredSubPlan}.
     */
    FilterExistsModeEnum DEFAULT_FILTER_EXISTS = FilterExistsModeEnum.VectoredSubPlan;

	/*
	 * FIXME I have added system property based query hints that can be used to
	 * disable the fast-range-count and distinct-term-scan optimizers in case we
	 * run into more edge cases. These query hints can be removed once we have
	 * more experience with these optimizers.
	 */
    
    /**
	 * The name of an property that may be used to enable or disable the
	 * {@link ASTFastRangeCountOptimizer}.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
	 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD
	 *      </a>
	 */
    String FAST_RANGE_COUNT_OPTIMIZER = "fastRangeCountOptimizer";

	boolean DEFAULT_FAST_RANGE_COUNT_OPTIMIZER = Boolean.valueOf(System
			.getProperty(FAST_RANGE_COUNT_OPTIMIZER, "true"));
    
    /**
	 * The name of an property that may be used to enable or disable the
	 * {@link ASTDistinctTermScanOptimizer}.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
	 *      query is slow </a>
	 */
    String DISTINCT_TERM_SCAN_OPTIMIZER = "distinctTermScanOptimizer";

	boolean DEFAULT_DISTINCT_TERM_SCAN_OPTIMIZER = Boolean.valueOf(System
			.getProperty(DISTINCT_TERM_SCAN_OPTIMIZER, "true"));

   /**
    * The name of the subclass derived from {@link ASTOptimizerList} that will
    * be used to optimize SPARQL QUERY and UPDATE requests. This class MUST
    * implement a public zero argument constructor.
    * 
    * @see #DEFAULT_AST_OPTIMIZER_CLASS
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/1113"> Hook to configure the
    *      ASTOptimizerList </a>
    */
   String AST_OPTIMIZER_CLASS = "ASTOptimizerClass";

   String DEFAULT_AST_OPTIMIZER_CLASS = System.getProperty(
         AST_OPTIMIZER_CLASS, DefaultOptimizerList.class.getName());
   
   /**
    * Switch to re-enable old, {@link ASTJoinOrderByTypeOptimizer} (which was
    * the predecessor of the {@link ASTJoinGroupOrderOptimizer}. By default,
    * the new strategy is enabled.
    * 
    * @see #OLD_JOIN_ORDER_OPTIMIZER
    */
   String OLD_JOIN_ORDER_OPTIMIZER = "OldJoinOrderOptimizer";

   
   /**
    * Used to mark a predicate for historical read.  When history mode is 
    * enabled, statements are not actually deleted, they are just marked as
    * history using StatementEnum.History.  By default these historical SPOs
    * are hidden from view during read.
    */
   String HISTORY = "history";
       

   boolean DEFAULT_OLD_JOIN_ORDER_OPTIMIZER = Boolean.valueOf(
         System.getProperty(OLD_JOIN_ORDER_OPTIMIZER, "false"));
   
   /**
    * Switch to disable normalization/decomposition of FILTER expressions. There 
    * might be two scenarios where decomposition of FILTER expressions comes 
    * with a significant overhead: (i) whenever large complex FILTERs are used
    * or (ii) when there are unselective parts of FILTERs that are evaluated too
    * early when decomposing FILTERs (cd. SP2B Q6).
    * 
    * @see #DEFAULT_NORMALIZE_FILTER_EXPRESSIONS
    */
   String NORMALIZE_FILTER_EXPRESSIONS = "normalizeFilterExpressions";

   boolean DEFAULT_NORMALIZE_FILTER_EXPRESSIONS = Boolean.valueOf(
         System.getProperty(NORMALIZE_FILTER_EXPRESSIONS, "false"));
   
   /**
    * Prefer pipelined hash join over its normal, non-pipelined variant.
    * If set to true, this parameter can still be overridden by query hint.
    * If set to false, the pipelined hash join will only be used in
    * some exceptional cases (i.e., for LIMIT queries in which pipelining 
    * brings a clear benefit). 
    */
   String PIPELINED_HASH_JOIN = "pipelinedHashJoin";

   boolean DEFAULT_PIPELINED_HASH_JOIN = Boolean.valueOf(System.getProperty(
           QueryHints.class.getName() + "." + PIPELINED_HASH_JOIN, "false"));
   
   /**
    * By default, a DISTINCT filter is applied when evaluating access paths
    * against the default graph, for correctness reasons. The hint (or the 
    * respective system property) can be used to disabled the distinct filter,
    * in order to accelerate default graph queries. This can be done whenever
    * it is known that NO triple occurs in more than one named graph. 
    * 
    * BE CAREFUL: if this condition does not hold and the filter is disabled, 
    * wrong query results (caused by duplicates) will be the consequence.
    * 
    * Note that this hint only takes effect in quads mode.
    */
   String DEFAULT_GRAPH_DISTINCT_FILTER = "defaultGraphDistinctFilter";

   boolean DEFAULT_DEFAULT_GRAPH_DISTINCT_FILTER = 
       Boolean.valueOf(System.getProperty(
           QueryHints.class.getName() + "." + DEFAULT_GRAPH_DISTINCT_FILTER, "true"));
   
   /**
    * {@link https://jira.blazegraph.com/browse/BLZG-1200}
    * 
    * {@see https://www.w3.org/TR/sparql11-query/#func-regex}  
    * 
    * {@see https://www.w3.org/TR/sparql11-query/#restrictString}
    * 
    * By default, regex is only applied to Literal String values.   Enabling this
    * query hint will attempt to autoconvert non-String literals into their
    * string value.  This is the equivalent of always using the str(...) function.
    * 
    */
   String REGEX_MATCH_NON_STRING = "regexMatchNonString";

   boolean DEFAULT_REGEX_MATCH_NON_STRING = Boolean.valueOf(System.getProperty(
           QueryHints.class.getName() + "." + REGEX_MATCH_NON_STRING, "false"));

}
