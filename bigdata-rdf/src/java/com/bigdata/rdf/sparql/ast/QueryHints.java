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

import com.bigdata.bop.BOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.optimizers.QueryHintScope;

/**
 * Query hint directives understood by a bigdata SPARQL end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QueryHints {

    /**
     * The namespace prefix used in SPARQL queries to signify query hints. You
     * can embed query hints into a SPARQL query as follows:
     * 
     * <pre>
     * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#name1=value1&amp;name2=value2&gt;
     * </pre>
     * 
     * where <i>name</i> is the name of a query hint and <i>value</i> is the
     * value associated with that query hint. Multiple query hints can be
     * specified (as shown in this example) using a <code>&amp;</code> character
     * to separate each name=value pair.
     * <p>
     * Query hints are either directives understood by the SPARQL end point or
     * {@link BOp.Annotations}. A list of the known directives is declared by
     * this interface.
     */
    String PREFIX = "BIGDATA_QUERY_HINTS";
    
    String NAMESPACE = "http://www.bigdata.com/queryHints#";

	/**
	 * Specify the query optimizer. For example, you can disable the query
	 * optimizer using
	 * 
	 * <pre>
	 * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sparql.ast.QueryHints.optimizer=None&gt;
	 * </pre>
	 * 
	 * Disabling the query optimizer can be useful if you have a query for which
	 * the static query optimizer is producing a inefficient join ordering. With
	 * the query optimizer disabled for that query, the joins will be run in the
	 * order given.  This makes it possible for you to decide on the right join
	 * ordering for that query.
	 * 
	 * @see QueryOptimizerEnum
	 */
    String OPTIMIZER = QueryHints.class.getName() + ".optimizer";

	/**
	 * A label which may be used to tag the instances of some SPARQL query
	 * template in manner which makes sense to the application (default
	 * {@value #DEFAULT_TAG}). The tag is used to aggregate performance
	 * statistics for tagged queries.
	 * 
	 * <pre>
	 * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sparql.ast.QueryHints.tag=Query12&gt;
	 * </pre>
	 * 
	 * @see http://sourceforge.net/apps/trac/bigdata/ticket/207 (Report on Top-N queries)
	 * @see http://sourceforge.net/apps/trac/bigdata/ticket/256 (Amortize RTO cost)
	 */
    String TAG = QueryHints.class.getName() + ".tag";

    /**
     * @see #TAG
     */
    String DEFAULT_TAG = "";
    
    // See AST2BOpBase.Annotations.HASH_JOINS
//	/**
//	 * If true, this query hint will let the evaluation strategy know it should
//	 * try to use the {@link SubqueryHashJoinOp} to perform a hash join between
//	 * subqueries.  Subqueries are identified in several ways: either an optional
//	 * join group, or a set of tails within one join group that create a cross
//	 * product if run normally (i.e. multiple free text searches).
//	 *  
//	 * <pre>
//	 * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sparql.ast.QueryHints.hashJoin=true&gt;
//	 * </pre>
//	 */
//    String HASH_JOIN = QueryHints.class.getName() + ".hashJoin";
//
//    /**
//     * @see #HASH_JOIN
//     */
//    String DEFAULT_HASH_JOIN = "false";

    /**
     * When <code>true</code>, will use the version of DISTINCT based on the
     * {@link HTree} and the native (C process) heap. When <code>false</code>,
     * use the version based on a JVM collection class. The JVM version does not
     * scale-up as well, but it offers higher concurrency.
     * 
     * @see AST2BOpBase#nativeDefaultGraph
     */
    String NATIVE_DISTINCT = QueryHints.class.getName() + ".nativeDistinct";

    boolean DEFAULT_NATIVE_DISTINCT = false;

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
    String NATIVE_HASH_JOINS = QueryHints.class.getName() + ".nativeHashJoins";

    boolean DEFAULT_HASH_JOINS = false;

    /**
     * When <code>true</code>, a merge-join pattern will be recognized if it
     * appears in a join group. When <code>false</code>, this can still be
     * selectively enabled using a query hint.
     */
    String MERGE_JOIN = QueryHints.class.getName() + ".mergeJoin";

    boolean DEFAULT_MERGE_JOIN = false;

    /**
     * When <code>true</code>, enables all query hints pertaining to analytic
     * query patterns.
     */
    String ANALYTIC = QueryHints.class.getName() + ".analytic";

    boolean DEFAULT_ANALYTIC = false;

    
    
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
     * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sparql.ast.QueryHints.queryId=36cff615-aaea-418a-bb47-006699702e45&gt;
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/283
     */
    String QUERYID = QueryHints.class.getName() + ".queryId";
    
}
