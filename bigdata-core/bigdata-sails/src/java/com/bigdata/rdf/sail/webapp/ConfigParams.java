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
package com.bigdata.rdf.sail.webapp;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;

/**
 * Interface declaring the <code>config-param</code>s understood by the
 * {@link BigdataRDFServletContextListener}.
 * <p>
 * Note: When used in a jini/River configuration, the name of the component for
 * those configuration options is the fully qualified class name for the
 * {@link NanoSparqlServer}.
 *
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/596"> Change
 *      web.xml parameter names to be consistent with Jini/River </a>
 */
public interface ConfigParams {

    /**
     * The property file (for a standalone bigdata instance) or the jini
     * configuration file (for a bigdata federation). The file must end with
     * either ".properties" or ".config". This parameter is ignored if the
     * {@link IIndexManager} is specified as an attribute of the web application
     * context.
     */
    final String PROPERTY_FILE = "propertyFile";

    /**
     * The default bigdata namespace of for the triple or quad store instance to
     * be exposed (default {@link #DEFAULT_NAMESPACE}). Note that there can be
     * many triple or quad store instances within a bigdata instance.
     */
    final String NAMESPACE = "namespace";
    
    final String DEFAULT_NAMESPACE = "kb";

    /**
     * When <code>true</code>, an instance of the specified {@link #NAMESPACE}
     * will be created if none exists.
     */
    final String CREATE = "create";
    
    final boolean DEFAULT_CREATE = true;
    
    /**
     * The size of the thread pool used to service SPARQL queries -OR- ZERO
     * (0) for an unbounded thread pool (default
     * {@value #DEFAULT_QUERY_THREAD_POOL_SIZE}).
     */
    final String QUERY_THREAD_POOL_SIZE = "queryThreadPoolSize";
    
    final int DEFAULT_QUERY_THREAD_POOL_SIZE = 16;
    
    /**
     * Force a compacting merge of all shards on all data services in a
     * bigdata federation (optional, default <code>false</code>).
     *
     * <strong>This option should only be used for benchmarking
     * purposes.</strong>
     */
    final String FORCE_OVERFLOW = "forceOverflow";

    /**
     * The commit time against which the server will assert a read lock by
     * holding open a read-only transaction against that commit point
     * (optional). When given, queries will default to read against this
     * commit point. Otherwise queries will default to read against the most
     * recent commit point on the database. Regardless, each query will be
     * issued against a read-only transaction.
     */
    final String READ_LOCK = "readLock";
    
    /**
     * When <code>true</code> and the KB instance is in the <code>quads</code>
     * mode, each named graph will also be described in in the same level of
     * detail as the default graph (default
     * {@value #DEFAULT_DESCRIBE_EACH_NAMED_GRAPH}). Otherwise only the default
     * graph will be described.
     * <p>
     * Note: I have changed the default to <code>false</code> since this
     * operation can be long-running for messy web graphs such as the TBL six
     * degrees of freedom crawl. We wind up with one named graph for each
     * "friend", which is a lot of named graphs, plus there are a lot of
     * predicates that show up in web data. Together, this makes the per-named
     * graph response not a very good default. However, you can certainly enable
     * this if you only have a reasonable number of named graphs and/or only
     * expose the SPARQL end point to a limited audience.
     */
    final String DESCRIBE_EACH_NAMED_GRAPH = "describeEachNamedGraph";
    
    final boolean DEFAULT_DESCRIBE_EACH_NAMED_GRAPH = false;
    
    /**
     * When <code>true</code>, requests will be refused for mutation operations
     * on the database made through the REST API. This may be used to help lock
     * down a public facing interface.
     */
    final String READ_ONLY = "readOnly";

    final boolean DEFAULT_READ_ONLY = false;

    /**
     * When non-zero, this specifies the timeout (milliseconds) for a query.
     * This may be used to limit resource consumption on a public facing
     * interface.
     */
    final String QUERY_TIMEOUT = "queryTimeout";

    final long DEFAULT_QUERY_TIMEOUT = 0L;

    /**
    * When non-zero, this specifies the timeout (milliseconds) for a warmup
    * period when the NSS starts up (warmup is disabled when this is ZERO).
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal
    *      on startup </a>
    */
    final String WARMUP_TIMEOUT = "warmupTimeout";

    final long DEFAULT_WARMUP_TIMEOUT = 0L;

    /**
    * The size of the thread pool used to warmup the journal (default
    * {@value #DEFAULT_WARMUP_THREAD_POOL_SIZE}).
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal
    *      on startup </a>
    */
    final String WARMUP_THREAD_POOL_SIZE = "warmupThreadPoolSize";
    
    final int DEFAULT_WARMUP_THREAD_POOL_SIZE = 20;
    
    /**
    * A comma delimited list of namespaces to be processed during the warmup 
    * procedure (optional). When not specified, ALL namespaces will be processed.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal
    *      on startup </a>
    */
    final String WARMUP_NAMESPACE_LIST = "warmupNamespaceList";
    
    String DEFAULT_WARMUP_NAMESPACE_LIST = "";
    
    /**
     * A class that implements the {@link BlueprintsServletProxy}.
     * 
     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1295"> Refactor Blueprints </a>
     * 
     */
    final String BLUEPRINTS_SERVLET_PROVIDER = "blueprintsServletProvider";
    
    final static String DEFAULT_BLUEPRINTS_SERVLET_PROVIDER = BlueprintsServletProxy.getDefaultProvider();

    /**
     * A class that implements the {@link MapgraphServletProxy}.
     */
    final String MAPGRAPH_SERVLET_PROVIDER = "mapgraphServletProvider";
    
    final static String DEFAULT_MAPGRAPH_SERVLET_PROVIDER = MapgraphServletProxy.getDefaultProvider();

    /**
     * List of the services this instance is allowed to call out to.
     */
    String SERVICE_WHITELIST = "serviceWhitelist";
}
