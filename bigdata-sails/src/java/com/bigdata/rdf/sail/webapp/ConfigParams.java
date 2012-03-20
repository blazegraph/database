/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp;

/**
 * Interface declaring the <code>config-param</code>s understood by the
 * {@link BigdataRDFServletContextListener}.
 */
public interface ConfigParams {

    /**
     * The property file (for a standalone bigdata instance) or the jini
     * configuration file (for a bigdata federation). The file must end with
     * either ".properties" or ".config".
     */
    String PROPERTY_FILE = "property-file";

    /**
     * The default bigdata namespace of for the triple or quad store instance to
     * be exposed (default {@link #DEFAULT_NAMESPACE}). Note that there can be
     * many triple or quad store instances within a bigdata instance.
     */
    String NAMESPACE = "namespace";
    
    String DEFAULT_NAMESPACE = "kb";

    /**
     * When <code>true</code>, an instance of the specified {@link #NAMESPACE}
     * will be created if none exists.
     */
    String CREATE = "create";
    
    boolean DEFAULT_CREATE = true;
    
    /**
     * The size of the thread pool used to service SPARQL queries -OR- ZERO
     * (0) for an unbounded thread pool (default
     * {@value #DEFAULT_QUERY_THREAD_POOL_SIZE}).
     */
    String QUERY_THREAD_POOL_SIZE = "query-thread-pool-size";
    
    int DEFAULT_QUERY_THREAD_POOL_SIZE = 16;
    
    /**
     * Force a compacting merge of all shards on all data services in a
     * bigdata federation (optional, default <code>false</code>).
     * 
     * <strong>This option should only be used for benchmarking
     * purposes.</strong>
     */
    String FORCE_OVERFLOW = "force-overflow";

    /**
     * The commit time against which the server will assert a read lock by
     * holding open a read-only transaction against that commit point
     * (optional). When given, queries will default to read against this
     * commit point. Otherwise queries will default to read against the most
     * recent commit point on the database. Regardless, each query will be
     * issued against a read-only transaction.
     */
    String READ_LOCK = "read-lock";
    
}
