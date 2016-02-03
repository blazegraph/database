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

/**
 * Configuration object.
 * 
 * @see ConfigParams
 */
public class SparqlEndpointConfig {

    /**
     * The default namespace.
     * 
     * @see ConfigParams#NAMESPACE
     */
    final public String namespace;

    /**
     * The default timestamp used to query the default namespace. The server
     * will obtain a read only transaction which reads from the commit point
     * associated with this timestamp.
     * <p>
     * Note: When {@link ConfigParams#READ_LOCK} is specified, the
     * {@link #timestamp} will actually be a read-only transaction identifier
     * which is shared by default for each query against the
     * {@link NanoSparqlServer}.
     * 
     * @see ConfigParams#READ_LOCK
     */
    final public long timestamp;

    /**
     * The #of threads to use to handle SPARQL queries -or- ZERO (0) for an
     * unbounded pool.
     * 
     * @see ConfigParams#QUERY_THREAD_POOL_SIZE
     */
    final public int queryThreadPoolSize;
    
    /**
     * When <code>true</code> and the KB instance is in the <code>quads</code>
     * mode, each named graph will also be described in in the same level of
     * detail as the default graph. Otherwise only the default graph will be
     * described.
     * 
     * @see ConfigParams#DESCRIBE_EACH_NAMED_GRAPH
     */
    final public boolean describeEachNamedGraph;

    /**
     * When <code>true</code>, requests will be refused for mutation operations
     * on the database made through the REST API. This may be used to help lock
     * down a public facing interface.
     * 
     * @see ConfigParams#READ_ONLY
     */
    final public boolean readOnly;

    /**
     * When non-zero, this specifies the timeout (milliseconds) for a query.
     * This may be used to limit resource consumption on a public facing
     * interface.
     * 
     * @see ConfigParams#QUERY_TIMEOUT
     */
    final public long queryTimeout;
    
    public SparqlEndpointConfig(final String namespace, final long timestamp,
            final int queryThreadPoolSize,
            final boolean describeEachNamedGraph, final boolean readOnly,
            final long queryTimeout) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (queryTimeout < 0L)
            throw new IllegalArgumentException();

        this.namespace = namespace;

        this.timestamp = timestamp;

        this.queryThreadPoolSize = queryThreadPoolSize;

        this.describeEachNamedGraph = describeEachNamedGraph;
        
        this.readOnly = readOnly;
        
        this.queryTimeout = queryTimeout;
        
    }

}
