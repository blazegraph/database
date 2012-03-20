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
 * Configuration object.
 */
public class SparqlEndpointConfig {

    /**
     * The default namespace.
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
     */
    final public long timestamp;

    /**
     * The #of threads to use to handle SPARQL queries -or- ZERO (0) for an
     * unbounded pool.
     */
    final public int queryThreadPoolSize;
    
    public SparqlEndpointConfig(final String namespace, final long timestamp,
            final int queryThreadPoolSize) {

        if (namespace == null)
            throw new IllegalArgumentException();

        this.namespace = namespace;

        this.timestamp = timestamp;

        this.queryThreadPoolSize = queryThreadPoolSize;

    }

}
