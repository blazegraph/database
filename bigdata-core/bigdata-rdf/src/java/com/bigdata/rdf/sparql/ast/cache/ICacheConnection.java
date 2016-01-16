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
package com.bigdata.rdf.sparql.ast.cache;

/**
 * Interface for an abstraction used to support application specific local
 * caches, remote caches, and cache fabrics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ICacheConnection {

    /**
     * Initialize the cache / cache connection.
     */
    void init();
    
    /**
     * Close the cache / cache connection.
     */
    void close();

    /**
     * Destroy the cache(s) associated with that namespace.
     * 
     * @param namespace
     *            The KB namespace.
     */
    void destroyCaches(final String namespace,final long timestamp);

//    /**
//     * Return a view of the named SOLUTIONS cache.
//     * 
//     * @param namespace
//     *            The KB namespace.
//     * @param timestamp
//     *            The timestamp of the view.
//     * 
//     * @return The view of the named solutions cache -or- <code>null</code> if
//     *         no cache is available for that KB.
//     */
//    ISolutionSetManager getSparqlCache(final String namespace,
//            final long timestamp);

    /**
     * Return a view of a maintained DESCRIBE cache.
     * 
     * @param namespace
     *            The KB namespace.
     * @param timestamp
     *            The timestamp of the view.
     * 
     * @return The view of the maintained DESCRIBE cache -or- <code>null</code>
     *         if no cache is available for that KB.
     */
    IDescribeCache getDescribeCache(final String namespace, final long timestamp);

}
