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
package com.bigdata.gom.om;

import org.openrdf.model.URI;

/**
 * The IIDGenerator interface is used to create default object URI
 * ids for new objects.  This can be exploited by various use cases where
 * a unique id can be conveniently managed by the system.
 * <p>
 * The interface allows applications to control the ID creation if required.
 * 
 * @author Martyn Cutcher
 *
 */
public interface IIDGenerator {

    /**
     * Generate a globally unique URI.
     * 
     * @return The URI.
     */
    URI genId();

    /**
     * Generate a globally unique URI.
     * 
     * @param scope
     *            The scope will be incorporated into the URI. This is not
     *            necessary to make the URI globally unique, but it can make it
     *            easier to embed some non-opaque semantics into a globally
     *            unique URI.
     * 
     * @return The URI.
     */
    URI genId(final String scope);

    /**
     * A rollback hook is required
     */
	void rollback();

}
