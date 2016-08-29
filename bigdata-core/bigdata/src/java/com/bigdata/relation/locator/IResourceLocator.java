/*

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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.locator;

import com.bigdata.relation.IDatabase;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IBigdataFederation;

/**
 * An object that knows how to resolve a resource identifier (aka namespace) to
 * an {@link ILocatableResource} instance. "Locating" a relation means (a)
 * resolving the namespace for the resource to the resource declaration as of
 * some timestamp; and (b) materializing (often from cache) an
 * {@link ILocatableResource} for that resource. Locatable resources are
 * essentially containers for indices, including an {@link IRelation} and an
 * {@link IDatabase}, which is essentially a container for {@link IRelation}s.
 * <p>
 * Note: a locator does not proxy for a resource. Either the resource is locally
 * accessible, including the case of a resource distributed across a federation,
 * or it is not since the resource is accessible using the
 * {@link IBigdataFederation}'s API. However, you CAN NOT "locate" a
 * <em>local</em> resource on another JVM or machine using this interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            The generic [T]ype of the resource.
 */
public interface IResourceLocator<T extends ILocatableResource> {

    /**
     * Locate.
     * 
     * @param namespace
     *            The resource namespace.
     * @param timestamp
     *            The timestamp for the view.
     * 
     * @return A view of the resource iff it exists (specifically, if the
     *         declaration for the resource can be resolved) -or-
     *         <code>null</code> if the resource declaration could not be
     *         resolved.
     */
    public T locate(String namespace, long timestamp);
    
    /**
     * Resources that hold hard references to local index objects MUST be
     * discarded during abort processing. Otherwise the same resource objects
     * will be returned from the cache and buffered writes on the indices for
     * those relations (if they are local index objects) will still be visible,
     * thus defeating the abort semantics.
     * 
     * @param instance
     *            The instance whose cached references should be discarded.
     * @param destroyed
     *            <code>true</code> iff the resource was destroyed, in which
     *            case the read-committed and unisolated views must also be
     *            discarded even if they do not correspond to the
     *            <i>instance</i>.
     */
    public void discard(final ILocatableResource<T> instance,boolean destroyed);

    /**
     * Discard unisolated resource views from the locator cache.
     *         
     * @see BLZG-2023, BLZG-2041.
     */
    public void clearUnisolatedCache();
    
}
