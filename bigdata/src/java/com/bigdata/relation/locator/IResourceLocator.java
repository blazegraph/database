/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.locator;

import com.bigdata.relation.IDatabase;
import com.bigdata.relation.IRelation;
import com.bigdata.service.IBigdataFederation;


/**
 * An object that knows how to resolve a resource identifer (aka namespace) to
 * an {@link ILocatableResource} instance. "Locating" a relation means (a)
 * resolving the identifier for the resource and (b) materializing (often from
 * cache) an {@link ILocatableResource} for that resource. Locatable resources
 * are essentially containers for indices, including an {@link IRelation} and an
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
     * @param identifier
     *            The resource identifier (aka namespace).
     * @param timestamp
     *            The timestamp for the view.
     * 
     * @return A view of the object iff it exists and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if there is an error when resolving the identifier.
     */
    public T locate(String namespace, long timestamp);
    
}
