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

package com.bigdata.gom.gpo;

import java.util.Iterator;

import com.bigdata.gom.skin.GenericSkinRegistry;

/**
 * A generic skin is a set of behaviors, typically disclosed by one or
 * more application interfaces, that can be layered over a generic
 * object.  The purpose of a generic skin is to allow the application
 * to view and interact with a generic object in terms of its own
 * interfaces.  Normally the property access methods of the
 * application interface are re-written in terms of generic
 * properties.  In addition, various methods that generate {@link
 * Iterator}s and the like are typically re-written in terms of link
 * sets.<p>
 *
 * This interface SHOULD be implemented so that the backing {@link
 * IGPO} implementation object may be recovered.  Only a single
 * method, {@link #asGeneric()} is required by this interface in order
 * to minimize the possibity of interface collisions when using a
 * generic object model to implement other interfaces, e.g., the W3C
 * DOM or some such.<p>
 *
 * Note: This interface is extended by {@link IGPO}.  This makes
 * it easy to write common code for {@link IGPO}s and {@link
 * IGenericSkin}s.<p>
 *
 * @see GenericSkinRegistry
 */

public interface IGenericSkin
{

    /**
     * This method returns the eventual delegate that implements the
     * {@link IGPO} interface and is primarily used to peel off a
     * skin and gain access to implementation specific methods on the
     * {@link IGPO} implementation object.  When invoked on an
     * object that directly implements the {@link IGPO} behavior
     * (vs delegating the interface to an implementation object), then
     * that object is returned.  The return value is never
     * <code>null</code>.<p>
     */
    public IGPO asGeneric();

}
