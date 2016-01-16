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
import java.util.Set;

import org.openrdf.model.URI;

/**
 * A collection of links into (edges in) or links out of (edges out) of an
 * {@link IGPO}.
 * 
 * <p>Note that the links out are only intended to be used to represent
 * many-many associations.  Standard one-many associations should be represented
 * by linksIn (many links pointing to one resource rather than many links from
 * one resource pointing to many other resources).
 * 
 * <p>The LinksOut as the low-cardinality part of a many-many association reflect
 * common asymmetry of such associations.  Experience suggests that if the 
 * association is more symmetric the cardinalities tend not to be large.
 */
public interface ILinkSet extends Set<IGPO> {
    
    /**
     * Returns <code>true</code> iff <i>o</i> is the same link set (same link
     * property and same container).
     * 
     * @see #getLinkProperty()
     * @see #getOwner()
     */
    boolean equals(Object o);

    /**
     * The generic object that is being pointed at by the members of the link
     * set.
     */
    IGPO getOwner();

    /**
     * The name of the property that the members of the link set use to point to
     * the generic object that is collecting this link set.
     */
    URI getLinkProperty();

    /**
     * <code>true</code> iff this link set models the links into the owner
     * (edges in) and <code>false</code> iff this link set models the links out
     * of the owner (edges out).
     */
    boolean isLinkSetIn();
    
    /**
     * The #of members in the link set.
     */
    int size();

    long sizeLong();

//    /**
//     * Returns an {@link Iterator} that visits the {@link IGPO} members of
//     * the {@link ILinkSet}. The {@link Iterator} SHOULD support concurrent
//     * modification of the link set membership. The {@link Iterator} SHOULD
//     * support {@link Iterator#remove()}.
//     */
//    Iterator<T> iterator();

    /**
     * Returns an {@link Iterator} that will visit objects that are instances of
     * the specificed class or interface (the backing {@link IGPO} objects
     * are wrapped by a suitable {@link IGenericSkin}).
     * 
     * @exception UnsupportedOperationException
     *                if <i>theClassOrInterface</i> is not a registered
     *                {@link IGenericSkin}.
     */
    <C> Iterator<C> iterator(Class<C> theClassOrInterface);

    <C> Iterator<C> statements();

//    /**
//     * Adds the generic object to the link set by appending it to the end of the
//     * link set. Does nothing if the generic object is already a member of the
//     * link set. (If you need to move the generic object to the end of the link
//     * set, you have to first remove it from the link set and then add it back
//     * in.)
//     * <p>
//     * 
//     * Note: Iff <i>g</i> was a member of a different link set for the property
//     * named by {@link #getLinkProperty()}, then <i>g</i> is
//     * <strong>removed</strong> from that link set as a side effect of this
//     * method. This is because the presence of <i>g</i> in a reverse link set is
//     * licensed by the forward link property <i>p</i> on <i>g</i>. Therefore
//     * <i>g</i> can belong to at most one reverse link set for a given property
//     * name <i>p</i>.
//     * <p>
//     * 
//     * @return <code>true</code> iff the generic object was not already a member
//     *         of the link set.
//     */
//    boolean add(IGenericSkin g);

//    /**
//     * Removes the generic object from the link set.
//     * 
//     * @return <code>true</code> iff the generic object was a member of the
//     *         link set. <code>false</code> is returned if the generic object
//     *         was NOT a member of the link set.
//     */
//
//    boolean remove( IGenericSkin g );
//        
//    /**
//     * Tests the generic object to determine whether or not it is a
//     * member of the link set (constant time operation).
//     *
//     * @return <code>true</code> iff the generic object is a member of
//     * the link set.
//     */
//
//    boolean contains( IGenericSkin g );

//    /**
//     * Clears the link set, which has the effect of clearing the link
//     * property on each generic object collected by the link set.  This
//     * method does NOT cause the generic objects in the link set to be
//     * removed from the store.
//     */
//
//    void clear();
}
