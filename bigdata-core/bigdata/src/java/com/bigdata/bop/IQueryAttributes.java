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
/*
 * Created on Aug 16, 2011
 */

package com.bigdata.bop;

/**
 * An interface providing access to named attribute values which may be shared
 * across operators or across multiple invocations of the same operator. The
 * bindings are scoped to the owning {@link IQueryContext}.
 * <p>
 * The key MUST include enough information to provide for the scope of the
 * attribute within the query and is typically the bop identifier plus an
 * attribute name. Bindings DO NOT cross a service partition so it is not
 * necessary to add the shardId or service UUID to the key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IQueryAttributes {

    /**
     * Return the attribute value under the key.
     * 
     * @param key
     *            The key.
     * 
     * @return The attribute value under that key -or- <code>null</code> if
     *         there is no bound attribute for that key.
     */
    Object get(Object key);

    /**
     * Bind an attribute under the key.
     * 
     * @param key
     *            The key.
     * @param val
     *            The new value (may not be <code>null</code>).
     * 
     * @return The old value.
     * 
     * @see #get(Object)
     */
    Object put(Object key, Object val);

    /**
     * Atomically bind an attribute value under the key iff the key is not
     * already bound.
     * 
     * @param key
     *            The key.
     * @param val
     *            The new value (may not be <code>null</code>).
     * 
     * @return The old value.
     */
    Object putIfAbsent(Object key, Object val);

    /**
     * Remove the attribute under the key.
     * 
     * @param key
     *            The key.
     * 
     * @return The old value.
     */
    Object remove(Object key);

    /**
     * Atomically remove the attribute under the key iff it has the given value.
     * 
     * @param key
     *            The key.
     * @param value
     *            The expected value for that key.
     *            
     * @return <code>true</code> iff the entry for that key was removed.
     */
    boolean remove(Object key,Object value);
    
}
