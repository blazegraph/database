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
 * Created on Mar 17, 2008
 */

package com.bigdata.counters;

/**
 * Shared abstraction layer for both {@link ICounterSet} and {@link ICounter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounterNode {

    /**
     * The immediate parent in the hierarchy -or- <code>null</code> iff this
     * is the root of the hierarchy.
     */
    public ICounterSet getParent();

    /**
     * The local name (does not include the path from the root).
     */
    public String getName();
    
    /**
     * Complete path from the root inclusive of the local name.
     */
    public String getPath();
    
    /**
     * The root of the hierarchy.
     */
    public ICounterSet getRoot();
    
    /**
     * <code>true</code> iff this is the root of the hierarchy.
     */
    public boolean isRoot();

    /**
     * <code>true</code> iff this is a collection of counters.
     */
    public boolean isCounterSet();
    
    /**
     * <code>true</code> iff this is a counter.
     */
    public boolean isCounter();
    
    /**
     * Return the directly attached object by name.
     * 
     * @param name
     *            The counter name.
     *            
     * @return The object.
     */
    public ICounterNode getChild(String name);

    /**
     * Return the object described by the path.
     * 
     * @param path
     *            The path.
     *            
     * @return The object or <code>null</code> if nothing exists for that
     *         path.
     */
    public ICounterNode getPath(String path);
    
}
