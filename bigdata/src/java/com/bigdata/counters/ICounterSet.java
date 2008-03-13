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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * A collection of named {@link Counter}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounterSet {

    /**
     * Separator for path name components.
     */
    public static final String pathSeparator = "/";
    
    /**
     * Visits {@link ICounter} matching the optional filter declared anywhere in
     * the hierarchy spanned by this {@link ICounterSet}.
     * 
     * @param filter
     *            An optional regular expression that will be applied to
     *            {@link ICounter#getPath()} to filter matches.  When specified,
     *            only {@link ICounter}s whose {@link ICounter#getPath()} match
     *            will be visited by the {@link Iterator}.
     */
    public Iterator<ICounter> getCounters(Pattern filter);
    
    /**
     * Return the directly attached {@link Counter} object by name.
     * 
     * @param name
     *            The counter name.
     *            
     * @return The {@link Counter} object.
     */
    public ICounter getCounterByName(String name);

    /**
     * Return the {@link ICounterSet} under which the counter with the specified
     * path would exist.
     * 
     * @param path
     *            The path of the counter.
     * 
     * @return The parent to under which a counter having that path would be
     *         found -or- <code>null</code> if there is no such parent.
     */
    public ICounterSet getParentByPath(String path);
    
    /**
     * Return the matching {@link ICounter} object by path from anywhere in the
     * spanned hierarchy.
     * 
     * @param path
     *            The counter path. The path MAY be relative to this
     *            {@link ICounterSet}.
     * 
     * @return The {@link ICounter} object.
     */
    public ICounter getCounterByPath(String path);

    /**
     * The immediate parent in the hierarchy -or- <code>null</code> iff this
     * is the root of the hierarchy.
     */
    public ICounterSet getParent();

    /**
     * Return the named directly attached {@link ICounterSet}.
     * 
     * @param name
     *            The name of the directly attached {@link ICounterSet}.
     *            
     * @return The directly attached {@link ICounterSet} -or- <code>null</code>
     *         iff there is no such {@link ICounterSet} attached as a direct
     *         child.
     */
    public ICounterSet getCounterSetByName(String name);
    
    /**
     * Name of this set of counters (does not include the path from the root).
     */
    public String getName();
    
    /**
     * Complete path from the root inclusive of the name of this set of
     * counters.
     */
    public String getPath();
    
    /**
     * The root set of counters.
     */
    public ICounterSet getRoot();
    
    /**
     * <code>true</code> iff there are no children.
     */
    public boolean isLeaf();
    
    /**
     * <code>true</code> iff this is the root of the hierarchy.
     */
    public boolean isRoot();
    
}
