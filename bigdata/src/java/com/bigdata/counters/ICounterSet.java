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
public interface ICounterSet extends ICounterNode {

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
    
    
//    /**
//     * Return the {@link ICounterSet} under which the counter with the specified
//     * path would exist.
//     * 
//     * @param path
//     *            The path of the counter.
//     * 
//     * @return The parent to under which a counter having that path would be
//     *         found -or- <code>null</code> if there is no such parent.
//     */
//    public ICounterSet getCounterSetByPath(String path);
    
//    /**
//     * Return the matching {@link ICounter} object by path from anywhere in the
//     * spanned hierarchy.
//     * 
//     * @param path
//     *            The counter path. The path MAY be relative to this
//     *            {@link ICounterSet}.
//     * 
//     * @return The {@link ICounter} object.
//     * 
//     * @throws IllegalArgumentException
//     *             if the path is <code>null</code>
//     * @throws IllegalArgumentException
//     *             if the path is an empty string.
//     */
//    public ICounter getCounterByPath(String path);
//
//    /**
//     * Return the named directly attached {@link ICounterSet}.
//     * 
//     * @param name
//     *            The name of the directly attached {@link ICounterSet}.
//     *            
//     * @return The directly attached {@link ICounterSet} -or- <code>null</code>
//     *         iff there is no such {@link ICounterSet} attached as a direct
//     *         child.
//     */
//    public ICounterSet getCounterSetByName(String name);
    
    /**
     * Adds any necessary {@link ICounterSet}s described in the path (ala
     * mkdirs).
     * 
     * @param path
     *            The path (may be relative or absolute).
     * 
     * @return The {@link ICounterSet} described by the path.
     * 
     * @throws IllegalArgumentException
     *             if the path is <code>null</code>
     * @throws IllegalArgumentException
     *             if the path is an empty string.
     */
    public ICounterSet makePath(String path);
    
    /**
     * A human readable representation of all counters in the hierarchy together
     * with their current value.
     */
    public String toString();

    /**
     * A human readable representation of all counters in the hierarchy together
     * with their current value.
     * 
     * @param filter
     *            An optional filter that will be used to select only specific
     *            counters.
     */
    public String toString(Pattern filter);

}
