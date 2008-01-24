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
 * Created on Jan 22, 2008
 */

package com.bigdata.sparse;

import java.util.Iterator;
import java.util.Map;

/**
 * A Timestamp Property Set is a property set with
 * {@link ITPV timestamp property values} representing data for a specific
 * {@link Schema}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITPS {

    /**
     * The {@link Schema} name.
     */
    public Schema getSchema();

    /**
     * The timestamp assigned by an atomic write operation.
     * 
     * @return The timestamp.
     * 
     * @throws IllegalStateException
     *             if no timestamp has been assigned.
     */
    public long getTimestamp();
    
    /**
     * The #of tuples - each tuple is an {@link ITPV}.
     */
    public int size();
    
    /**
     * Return the most recent value for the named property whose timestamp is
     * not greater than the specified timestamp.
     * 
     * @param name
     *            The property name.
     * @param timestamp
     *            A timestamp to obtain the value for the named property whose
     *            timestamp does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most recent
     *            value for the property.
     * 
     * @return An object representing value of the property as of the indicated
     *         timestamp and never <code>null</code>.
     */
    public ITPV get(String name, long timestamp);
    
    /**
     * Return the most recent value for the named property.
     * 
     * @param name
     *            The propery name.
     *            
     * @return An object representing value of the property as of the indicated
     *         timestamp and never <code>null</code>.
     */
    public ITPV get(String name);
    
    /**
     * Visits all tuples in order by <em>ascending timestamp</em>.
     */
    public Iterator<ITPV> iterator();

    /**
     * Return a copy of the tuples showing only the most recent value for each
     * property.
     */
    public Map<String,Object> asMap();
    
    /**
     * Return a copy of the tuples showing only the most recent value for each
     * property whose timestamp is not greater than the given timestamp.
     * 
     * @param timestamp
     *            The timestamp (use {@link Long#MAX_VALUE} to read the most
     *            recent value for each property).
     * 
     * @return A map containing a copy of the selected property values. A
     *         deleted property will not be contained in the map.
     */
    public Map<String,Object> asMap(long timestamp);
    
    /**
     * Return a copy of the tuples showing only the most recent value for each
     * property whose timestamp is not greater than the given timestamp.
     * 
     * @param timestamp
     *            The timestamp (use {@link Long#MAX_VALUE} to read the most
     *            recent value for each property).
     * @param filter
     *            An optional filter that may be used to select only specific
     *            property names.
     * 
     * @return A map containing a copy of the selected property values. A
     *         deleted property will not be contained in the map.
     */
    public Map<String, Object> asMap(long timestamp, INameFilter filter);
    
}
