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
 * Created on Apr 19, 2006
 */
package com.bigdata.cache;

/**
 * Interface receives notice of cache eviction events.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see com.bigdata.cache.ICachePolicy
 */

public interface ICacheListener<K,T> {

    /**
     * The object was evicted from the cache.
     * 
     * @param entry
     *            The cache entry for the object that is being evicted. The
     *            entry is no longer valid once this method returns and MAY be
     *            reused by the {@link ICachePolicy} implementation.
     */
    
    public void objectEvicted( ICacheEntry<K,T> entry );
    
}
