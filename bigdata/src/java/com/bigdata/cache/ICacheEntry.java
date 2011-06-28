/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 13, 2005
 */
package com.bigdata.cache;

/**
 * <p>
 * Interface for hard reference cache entries exposes a <i>dirty</i> flag in
 * addition to the object identifier and object reference.
 * </p>
 * 
 * @author thompsonbry
 * @version $Id$
 * 
 * @todo Support clearing updated objects for hot cache between transactions.
 *       Add metadata boolean that indicates whether the object was modified
 *       since the cache was last cleared regardless of whether the object has
 *       since been installed on the persistence layer and marked as clean. The
 *       purpose of this is to support clearing of modified objects from the
 *       object cache when a transaction is aborted. Such objects must be
 *       cleared if they have been modified since they were installed in the
 *       cache regardless of whether they are currently dirty or not. Supporting
 *       this feature will probably require a hard reference Set containing the
 *       object identifier of each object that was marked as dirty in the cache
 *       since the cache was last cleared.
 */
public interface ICacheEntry<K,T> extends IWeakRefCacheEntry<K,T> {

    /**
     * Return true iff the object associated with this entry is dirty.
     */
    public boolean isDirty();

    /**
     * Set the dirty flag.
     * 
     * @param dirty
     *            true iff the object associated with this entry is dirty.
     */
    public void setDirty(boolean dirty);

}
