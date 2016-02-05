/*

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
package com.bigdata.btree.proc;

/**
 * Interface for procedures that are mapped across one or more index
 * partitions based on a key range (fromKey, toKey).  The keys are
 * interpreted as variable length unsigned byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IKeyRangeIndexProcedure<T> extends IIndexProcedure<T> {
    
    /**
     * Return the lowest key that will be visited (inclusive). When
     * <code>null</code> there is no lower bound.
     */
    public byte[] getFromKey();

    /**
     * Return the first key that will not be visited (exclusive). When
     * <code>null</code> there is no upper bound.
     */
    public byte[] getToKey();

}
