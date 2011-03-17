/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 23, 2011
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.Arrays;

/**
 * An ordered array of bop identifiers which can be used as a signature for a
 * join path segment. Unlike an <code>int[]</code>, instances of this class may
 * be used safely in a hash map.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PathIds {

    final int[] ids;

    /**
     * The length of the path.
     */
    public int length() {

        return ids.length;
        
    }
    
    public PathIds(final int[] ids) {

        if (ids == null)
            throw new IllegalArgumentException();
        
        this.ids = ids;

    }
    
    /**
     * Same path ids.
     */
    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof PathIds)) {
            return false;
        }
        
        return Arrays.equals(ids, ((PathIds) o).ids);

    }

    /**
     * The hash code of an edge is the hash code of the vertex with the smaller
     * hash code X 31 plus the hash code of the vertex with the larger hash
     * code. This definition compensates for the arbitrary order in which the
     * vertices may be expressed and also recognizes that the vertex hash codes
     * are based on the bop ids, which are often small integers.
     */
    public int hashCode() {

        int h = hash;
        
        if (h == 0) {
        
            for (int id : ids) {

                h = 31 * h + id;

            }

            hash = h;
            
        }
        
        return hash;

    }

    private int hash;

    /**
     * Return the path id array as a string.
     */
    @Override
    public String toString() {
        
        return Arrays.toString(ids);
        
    }
    
}
