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
 * Created on Jun 13, 2009
 */

package com.bigdata.util;

import java.util.Comparator;

/**
 * Places {@link Long} values into descending order.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReverseLongComparator implements Comparator<Long> {
    
    public int compare(final Long o1, final Long o2) {

        final long l1 = o1.longValue();

        final long l2 = o2.longValue();

        if (l1 < l2) {

            return 1;

        } else if (l1 > l2) {
            
            return -1;
            
        }
        
        return 0;
        
    }

}
