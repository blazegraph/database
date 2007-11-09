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
 * Created on Nov 17, 2006
 */
package com.bigdata.btree;

import com.bigdata.cache.HardReferenceQueue;

/**
 * Hard reference cache eviction listener for leaves always throws an
 * exception. This is used for some unit tests to ensure that cache
 * evictions are not occurring and that copy on write situations are
 * therefore never triggered (except that they will of course be triggered
 * following a commit).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see SimpleEntry#NoSerializer
 */
public class NoEvictionListener implements
        IEvictionListener {

    public void evicted(HardReferenceQueue<PO> cache, PO ref) {

        assert ref instanceof Leaf;
        
        if( ref.isDirty() ) {

            throw new UnsupportedOperationException(
                    "Leaf eviction is disabled for this unit test: leaf=" + ref);
            
        }

    }

}