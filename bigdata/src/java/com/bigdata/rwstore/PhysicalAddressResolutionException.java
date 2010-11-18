/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Nov 18, 2010
 */

package com.bigdata.rwstore;

/**
 * Exception thrown when a logical address maps onto a physical address which is
 * not currently allocated. The most common cause of this exception is a read on
 * the database using a historical commit point which is not protected by a read
 * lock. You should be using a read-only transaction rather than a bare
 * historical read in order to be protected by a read lock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PhysicalAddressResolutionException extends
        IllegalArgumentException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public PhysicalAddressResolutionException(final long addr) {

        super("Address did not resolve to physical address: " + addr);
        
    }
    
}
