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
 * Created on Feb 29, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;

/**
 * Interface for deciding when an index partition should be joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJoinHandler extends Serializable {

    /**
     * Return <code>true</code> if a cursory examination of an index partition
     * suggests that it SHOULD be joined with either its left or right sibling.
     * The basic determination is that the index partition is "undercapacity".
     * Normally this is decided in terms of the range count of the index
     * partition.
     * 
     * @param ndx
     *            An index partition.
     * 
     * @return <code>true</code> if the index partition should be joined.
     */
    public boolean shouldJoin(IIndex ndx);
    
}
