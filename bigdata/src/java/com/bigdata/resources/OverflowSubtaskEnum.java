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
 * Created on Feb 8, 2009
 */

package com.bigdata.resources;

import com.bigdata.btree.IndexSegment;
import com.bigdata.service.ResourceService;

/**
 * Various kinds of subtasks for asynchronous index partition overflow tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum OverflowSubtaskEnum {

    /**
     * Corresponds to the total overflow task for an index partition.
     */
    Total,
    /**
     * Operation responsible for the atomic update of the index partition view
     * as part of any of the asynchronous overflow tasks
     * 
     * @see OverflowActionEnum
     */
    AtomicUpdate,
    /**
     * Copying historical data from the old journal.
     */
    CopyHistory,
    /**
     * Registering a new index partition.
     */
    RegisterIndex,
    /**
     * Operation copying an {@link IndexSegment} using the
     * {@link ResourceService}.
     */
    SendSegment;

}
