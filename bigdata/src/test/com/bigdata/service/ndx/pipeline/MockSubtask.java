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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.ndx.pipeline.AbstractKeyRangeMasterTestCase.L;

/**
 * Class exists solely to make it easier to write the unit tests by aligning the
 * various generic types across the master, the subtask, and their statistics
 * objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class MockSubtask<//
H extends MockMasterStats<L, HS>, //
O extends Object, //
E extends KVO<O>, //
L extends Object, //
S extends MockSubtask, //
HS extends MockSubtaskStats, //
M extends MockMaster<H, O, E, S, L, HS>//
> extends AbstractSubtask<HS, M, E, L> {

    /**
     * @param master
     * @param locator
     * @param buffer
     */
    public MockSubtask(M master, L locator,
            BlockingBuffer<E[]> buffer) {

        super(master, locator, buffer);
        
    }

    /** NOP. */
    @Override
    protected void notifyClientOfRedirect(L locator, Throwable cause) {

        if (log.isInfoEnabled())
            log.info("locator=" + locator + ", cause=" + cause);
        
    }

}
