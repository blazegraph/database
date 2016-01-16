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
 * Created on Oct 14, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.PipelineOp;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rwstore.sector.MemoryManager;

/**
 * Test suite for the {@link HTreeHashJoinUtility}.
 * 
 * TODO Write a unit test which verifies that the ivCache is used and that the
 * cached {@link BigdataValue}s are correctly restored when the rightSolutions
 * had cached values and the leftSolutions did not have a value cached for the
 * same IVs. For example, this could be done with a cached value on an IV which
 * is not a join variable and which is only present in the rightSolutions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestHTreeHashJoinUtility.java 5527 2011-11-04 17:41:14Z
 *          thompsonbry $
 */
public class TestHTreeHashJoinUtility extends AbstractHashJoinUtilityTestCase {

    /**
     * 
     */
    public TestHTreeHashJoinUtility() {
    }

    /**
     * @param name
     */
    public TestHTreeHashJoinUtility(String name) {
        super(name);
    }
    
    private MemoryManager mmgr;

    @Override
    protected void tearDown() throws Exception {

        if (mmgr != null) {
            mmgr.clear();
            mmgr = null;
        }

        super.tearDown();

    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
    
        /*
         * This wraps an efficient raw store interface around a
         * child memory manager created from the IMemoryManager
         * which is backing the query.
         */
        
        mmgr = new MemoryManager(DirectBufferPool.INSTANCE);

    }

    @Override
    protected HTreeHashJoinUtility newHashJoinUtility(final PipelineOp op,
            final JoinTypeEnum joinType) {

        return new HTreeHashJoinUtility(mmgr, op, joinType);

    }

}
