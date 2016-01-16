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

/**
 * Test suite for the {@link JVMHashJoinUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestJVMHashJoinUtility.java 5343 2011-10-17 15:29:02Z
 *          thompsonbry $
 */
public class TestJVMHashJoinUtility extends AbstractHashJoinUtilityTestCase {

    /**
     * 
     */
    public TestJVMHashJoinUtility() {
    }

    /**
     * @param name
     */
    public TestJVMHashJoinUtility(String name) {
        super(name);
    }

    @Override
    protected JVMHashJoinUtility newHashJoinUtility(final PipelineOp op,
            final JoinTypeEnum joinType) {
        
        return new JVMHashJoinUtility(op, joinType);
        
    }
    
}
