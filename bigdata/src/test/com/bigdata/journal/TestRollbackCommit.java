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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.util.ChecksumUtility;

/**
 * Test the ability to rollback a commit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRollbackCommit extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestRollbackCommit() {
    }

    /**
     * @param name
     */
    public TestRollbackCommit(String name) {
     
        super(name);
        
    }

    public void test_rollback() {

        final Journal journal = new Journal(getProperties());

        try {

            final ChecksumUtility checker = new ChecksumUtility();
            
            final ByteBuffer rootBlock0 = journal.getBufferStrategy()
                    .readRootBlock(true/* rootBlock0 */);
            
            final IRootBlockView rootBlockView0 = new RootBlockView(
                    true/* rootBlock0 */, rootBlock0, checker);

            if (log.isInfoEnabled())
                log.info("rootBlock0=" + rootBlockView0);

            final ByteBuffer rootBlock1 = journal.getBufferStrategy()
                    .readRootBlock(false/* rootBlock0 */);

            final IRootBlockView rootBlockView1 = new RootBlockView(
                    false/* rootBlock0 */, rootBlock1, checker);

            if (log.isInfoEnabled())
                log.info("rootBlock1=" + rootBlockView1);

            // true iff rootBlock0 is the current root block.
            final boolean isRootBlock0 = journal.getRootBlockView()
                    .isRootBlock0();

            if (log.isInfoEnabled())
                log.info("Using rootBlock=" + (isRootBlock0 ? 0 : 1));

            // write a record.
            journal.write(getRandomData());

            // commit the journal.
            journal.commit();

            if (log.isInfoEnabled())
                log.info("After commit   =" + journal.getRootBlockView());

            // verify that the other root block is now the current one.
            assertNotSame(isRootBlock0, journal.getRootBlockView()
                    .isRootBlock0());

            journal.rollback();

            if (log.isInfoEnabled())
                log.info("After rollback =" + journal.getRootBlockView());

            // verify that the state of rootBlock0 is as before the commit.
            assertEquals(rootBlock0, journal.getBufferStrategy().readRootBlock(
                    true/* rootBlock0 */));

            // verify that the state of rootBlock1 is as before the commit.
            assertEquals(rootBlock1, journal.getBufferStrategy().readRootBlock(
                    false/* rootBlock0 */));
            
        } finally {
            
            journal.destroy();
            
        }
        
    }
        
}
