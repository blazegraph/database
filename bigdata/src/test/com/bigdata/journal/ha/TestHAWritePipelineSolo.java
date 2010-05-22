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
 * Created on May 21, 2010
 */

package com.bigdata.journal.ha;

import com.bigdata.io.TestCase3;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCacheService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ha.HAReceiveService;
import com.bigdata.journal.ha.HASendService;
import com.bigdata.journal.ha.Quorum;

/**
 * This is a test suite for a write pipeline composed from a
 * {@link WriteCacheService}, an {@link HASendService} and an
 * {@link HAReceiveService} without the {@link AbstractJournal} or
 * {@link Quorum}. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHAWritePipelineSolo extends TestCase3 {

    /**
     * 
     */
    public TestHAWritePipelineSolo() {
    }

    /**
     * @param name
     */
    public TestHAWritePipelineSolo(String name) {
        super(name);
    }

    /**
     * FIXME Write a test which sends the data correctly down the pipeline. This
     * will verify the pipeline outside of the {@link Journal}. Make sure that
     * the {@link WriteCache} is computing the necessary checksums for the cache
     * as a whole. The data should be streamed to a file for comparison
     * afterwards, which will allow us to test the scattered writes (RW) and
     * WORM {@link WriteCache} modes.
     */
    public void test_writePipeline() {
        fail("write test");
    }

}
