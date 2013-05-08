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
 * Created on May 8th, 2013
 */
package com.bigdata.journal;

import java.io.File;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link CommitCounterUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestCommitCounterUtility extends TestCase2 {

    public TestCommitCounterUtility() {
    }

    public TestCommitCounterUtility(String name) {
        super(name);
    }

    public void test01() {
        
        final File dir = new File("/tmp");
        
        final String ext = ".tmp";

        final File f = new File(
                "/tmp/000/000/000/000/000/000/000000000000000000001.tmp");

        assertEquals(f, CommitCounterUtility.getCommitCounterFile(dir, 1L, ext));

        assertEquals(1L,
                CommitCounterUtility.parseCommitCounterFile(f.getName(), ext));

        assertEquals("000000000000000000001",
                CommitCounterUtility.getBaseName(f.getName(), ext));

    }
    
}
