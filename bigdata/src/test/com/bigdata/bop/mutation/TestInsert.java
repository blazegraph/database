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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop.mutation;

import junit.framework.TestCase2;

import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.concurrent.LockManager;
import com.bigdata.journal.ConcurrencyManager;

/**
 * Test suite for {@link InsertOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInsert extends TestCase2 {

    /**
     * 
     */
    public TestInsert() {
    }

    /**
     * @param name
     */
    public TestInsert(String name) {
        super(name);
    }

    /**
     * @todo test writing an index. verify read back after the write.
     * 
     * @todo One of the critical things to verify is that the appropriate locks
     *       are being obtain before writing on an access path. Other than
     *       tracing this through the code, the best way to verify this is with
     *       a concurrent stress test. A failure to obtain the appropriate lock
     *       will show up as an error in the B+Tree due to concurrent
     *       modifications of the underlying data structures.
     *       <p>
     *       Indices are protected by one of two different mechanisms. (1) When
     *       a journal is used without regard to the concurrency manager, the
     *       {@link UnisolatedReadWriteIndex} is responsible for imposing the
     *       appropriate concurrency constraints. (2) When running on a data
     *       service, the {@link ConcurrencyManager} relies on the
     *       {@link LockManager} to serialize access to the mutable indices.
     *       <p>
     *       The query engine refactor needs to respect these same mechanisms in
     *       order to co-exist with the existing design patterns.
     */
    public void test_insert() {

        fail("write tests");
        
    }
    
}
