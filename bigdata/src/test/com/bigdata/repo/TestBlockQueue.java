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
 * Created on Jan 21, 2008
 */

package com.bigdata.repo;

/**
 * Unit tests for atomic block operations supporting queues.
 * <p>
 * Note: Queues MAY be built from the operations to atomically read or delete
 * the first block for the file version. The "design pattern" is to have clients
 * append blocks to the file version, taking care that logical rows never cross
 * a block boundary (e.g., by flushing partial blocks). A master then reads the
 * head block from the file version, distributing the logical records therein to
 * consumers and providing failsafe processing in case consumers die or take too
 * long. Once all records for the head block have been processed the master
 * simply deletes the head block. This "pattern" is quite similar to map/reduce
 * and, like map/reduce, requires that the consumer operations may be safely
 * re-run.
 * 
 * @todo unit tests of {@link BigdataRepository#readHead(String, int)} and
 *       {@link BigdataRepository#deleteHead(String, int)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBlockQueue extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestBlockQueue() {
    }

    /**
     * @param arg0
     */
    public TestBlockQueue(String arg0) {
        super(arg0);
    }

}
