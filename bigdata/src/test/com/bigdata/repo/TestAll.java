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
 * Created on Feb 4, 2007
 */

package com.bigdata.repo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Aggregates the tests in increasing dependency order.
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("Bigdata Scale-Out Repository");
 
        /*
         * Low-level block IO operations for file versions.
         * 
         * FIXME test overflow handling of the data index, especially with an
         * eye to the index split points and the optimization to store blocks
         * using raw records on the journal (thereby requiring an extension of
         * the overflow semantics to get the data inline on the
         * {@link IndexSegment}).
         */
        
        // test atomic append operations on the file and read back.
        suite.addTestSuite( TestAppendBlock.class );

        // test copying streams to a file using atomic append.
        suite.addTestSuite( TestCopyStream.class );
        
        // test some specifics of the FileVersionOutputStream.
        suite.addTestSuite( TestFileVersionOutputStream.class );
        
        // test random block read / write / update / delete operations.
        suite.addTestSuite( TestRandomBlockOps.class );
        
        /*
         * These two tests should be part of the map/reduce master test suite.
         * 
         * @todo test split of a large file into blocks (the file is conditioned
         * by the application for this by flushing the stream before the next
         * 64k block) and the read of each block by its appropriate client.
         * 
         * @todo also test ability to figure out which client is "near" the
         * blocks by consulting the {@link MetadataIndex} and an as yet
         * undefined network topology model.
         */
        
        /*
         * High-level operations on "file"s.
         * 
         * @todo test various operations on the file metadata index, including
         * those that write on the data index.
         * 
         * @todo add test suite covering (a) CRUD operations; (c) range scan and
         * range delete operations; (d) performance tests under simulated load;
         * 
         * @todo test overflow of the file metadata index.
         * 
         * @todo test policy for compacting merge to eventually eradicate old
         * file versions and their metadata.
         * 
         * @todo test policy for replication counts - the file identifiers space
         * is partitioned by an application specified policy into a variety of
         * zones with their own replication and compacting merge policies.
         * typical zones would correspond to /tmp, /highly-available, etc. The
         * media indexing policy could also be per-zone. A "zone" corresponds to
         * a set of index partitions.
         */
        suite.addTestSuite( TestFileMetadataIndex.class );
 
        // @todo test document metadata range scan and range delete.
        
        /* 
         * @todo test full text indexing and search.
         */
//        suite.addTestSuite( TestSearch.class );
        
        return suite;
        
    }

}
