/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Defines some helper methods for testing {@link ICommitRecord}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractCommitRecordTestCase extends TestCase {

    public AbstractCommitRecordTestCase() {
    }

    public AbstractCommitRecordTestCase(String name) {
        super(name);
    }

    Random r = new Random();

    /**
     * Compare two {@link ICommitRecord}s for equality in their data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(ICommitRecord expected, ICommitRecord actual) {
        
        assertEquals("timestamp", expected.getTimestamp(), actual.getTimestamp());

        assertEquals("#roots", expected.getRootAddrCount(), actual.getRootAddrCount());
        
        final int n = expected.getRootAddrCount();
        
        for(int i=0; i<n; i++) {
        
            assertEquals("rootAddrs", expected.getRootAddr(i), actual.getRootAddr(i));
            
        }
        
    }

    public ICommitRecord getRandomCommitRecord() {

        final long timestamp = System.currentTimeMillis();

        // using the clock for this as well so that it is an ascending value.
        final long commitCounter = System.currentTimeMillis();

        final int n = ICommitRecord.MAX_ROOT_ADDRS;
        
        long[] roots = new long[n];
        
        for(int i=0; i<n; i++) {

            boolean empty = r.nextInt(100)<30;
        
            roots[i] = empty ? 0L : r.nextInt(Integer.MAX_VALUE);
            
        }

        return new CommitRecord(timestamp,commitCounter,roots);
        
    }
    

}
