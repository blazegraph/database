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
 * Created on Dec 19, 2006
 */
package com.bigdata.rdf.sail;

import java.util.Random;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 */
public class TestMROWTransactionsWithHistory extends TestMROWTransactions {

    public TestMROWTransactionsWithHistory() {
    }

    public TestMROWTransactionsWithHistory(final String arg0) {
        super(arg0);
    }

    public void test_multiple_csem_transaction_1ms_history_stress() throws Exception {

        final Random r = new Random();
        
        for (int i = 0; i < 10; i++) {

            final int nreaderThreads = r.nextInt(19) + 1;
            
            log.warn("Trial: " + i + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(1/* retentionMillis */,
                    nreaderThreads, 100/* nwriters */, 400/* nreaders */, false/* isolatableIndices */);

        }
        
    }

    public void test_multiple_csem_transaction_1ms_history_stress_readWriteTx()
            throws Exception {

        final Random r = new Random();

        for (int i = 0; i < 10; i++) {

            final int nreaderThreads = r.nextInt(19) + 1;

            log.warn("Trial: " + i + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(1/* retentionMillis */,
                    nreaderThreads, 100/* nwriters */, 400/* nreaders */, true/* isolatableIndices */);

        }

    }

    // Random history period GTE 1
    public void test_multiple_csem_transaction_withHistory_stress() throws Exception {

        final Random r = new Random();
        
        for (int i = 0; i < 10; i++) {

            final int retentionMillis = (r.nextInt(10) * 10) + 1;
            
            final int nreaderThreads = r.nextInt(19) + 1;
            
            log.warn("Trial: " + i + ", retentionMillis=" + retentionMillis
                    + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(retentionMillis, nreaderThreads,
                    100/* nwriters */, 400/* nreaders */, false/* isolatableIndices */);

        }
        
    }
    
    // Random history period GTE 1
    public void test_multiple_csem_transaction_withHistory_stress_withReadWriteTx() throws Exception {

        final Random r = new Random();
        
        for (int i = 0; i < 10; i++) {

            final int retentionMillis = (r.nextInt(10) * 10) + 1;
            
            final int nreaderThreads = r.nextInt(19) + 1;
            
            log.warn("Trial: " + i + ", retentionMillis=" + retentionMillis
                    + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(retentionMillis, nreaderThreads,
                    100/* nwriters */, 400/* nreaders */, true/* isolatableIndices */);

        }
        
    }
    
}
