package com.bigdata.rdf.sail;

import java.util.Random;

public class TestMROWTransactionsWithHistory extends TestMROWTransactions {

    public TestMROWTransactionsWithHistory() {
    }

    public TestMROWTransactionsWithHistory(String arg0) {
        super(arg0);
    }

//  public void test_multiple_csem_transaction_withHistory() throws Exception {
//      domultiple_csem_transaction(1);
//  }
//  
//  public void test_multiple_csem_transaction_onethread_withHistory() throws Exception {
//      domultiple_csem_transaction_onethread(1);
//  }
    
    public void test_multiple_csem_transaction_withHistory_stress() throws Exception {

        final Random r = new Random();
        
        for (int i = 0; i < 100; i++) {

            final int retentionMillis = (r.nextInt(10) * 10) + 1;
            
            final int nreaderThreads = r.nextInt(19) + 1;
            
            log.warn("Trial: " + i + ", retentionMillis=" + retentionMillis
                    + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(retentionMillis, nreaderThreads,
                    20/* nwriters */, 400/* nreaders */);

        }
        
    }
    
}
