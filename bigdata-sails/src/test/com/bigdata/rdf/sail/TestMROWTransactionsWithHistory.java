package com.bigdata.rdf.sail;

public class TestMROWTransactionsWithHistory extends TestMROWTransactions {

	public TestMROWTransactionsWithHistory() {
	}

	public TestMROWTransactionsWithHistory(String arg0) {
		super(arg0);
	}

	public void test_multiple_csem_transaction_withHistory() throws Exception {
		domultiple_csem_transaction(1);
	}
	
	public void test_multiple_csem_transaction_onethread_withHistory() throws Exception {
		domultiple_csem_transaction_onethread(1);
	}
	
}
