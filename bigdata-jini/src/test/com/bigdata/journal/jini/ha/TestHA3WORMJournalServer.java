package com.bigdata.journal.jini.ha;

import com.bigdata.journal.BufferMode;

public class TestHA3WORMJournalServer extends TestHA3JournalServer {
	
	
	public TestHA3WORMJournalServer() {}
	
	public TestHA3WORMJournalServer(String nme) {
		super(nme);
	}
	
    protected BufferMode getDiskMode() {
    	return BufferMode.DiskWORM;
    }

}
