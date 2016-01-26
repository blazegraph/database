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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryLanguage;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;

/**
 */
public class TestSparqlUpdateCommit extends ProxyBigdataSailTestCase {

    private static final Logger log = Logger.getLogger(TestSparqlUpdateCommit.class);
    
    /**
     * 
     */
    public TestSparqlUpdateCommit() {
    }

    /**
     * @param arg0
     */
    public TestSparqlUpdateCommit(String arg0) {
        super(arg0);
    }

    
    /**
     * Test whether sparql update results in auto-commit.
     */
    public void testCountCommits() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getProperties());

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();

            final CommitCounter counter = new CommitCounter();
            cxn.addChangeLog(counter);

            cxn.prepareUpdate(QueryLanguage.SPARQL, 
                    "insert data { <x:s> <x:p> \"foo\" . }").execute();
            
            cxn.prepareUpdate(QueryLanguage.SPARQL, 
                    "insert data { <x:s> <x:p> \"bar\" . }").execute();

            cxn.commit();
            
            assertTrue(counter.n == 1);
            
        } finally {
            if (cxn != null)
                cxn.close();
            
            sail.__tearDownUnitTest();
        }
    }
    
    public static class CommitCounter implements IChangeLog {
        
        int n = 0;
        
        @Override
        public void transactionCommited(long commitTime) {
            n++;
        }
        
        @Override
        public void transactionPrepare() {
        }
        
        @Override
        public void transactionBegin() {
        }
        
        @Override
        public void transactionAborted() {
        }
        
        @Override
        public void close() {
        }
        
        @Override
        public void changeEvent(IChangeRecord record) {
        }
    } 

}
