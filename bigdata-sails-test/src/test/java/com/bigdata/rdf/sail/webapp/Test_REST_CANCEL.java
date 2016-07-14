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

package com.bigdata.rdf.sail.webapp;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Test;
import junit.framework.TestCase2;

import com.bigdata.rdf.sail.webapp.client.MockRemoteRepository;

/**
 * The test verifies the correctness of the HTTP request
 * for an auto-cancellation of running queries. 
 * It uses {@link MockRemoteRepository} to emulate
 * queries running on a server side without an actual 
 * connection with a remote server.
 * 
 */
public class Test_REST_CANCEL extends TestCase2 {
    
    private UUID[] uuids;
    private MockRemoteRepository remote;

    public Test_REST_CANCEL() {

    }

    public Test_REST_CANCEL(final String name) {

        super(name);
        
        this.uuids = new UUID[] {
                
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                
        };

    }

    public static Test suite() {

        return ProxySuiteHelper.suiteWhenStandalone(Test_REST_CANCEL.class,
                "test.*", TestMode.quads
                , TestMode.sids
                , TestMode.triples
                );
       
    }

    @Override
    public void setUp() throws Exception {
        
        super.setUp();
        
        remote = MockRemoteRepository.create(null, "");
    }
    
    /*
     * The test verifies that the close() method cancels all the running queries 
     */
    
    public void test_Cancel_Queries() throws Exception {
        
        if(true) {
            // FIXME See https://jira.blazegraph.com/browse/BLZG-2021
            return;
        }
        
        final String query = "select * {?s ?p ?o}";
        
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            
            runQuery(query, executor, 0);
            runQuery(query, executor, 1);
            runQuery(query, executor, 2);
            
            /*
             * Note: This test appears to be based on the observation that the
             * queries will still be running and that the close() of the 
             * RemoteRepositoryManager will result in the MockRemoteRepository
             * setting the opts such that the UUIDs of the cancelled in flight
             * queries will be reported out.
             */
            remote.data.opts = null;

            remote.getRemoteRepositoryManager().close();
            
            final String[] actual = remote.data.opts.requestParams.get("queryId");
            
            final String[] expected = new String[] {
                    uuids[0].toString(),
                    uuids[1].toString(),
                    uuids[2].toString()
            };
            
            Arrays.sort(expected);
            Arrays.sort(actual);
            
            assertEquals(expected[0], actual[0]);
            assertEquals(expected[1], actual[1]);
            assertEquals(expected[2], actual[2]);
            
            
        } finally {
            
            executor.shutdownNow();
            
        }
   }

    private void runQuery(final String query, final ExecutorService executor, final int i) throws InterruptedException {
    	
        executor.submit(new Runnable(){@Override public void run() { 
        	
        	try {        		
        		remote.prepareTupleQuery(query, uuids[i]).evaluate();
        	} catch (IOException | InterruptedException e) {
        		// expected
        		log.debug(e);
        	} catch (Exception e) {
        		throw new RuntimeException(e);
        	} 
        	
        }});
        
        // SEND_MONITOR from MockRemoteRepository is used to block execution
        // until this monitor will be notified in httpClient.send().
        // It ensures, that after exit from runQuery() method, test could rely
        // on availability of request parameters, captured in httpClient.send(). 
        // @see MockRemoteRepository.create().
        synchronized(MockRemoteRepository.SEND_MONITOR) {
            MockRemoteRepository.SEND_MONITOR.wait();
        }
        
    }

}
