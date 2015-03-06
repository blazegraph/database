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
package com.bigdata.journal.jini.ha;

import net.jini.config.Configuration;

/**
 * Test suite for HA1 with concurrent writers.
 * <p>
 * Note: A different code path is used for commit for HA1 than HA3 (no call to
 * postCommit() or postHACommit(). Thus some kinds of errors will only be
 * observable in HA3.  See #1136.
 * 
 * @see TestHA3GroupCommit
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA1GroupCommit extends AbstractHAGroupCommitTestCase {

    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {
        
        return new String[]{
//        		"com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor(),
        };
        
    }
    
    public TestHA1GroupCommit() {
    }

    public TestHA1GroupCommit(String name) {
        super(name);
    }

    @Override
    protected int replicationFactor() {

        return 1;
        
    }

    /**
     * Create 2 namespaces and then load data into those namespaces in parallel.
     * 
     * @throws Exception
     */
    public void test_HA1_GroupCommit_2Namespaces_ConcurrentWriters() throws Exception {

       startA();

       doGroupCommit_2Namespaces_ConcurrentWriters(false/* reallyLargeLoad */);
       
    }

    /**
     * Create 2 namespaces and then load a large amount data into those namespaces in parallel.
     * 
     * @throws Exception
     */
    public void test_HA1_GroupCommit_2Namespaces_ConcurrentWriters_LargeLoad() throws Exception {

       startA();

       doGroupCommit_2Namespaces_ConcurrentWriters(true/* reallyLargeLoad */);
       
    }

    /**
     * Create 2 namespaces and then load data into those namespaces in parallel
     * using a "DROP ALL; LOAD" pattern. A set of such tasks are generated and
     * the submitted in parallel. LOADs into the same namespace will be
     * serialized by the backend. Loads into different namespaces will be
     * parallelized.
     * 
     * @throws Exception
     */
    public void test_HA1_groupCommit_create2Namespaces_manyConcurrentLoadWithDropAll()
          throws Exception {

       final int nnamespaces = 2;
       final int nruns = 20;
       final boolean reallyLargeLoad = false;

       startA();

       doManyNamespacesConcurrentWritersTest(nnamespaces, nruns, reallyLargeLoad);

    }

   /**
    * Test creates N namespaces and then loads the data into those namespaces in
    * parallel using a "DROP ALL; LOAD" pattern and a small payload for the
    * updates. Due to the small payload, it is reasonable to expect that some
    * commit groups will be melded that have more than one update for a given
    * namespace.
    */
   public void test_HA1_groupCommit_ManyNamespacesConcurrentWritersSmallUpdates()
         throws Exception {

      final int nnamespaces = 10;
      final int nruns = 50;

      startA();

      doManyNamespacesConcurrentWritersSmallUpdatesTest(nnamespaces, nruns);

   }

}
