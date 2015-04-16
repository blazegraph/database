/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import junit.framework.Test;

import com.bigdata.journal.IIndexManager;

/**
 * Proxied test suite for testing the transaction management API.
 * 
 * @param <S>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
 *      transactions in the REST API</a>
 * 
 *      FIXME (***) Test operations isolated by transactions.
 * 
 *      FIXME Test that a sequence of queries may be isolated by a read-only
 *      transaction and that the queries have snapshot isolation across the
 *      transaction that is preserved even when there are new commit points that
 *      write on the namespace.
 *      
 *      FIXME Write tests in which we force operations where the transaction is
 *      not active and make sure that the API is behaving itself in terms of the
 *      error messages.
 * 
 *      FIXME Write HA tests for transaction coordination. See #1189
 * 
 *      FIXME Modify the multi-tenancy API stress test to obtain a stress test
 *      that also operations against namespaces that are configured with
 *      isolatable index support.
 * 
 *      TODO Test that a transaction may be created without regard to the end
 *      point. The transaction is about the transaction manager, not a given
 *      namespace. Isolation of a namespace is obtained by having the
 *      transaction pin the commit point associated with its start time (its
 *      readsOnCommitTime). Thus we can actually use a single transaction to
 *      coordinate an operation on more than one namespace in the same database.
 * 
 *      TODO Write unit test that is federation specific and which verifies that
 *      read/write transactions are correctly rejected since they are not
 *      supported for scale-out (we do not support distributed 2-phase
 *      transactions).
 */
public class Test_REST_TX_API<S extends IIndexManager> extends
      AbstractTestNanoSparqlClient<S> {

   public Test_REST_TX_API() {

   }

   public Test_REST_TX_API(final String name) {

      super(name);

   }

   public static Test suite() {

      return ProxySuiteHelper.suiteWhenStandalone(Test_REST_TX_API.class,
            "test.*", TestMode.quads
      // , TestMode.sids
      // , TestMode.triples
            );

   }

   public void test_CREATE_TX_01() {
      fail("write lots of tests");
   }
   
}
