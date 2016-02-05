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

import java.util.Properties;

import junit.framework.Test;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.IRemoteTx;
import com.bigdata.rdf.sail.webapp.client.RemoteTransactionManager;

/**
 * Proxied test suite for testing the transaction management API. The outer
 * class provides a test suite for behaviors that are consistent without regard
 * to whether or not isolatable indices have been enabled. There are then two
 * inner classes that provide tests where we are controlling the configuration
 * and verifying behaviors that are specific to when isolatable indices are /
 * are not enabled.
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
 * 
 *      FIXME Write a test suite that uses a mixture of unisolated and
 *      read/write transactions. Verify that we can use unisolated transactions
 *      for bulk load and isolated transactions for smaller mutations and that
 *      the indices are consistent (especially, this is concerned with the
 *      revision timestamps on the indices that are used to detect write-write
 *      conflicts in read/write transactions - the unisolated updates need to be
 *      touching those timestamps if the index supports isolation in order for
 *      the read/write transactions to detect a conflict created by an
 *      unisolated update since the read/write transaction was created.)
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

//      return ProxySuiteHelper.suiteWhenStandalone(Test_REST_TX_API.NoReadWriteTx.class,
//            "test.*", TestMode.quads
//      // , TestMode.sids
//      // , TestMode.triples
//            );
//
//      return ProxySuiteHelper.suiteWhenStandalone(Test_REST_TX_API.ReadWriteTx.class,
//            "test.*", TestMode.quads
//      // , TestMode.sids
//      // , TestMode.triples
//            );

   }

   /**
    * Create an unisolated transaction, verify its metadata, and abort it.
    */
   public void test_CREATE_TX_UNISOLATED_01() throws Exception {

      assertNotNull(m_mgr);
      assertNotNull(m_mgr.getTransactionManager());

      final IRemoteTx tx = m_mgr.getTransactionManager().createTx(
            RemoteTransactionManager.UNISOLATED);

      try {

         assertTrue(tx.isActive());
         assertFalse(tx.isReadOnly());

      } finally {

         tx.abort();

      }

      assertFalse(tx.isActive());
      assertFalse(tx.isReadOnly());

   }

   /**
    * Create an unisolated transaction and commit it. This should be a NOP since
    * nothing is written on the database.
    * 
    * TODO Create an unisolated transaction, write on the transaction, commit
    * the transaction and verify that we can read back the write set after the
    * commit. Note that we can only write on the resulting transaction if the
    * namespace supports isolatable indices.
    */
   public void test_CREATE_TX_UNISOLATED_02() throws Exception {

      assertNotNull(m_mgr);
      assertNotNull(m_mgr.getTransactionManager());

      final IRemoteTx tx = m_mgr.getTransactionManager().createTx(
            RemoteTransactionManager.UNISOLATED);

      try {

      } finally {

         tx.commit();

      }

      assertFalse(tx.isActive());

   }

   /**
    * Create an read-only transaction, verify its metadata, and abort it.
    */
   public void test_CREATE_TX_READ_ONLY_01() throws Exception {

      assertNotNull(m_mgr);
      assertNotNull(m_mgr.getTransactionManager());

      final IRemoteTx tx = m_mgr.getTransactionManager().createTx(
            RemoteTransactionManager.READ_COMMITTED);

      try {

         assertTrue(tx.isActive());
         assertTrue(tx.isReadOnly());

      } finally {

         tx.abort();

      }

      assertFalse(tx.isActive());
      assertTrue(tx.isReadOnly());

   }

   /**
    * Create an read-only transaction and commit it. This should be a NOP since
    * nothing is written on the database.
    * 
    * TODO Actually read on the transaction. Verify that we do not see concurrent
    * updates.
    * 
    * TODO Do something similar with read-historical transactions. Verify that
    * we do not see concurrent updates and that we do not see updates for commit
    * points after the transaction start (this is nearly the same thing, but we
    * also should create the read-only tx only once we know that a commit point
    * has been pinned and that subsequent commits have been applied and verify
    * that the new tx is also reading from the correct commit point.)
    */
   public void test_CREATE_TX_READ_ONLY_02() throws Exception {

      assertNotNull(m_mgr);
      assertNotNull(m_mgr.getTransactionManager());

      final IRemoteTx tx = m_mgr.getTransactionManager().createTx(
            RemoteTransactionManager.READ_COMMITTED);

      try {

      } finally {

         tx.commit();

      }

      assertFalse(tx.isActive());

   }

//   public void test_TX_STUFF() {
//
//      fail("write lots of tests");
//
//   }

   /**
    * An *extension* of the test suite that uses a namespace that is NOT
    * configured to support read/write transactions. This extension is used to
    * verify that certain operations are NOT permitted when the namespace does
    * not support isolatable indices.
    * <p>
    * Note: This does not change whether or not a transaction may be created,
    * just whether or not the namespace will allow an operation that is isolated
    * by a read/write transaction.
    */
   public static class NoReadWriteTx<S extends IIndexManager> extends
         Test_REST_TX_API<S> {

      @Override
      public Properties getProperties() {

         final Properties p = new Properties(super.getProperties());

         p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "false");

         return p;

      }
      
      public NoReadWriteTx() {

      }

      public NoReadWriteTx(final String name) {

         super(name);

      }
      
      // FIXME Write tests.
      public void test_TX_STUFF() {
         
      }

   }

   /**
    * An *extension* of the test suite that uses a namespace that is configured
    * to support read/write transactions.
    * <p>
    * Note: This does not change whether or not a transaction may be created,
    * just whether or not the namespace will allow an operation that is isolated
    * by a read/write transaction.
    */
   public static class ReadWriteTx<S extends IIndexManager> extends
         Test_REST_TX_API<S> {

      @Override
      public Properties getProperties() {

         final Properties p = new Properties(super.getProperties());

         p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");

         return p;

      }

      public ReadWriteTx() {

      }

      public ReadWriteTx(final String name) {

         super(name);

      }
      
      // FIXME Write tests.
      public void test_TX_STUFF() {
         
      }

   }

}
