/**
Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.client;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

/**
 * Remote client for the Transaction Management API.
 * 
 * @author bryan
 * @since 1.5.2
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
 *      transactions in the REST API</a>
 */
public class RemoteTransactionManager {

   /**
    * The constant that SHOULD used to request a read/write transaction. The
    * transaction will read on the current commit point on the database at the
    * time that the request is processed by the server.
    */
   public static final long UNISOLATED = 0L;
   
   /**
    * The constant that should be used to request a read-only transaction
    * against the current commit point on the database at the time that the
    * request is processed by the server.
    */
   public static final long READ_COMMITTED = -1L;

   /**
    * Return <code>true</code> iff the transaction identifier would be
    * associated with a read/write transaction. This is a purely syntactic check
    * of the numerical value of the transaction identifier. Negative transaction
    * identifiers are read/write transactions.
    * 
    * @param txId
    *           The transaction identifier.
    *           
    * @return <code>true</code> iff it is a read/write transaction.
    */
   public static boolean isReadWriteTx(final long txId) {
      return txId < READ_COMMITTED;
   }

   final private RemoteRepositoryManager mgr;
   
   public RemoteTransactionManager(final RemoteRepositoryManager remoteRepositoryManager) {
      
      if(remoteRepositoryManager == null)
         throw new IllegalArgumentException();
      
      this.mgr = remoteRepositoryManager;
      
   }

   private class RemoteTx implements IRemoteTx {

      /**
       * The transaction identifier.
       */
      private final long txId;
      
      /**
       * The commit time on which the transaction will read.
       */
      private final long readsOnCommitTime;
      
      /**
       * Flag indicates whether the client believes the transaction to be
       * active. Note that the transaction could have been aborted by the
       * server, so the client's belief could be incorrect. The client
       * uses this information to refuse to attempt operations if it believes
       * that the transaction is not active.
       */
      private final AtomicBoolean active = new AtomicBoolean(true);

      /**
       * Note: This object is used both to provide synchronization.
       */
      private Object lock = this;
      
      RemoteTx(final long txId, final long readsOnCommitTime) {
         if (txId == -1L) {
            // This is the symbolic constant for a READ_COMMITTED operation. It
            // is not a transaction identifier.
            throw new IllegalArgumentException();
         }
         if (txId == 0L) {
            // This is the symbolic constant for an UNISOLATED operation. It
            // is not a transaction identifier.
            throw new IllegalArgumentException();
         }
         this.txId = txId;
         this.readsOnCommitTime = readsOnCommitTime;
      }

      @Override
      public long getTxId() {
         return txId;
      }

      @Override
      public long getReadsOnCommitTime() {
         return readsOnCommitTime;
      }

      @Override
      public boolean isReadOnly() {
         /*
          * Note: read/write transaction identifiers are negative numbers. -1L
          * is a READ_COMMITTED operation, but is not allowed but our ctor. 0L
          * is an UNISOLATED operation, but is not allowed by our ctor. Thus if
          * the value is positive, we assume it is a read-only transaction
          * identifier (as assigned by the remote transaction service and not
          * just a timestamp) and if it is negative we assume it is a read/write
          * transaction identifier (as assigned by the remote transaction
          * service).
          */
         return txId > 0;
      }

      /**
       * Look at the {@link #active} flag and throw an exception if the client
       * already believes that the transaction is not active.
       */
      private void assertClientThinksTxActive() {
         if (!active.get())
            throw new TransactionNotActiveException("serviceURL="
                  + mgr.getBaseServiceURL() + ", txId=" + txId);
      }

      // TODO Expose STATUS-TX result.
//      @Override
      public boolean isActive() throws Exception {
//         if (!active.get()) {
//            // Known to be inactive.
//            return false;
//         }
//         synchronized (lock) {
//            // Ask the server and update our flag state.
//            final boolean isActive = statusTx(txId);
//            active.set(isActive);
//         }
         return active.get();
      }

      @Override
      public boolean prepare() throws Exception {
         synchronized (lock) {
            assertClientThinksTxActive();
            // tell the server to prepare the transaction.
            return prepareTx(txId);
         }
      }

      @Override
      public void abort() throws Exception {
         synchronized (lock) {
            assertClientThinksTxActive();
            abortTx(txId);
            active.set(false);
         }
      }

      @Override
      public void commit() throws Exception {
         synchronized (lock) {
            assertClientThinksTxActive();
            commitTx(txId);
            active.set(false);
         }
      }
      
   }

   /**
    * CREATE-TX: Create a transaction on the server.
    * <dl>
    * <dt>{@link #UNISOLATED}</dt>
    * <dd>his requests a new read/write transaction. The transaction will read
    * on the last commit point on the database at the time that the transaction
    * was created. This is the default behavior if the timestamp parameter is
    * not specified. Note: The federation architecture (aka scale-out) does NOT
    * support distributed read/write transactions - all mutations in scale-out
    * are shard-wise ACID.</dd>
    * <dt>{@link #READ_COMMITTED}</dt>
    * <dd>This requests a new read-only transaction. The transaction will read
    * on the last commit point on the database at the time that the transaction
    * was created.</dd>
    * <dt>A <i>timestamp</i></dt>
    * <dd>This requests a new read-only transaction. The operation will be
    * executed again the most recent committed state whose commit timestamp is
    * less than or equal to timestamp.</dd>
    * </dl>
    * 
    * @param timestamp
    *           The timestamp used to indicate the type of transaction
    *           requested.
    *           
    * @return The transaction object.
    */
   public IRemoteTx createTx(final long timestamp) throws Exception {
      
      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx");

      opts.method = "POST";

      opts.addRequestParam("timestamp", Long.toString(timestamp));
      
      JettyResponseListener response = null;

      try {

         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

         return transactionResult(response);

      } finally {

         if (response != null)
            response.abort();

      }

   }

   /**
    * FIXME LIST-TX: Return the set of active transactions.
    */
   public Set<RemoteTx> listTx() {
      throw new UnsupportedOperationException();
   }

   /**
    * FIXME STATUS-TX: Return information about a transaction, including whether
    * or not it is active.
    * 
    * @param txId The transaction identifier.
    * 
    * @return
    * @throws Exception  
    */
   private boolean statusTx(final long txId) throws Exception {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/"+Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("ABORT");
      
      JettyResponseListener response = null;

      try {

         // TODO convert unknown transaction into TransactionNotActiveException.
         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

         // TODO Define, parse, and return the status metadata.
         throw new UnsupportedOperationException();
         
      } finally {

         if (response != null)
            response.abort();

      }
      
   }

   /**
    * FIXME PREPARE-TX: calls threw to Tx.validateWriteSet() on the server.
    * 
    * @param txId
    *           The transaction identifier.
    * 
    * @return <code>true</code> if the write set of the transaction was
    *         validated.
    * 
    * @throws TransactionNotActiveException
    *            if there is no such transaction on the server.
    */
   private boolean prepareTx(final long txId) throws Exception {
      
      if(!isReadWriteTx(txId)) {
         // NOP unless read/write transaction.
         return true;
      }
      
      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/"+Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("PREPARE");
      
      JettyResponseListener response = null;

      try {

         // TODO convert unknown transaction into TransactionNotActiveException.
         final JettyResponseListener listener = RemoteRepository
               .checkResponseCode(response = mgr.doConnect(opts));

         switch (listener.getStatus()) {
         case 200:
            return true;
         default:
            return false;
         }
         
      } finally {

         if (response != null)
            response.abort();

      }

   }
   
   /**
    * ABORT-TX
    * 
    * @param txId
    *           The transaction identifier.
    */
   private void abortTx(final long txId) throws Exception {
      
      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/"+Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("ABORT");
      
      JettyResponseListener response = null;

      try {

         // TODO convert unknown transaction into TransactionNotActiveException.
         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

      } finally {

         if (response != null)
            response.abort();

      }
      
   }
   
   /**
    * COMMIT-TX:
    * 
    * @param txId
    *           The transaction identifier.
    * 
    * @throws Exception
    * @throws TransactionNotActiveException
    *            if there is no such transaction on the server.
    */
   private void commitTx(final long txId) throws Exception {
      
      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/"+Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("COMMIT");
      
      JettyResponseListener response = null;

      try {

         // TODO convert unknown transaction into TransactionNotActiveException.
         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

      } finally {

         if (response != null)
            response.abort();

      }

   }
   
   private RemoteTx transactionResult(final JettyResponseListener response)
         throws Exception {

      try {

         final String contentType = response.getContentType();

         if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

            throw new RuntimeException("Expecting Content-Type of "
                  + IMimeTypes.MIME_APPLICATION_XML + ", not " + contentType);

         }

         final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();

         final AtomicLong txId = new AtomicLong();
         final AtomicLong readsOnCommitTime = new AtomicLong();

         /*
          * For example: <tx txId="-512" readsOnCommitTime="11201201212"/>
          */
         parser.parse(response.getInputStream(), new DefaultHandler2() {

            @Override
            public void startElement(final String uri, final String localName,
                  final String qName, final Attributes attributes) {

               if (!"tx".equals(qName))
                  throw new RuntimeException("Expecting: 'tx', but have: uri="
                        + uri + ", localName=" + localName + ", qName=" + qName);

               txId.set(Long.valueOf(attributes.getValue("txId")));

               readsOnCommitTime.set(Long.valueOf(attributes
                     .getValue("readsOnCommitTime")));

            }

         });

         // done.
         return new RemoteTx(txId.get(), readsOnCommitTime.get());

      } finally {

         if (response != null) {
            response.abort();
         }

      }

   }

}
