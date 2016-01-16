/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.client;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

   /**
    * Flyweight constructor for stateless transaction manager client.
    * 
    * @param remoteRepositoryManager
    */
   public RemoteTransactionManager(
         final RemoteRepositoryManager remoteRepositoryManager) {

      if (remoteRepositoryManager == null)
         throw new IllegalArgumentException();

      this.mgr = remoteRepositoryManager;

   }

   private class RemoteTxState0 implements IRemoteTxState0 {

      /**
       * The transaction identifier.
       */
      private final long txId;

      /**
       * The commit time on which the transaction will read.
       */
      private final long readsOnCommitTime;

      private RemoteTxState0(final long txId, final long readsOnCommitTime) {
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
         return false;
      }

   }

   /**
    * Note: It is not possible to establish a canonical factory pattern for a
    * {@link RemoteTx} because we can not be certain that we are not accessing
    * the same REST API through different URls and because we can not be certain
    * that different URLs are not in fact the same REST API. Therefore there is
    * nothing that will allow us to ensure that the returned {@link RemoteTx}
    * objects are truely 1:1 with the transaction on the database. This means
    * that {@link RemoteTx#lock} that serializes operations on the
    * {@link RemoteTx} may be defeated by this factory if it is used for
    * transactions discovered by <code>LIST-TX</code> rather than just those
    * known to be created for the client by <code>CREATE-TX</code>
    * 
    * FIXME The [readsOnCommitTime] is not available for scale-out. Either allow
    * [null] or use -1L if it is not available. Make this consistent in the
    * server, client, and documentation.
    */
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
       * server, so the client's belief could be incorrect. The client uses this
       * information to refuse to attempt operations if it believes that the
       * transaction is not active.
       */
      private final AtomicBoolean active = new AtomicBoolean(true);

      /**
       * Note: This object is used both to provide synchronization.
       */
      private Object lock = this;

      private RemoteTx(final IRemoteTxState0 tmp) {
         final long txId = tmp.getTxId();
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
         this.readsOnCommitTime = tmp.getReadsOnCommitTime();
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
            throw new RemoteTransactionNotFoundException(txId,
                  mgr.getBaseServiceURL());
      }

      @Override
      public boolean isActive() {
         return active.get();
      }

      @Override
      public boolean prepare() throws RemoteTransactionNotFoundException {
         synchronized (lock) {
            assertClientThinksTxActive();
            // tell the server to prepare the transaction.
            return prepareTx(txId);
         }
      }

      @Override
      public void abort() throws RemoteTransactionNotFoundException {
         synchronized (lock) {
            assertClientThinksTxActive();
            abortTx(txId);
            active.set(false);
         }
      }

      @Override
      public void commit() throws RemoteTransactionValidationException,
            RemoteTransactionNotFoundException {
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
   public IRemoteTx createTx(final long timestamp) {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx");

      opts.method = "POST";

      opts.addRequestParam("timestamp", Long.toString(timestamp));

      JettyResponseListener response = null;

      try {

         final JettyResponseListener listener = RemoteRepository
               .checkResponseCode(response = mgr.doConnect(opts));

         switch (listener.getStatus()) {
         case 201:
            return new RemoteTx(singleTxResponse(response));
         default:
            throw new HttpException(listener.getStatus(), "status="
                  + listener.getStatus() + ", reason" + listener.getReason());
         }

      } catch (Exception t) {

         throw new RuntimeException(t);

      } finally {

         if (response != null)
            response.abort();

      }

   }

   /**
    * <code>LIST-TX</code>: Return the set of active transactions.
    */
   public Iterator<IRemoteTxState0> listTx() {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx");

      opts.method = "GET";

      JettyResponseListener response = null;

      try {

         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

         // Note: iterator return supports streaming results (not implemented).
         return multiTxResponse(response).iterator();

      } catch (Exception e) {

         throw new RuntimeException(e);
         
      } finally {

         if (response != null)
            response.abort();

      }

   }

   /**
    * STATUS-TX: Return information about a transaction, including whether or
    * not it is active.
    * 
    * @param txId
    *           The transaction identifier.
    * 
    * @return The {@link IRemoteTx} for that transaction.
    * 
    * @throws RemoteTransactionNotFoundException
    *            if the transaction was not found on the server.
    * 
    * @throws RemoteTransactionNotFoundException
    */
   public IRemoteTxState0 statusTx(final long txId)
         throws RemoteTransactionNotFoundException {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/" + Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("STATUS");

      JettyResponseListener response = null;

      try {

         RemoteRepository.checkResponseCode(response = mgr.doConnect(opts));

         return singleTxResponse(response);

      } catch (HttpException ex) {

         switch (ex.getStatusCode()) {
         case 404: // GONE
            throw new RemoteTransactionNotFoundException(txId,
                  mgr.getBaseServiceURL());
         default: // Unexpected status code.
            throw new RuntimeException(ex);
         }

      } catch(Exception t) {
         
         throw new RuntimeException(t);
         
      } finally {

         if (response != null)
            response.abort();

      }

   }

   /**
    * <code>PREPARE-TX</code>: Validate a transaction on the server.
    * 
    * @param txId
    *           The transaction identifier.
    * 
    * @return <code>true</code> if the transaction is read-only or if write set
    *         of the transaction was validated and <code>false</code> iff the
    *         server was unable to validate a read-write transaction known to
    *         the server.
    * 
    * @throws RemoteTransactionNotFoundException
    *            if there is no such transaction on the server.
    */
   private boolean prepareTx(final long txId)
         throws RemoteTransactionNotFoundException {

      if (!isReadWriteTx(txId)) {
         // NOP unless read/write transaction.
         return true;
      }

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/" + Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("PREPARE");

      JettyResponseListener response = null;

      try {

         final JettyResponseListener listener = RemoteRepository
               .checkResponseCode(response = mgr.doConnect(opts));

         switch (listener.getStatus()) {
         case 200:
            return true;
         default:
            throw new HttpException(listener.getStatus(), "status="
                  + listener.getStatus() + ", reason" + listener.getReason());
         }

      } catch (HttpException ex) {

         switch (ex.getStatusCode()) {
         case 404: // GONE
            throw new RemoteTransactionNotFoundException(txId,
                  mgr.getBaseServiceURL());
         case 409: // CONFLICT
            // validation failed.
            return false;
         default: // Unexpected status code.
            throw new RuntimeException(ex);
         }

      } catch(Exception t) {
         
         throw new RuntimeException(t);
         
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
   private void abortTx(final long txId)
         throws RemoteTransactionNotFoundException {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/" + Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("ABORT");

      JettyResponseListener response = null;

      try {

         final JettyResponseListener listener = RemoteRepository
               .checkResponseCode(response = mgr.doConnect(opts));

         switch (listener.getStatus()) {
         case 200:
            return;
         default:
            throw new HttpException(listener.getStatus(), "status="
                  + listener.getStatus() + ", reason" + listener.getReason());
         }

      } catch (HttpException ex) {

         switch (ex.getStatusCode()) {
         case 404: // GONE
            throw new RemoteTransactionNotFoundException(txId,
                  mgr.getBaseServiceURL());
         default: // Unexpected status code.
            throw new RuntimeException(ex);
         }

      } catch(Exception t) {
         
         throw new RuntimeException(t);
         
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
    * @throws RemoteTransactionNotFoundException
    *            if there is no such transaction on the server.
    * @throws RemoteTransactionValidationException
    *            if the transaction exists but could not be validated.
    */
   private void commitTx(final long txId)
         throws RemoteTransactionNotFoundException,
         RemoteTransactionValidationException {

      final ConnectOptions opts = new ConnectOptions(mgr.getBaseServiceURL()
            + "/tx/" + Long.toString(txId));

      opts.method = "POST";

      opts.addRequestParam("COMMIT");

      JettyResponseListener response = null;

      try {

         final JettyResponseListener listener = RemoteRepository
               .checkResponseCode(response = mgr.doConnect(opts));

         switch (listener.getStatus()) {
         case 200:
            return;
         default:
            throw new HttpException(listener.getStatus(), "status="
                  + listener.getStatus() + ", reason" + listener.getReason());
         }
         
      } catch(HttpException ex) {

         switch (ex.getStatusCode()) {
         case 404: // GONE
            throw new RemoteTransactionNotFoundException(txId,
                  mgr.getBaseServiceURL());
         case 409: // CONFLICT
            // Validation failed.
            throw new RemoteTransactionValidationException(txId,
                  mgr.getBaseServiceURL());
         default: // Unexpected status code.
            throw new RuntimeException(ex);
         }

      } catch(Exception t) {
         
         throw new RuntimeException(t);
         
      } finally {

         if (response != null)
            response.abort();

      }

   }

   private IRemoteTxState0 singleTxResponse(final JettyResponseListener response)
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

               if ("response".equals(qName)) {
                  // This is the outer element.
                  return;
               }

               if (!"tx".equals(qName))
                  throw new RuntimeException("Expecting: 'tx', but have: uri="
                        + uri + ", localName=" + localName + ", qName=" + qName);

               txId.set(Long.valueOf(attributes.getValue("txId")));

               readsOnCommitTime.set(Long.valueOf(attributes
                     .getValue("readsOnCommitTime")));

            }

         });

         // done.
         return new RemoteTxState0(txId.get(), readsOnCommitTime.get());

      } finally {

         if (response != null) {
            response.abort();
         }

      }

   }

   private List<IRemoteTxState0> multiTxResponse(
         final JettyResponseListener response) throws Exception {

      try {

         final String contentType = response.getContentType();

         if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

            throw new RuntimeException("Expecting Content-Type of "
                  + IMimeTypes.MIME_APPLICATION_XML + ", not " + contentType);

         }

         final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();

         final List<IRemoteTxState0> list = new LinkedList<IRemoteTxState0>();

         /*
          * For example: <tx txId="-512" readsOnCommitTime="11201201212"/>
          */
         parser.parse(response.getInputStream(), new DefaultHandler2() {

            @Override
            public void startElement(final String uri, final String localName,
                  final String qName, final Attributes attributes) {

               if ("response".equals(qName)) {
                  // This is the outer element.
                  return;
               }

               if (!"tx".equals(qName))
                  throw new RuntimeException("Expecting: 'tx', but have: uri="
                        + uri + ", localName=" + localName + ", qName=" + qName);

               final long txId = Long.valueOf(attributes.getValue("txId"));

               final long readsOnCommitTime = Long.valueOf(attributes
                     .getValue("readsOnCommitTime"));

               list.add(new RemoteTxState0(txId, readsOnCommitTime));

            }

         });

         // done.
         return list;

      } finally {

         if (response != null) {
            response.abort();
         }

      }

   }

}
