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

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.XMLBuilder.Node;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ITxState;
import com.bigdata.util.InnerCause;
import com.bigdata.util.NV;

/**
 * Servlet provides a REST interface for managing stand-off read/write
 * transaction. Given a namespace that is provisioned for read/write
 * transactions, a client can create a transaction (obtaining an identifier for
 * that transaction), do work with that transaction (including mutation and
 * query), prepare the transaction, and finally commit or abort the transaction.
 * <p>
 * Transaction isolation requires that the namespace is provisioned with support
 * for isolatable indices. See {@link BigdataSail.Options#ISOLATABLE_INDICES}.
 * When isolation is enabled for a namespace, the updates for the namespace will
 * be buffered by isolated indices. When a transaction prepares, the write set
 * will be validated by comparing the modified tuples against the unisolated
 * indices touched by the write set and looking for conflicts (revision
 * timestamps that have been updated since the transaction start). If a conflict
 * can not be reconciled, then the transaction will be unable to commit.
 * <p>
 * Read only transactions may also be requested in order to have snapshot
 * isolation across a series of queries. blazegraph provides snapshot isolation
 * for queries regardless. The use of an explicit read only transaction is only
 * required to maintain the same snapshot across multiple queries.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
 *      transactions in the REST API</a>
 * 
 * @see BigdataSail.Options#ISOLATABLE_INDICES
 */
public class TxServlet extends BigdataRDFServlet {

   /**
     * 
     */
   private static final long serialVersionUID = 1L;

   static private final transient Logger log = Logger
         .getLogger(TxServlet.class);

   /**
    * The URL query parameter for a PREPARE message.
    */
   static final transient String ATTR_PREPARE = "PREPARE";

   /**
    * The URL query parameter for a COMMIT message.
    */
   static final transient String ATTR_COMMIT = "COMMIT";

   /**
    * The URL query parameter for a ABORT message.
    */
   static final transient String ATTR_ABORT = "ABORT";

   /**
    * The URL query parameter for a STATUS message.
    */
   static final transient String ATTR_STATUS = "STATUS";

   /**
    * The name of the URL query parameter which indicates the timestamp for a
    * CREATE message.
    */
   static final transient String ATTR_TIMESTAMP = QueryServlet.ATTR_TIMESTAMP;

   public TxServlet() {

   }

   /**
    * Methods for transaction management (create, prepare, isActive, commit,
    * abort).
    */
   @Override
   protected void doPost(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      if (!isReadable(getServletContext(), req, resp)) {

         // Service must be available.
         return;
      }

      if (req.getRequestURI().endsWith("/tx")) {

         // CREATE-TX
         doCreateTx(req, resp);

         return;

      } else if (req.getParameter(ATTR_PREPARE) != null) {

         doPrepareTx(req, resp);

      } else if (req.getParameter(ATTR_ABORT) != null) {

         doAbortTx(req, resp);

      } else if (req.getParameter(ATTR_COMMIT) != null) {

         doCommitTx(req, resp);

      } else if (req.getParameter(ATTR_STATUS) != null) {

         doStatusTx(req, resp);

      } else {

         buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
               MIME_TEXT_HTML, "Unknown transaction management request");

      }

   }

   @Override
   protected void doGet(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      if (!isReadable(getServletContext(), req, resp)) {

         // Service must be available.
         return;
      }

      if (req.getRequestURI().endsWith("/tx")) {

         // LIST-TX
         doListTx(req, resp);

         return;

      } else if (req.getParameter(ATTR_STATUS) != null) {

         doStatusTx(req, resp);

      } else {

         buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
               MIME_TEXT_HTML, "Unknown transaction management request");

      }

   }

   /** <code>CREATE-TX(?timestamp=...)</code> */
   private void doCreateTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      // Note: This parameter has default values for CREATE-TX.
      final long timestamp = getTimestamp(req);

      if (timestamp == ITx.UNISOLATED) {

         // read/write transaction.

         if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
         }

         if (getIndexManager() instanceof IBigdataFederation) {

            buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
                  MIME_TEXT_HTML,
                  "Scale-out does not support distributed read/write transactions");

         }

      } else if (timestamp == ITx.READ_COMMITTED) {

         // read-only transaction reading from the lastCommitTime.

      } else if (timestamp > ITx.UNISOLATED) {

         /*
          * Create a read-only transaction reading from the most recent
          * committed state whose commit timestamp is less than or equal to
          * timestamp.
          */

      } else {

         buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
               MIME_TEXT_HTML, "Illegal value: timestamp=" + timestamp);

      }

      try {

         /*
          * Now that we have validated the request, create the transaction and
          * report it to the client.
          */

         final long txId = getBigdataRDFContext().newTx(timestamp);

         final ITx tx = ((Journal) getIndexManager()).getTransactionManager()
               .getTx(txId);

         // TODO This URL is correct IFF we only allow CREATE-TX at the correct
         // path.
         final String txURL = req.getRequestURL().append('/')
               .append(Long.valueOf(txId)).toString();

         final StringWriter w = new StringWriter();

         final XMLBuilder t = new XMLBuilder(w);

         t.root("tx")//
               .attr("txId", txId)//
               .attr("readsOnCommitTime", tx.getReadsOnCommitTime())//
               .attr("readOnly", tx.isReadOnly())//
               .attr("aborted", tx.isAborted())//
               .attr("committed", tx.isCommitted())//
               .close();

         buildAndCommitResponse(resp, HttpServletResponse.SC_CREATED,
               MIME_APPLICATION_XML, w.toString(), new NV("Location", txURL));

      } catch (Throwable t) {

         launderThrowable(t, resp, "CREATE-TX");

      }

   }

   /**
    * Return <code>true</code> iff a transaction identifier was parsed from the
    * request and otherwise commit a {@link HttpServletResponse#SC_BAD_REQUEST}
    * response.
    * <p>
    * COMMIT-TX, ABORT-TX, PREPARE-TX, and STATUS-TX all need to extract the
    * transaction identifier from the last component of the path which should be
    * <code>/tx/txId</code>.
    * 
    * @param req
    *           The request.
    * @param resp
    *           The response.
    * @param ref
    *           The transaction identifier will be saved in this reference.
    * @return <code>true</code> if a transaction identifier was extracted. if
    *         <code>false</code> then no transaction identifier was found and a
    *         {@link HttpServletResponse#SC_BAD_REQUEST} response was committed.
    * 
    * @throws IOException
    */
   private final boolean getTxId(final HttpServletRequest req,
         final HttpServletResponse resp, final AtomicLong ref) throws IOException {
      
      /*
       * The path info follows the servlet and starts with /. So for
       * "/bigdata/tx/559" this will be "/559". We strip of the leading "/" and
       * the rest is the transaction identifier.
       */
      final String pathInfo = req.getPathInfo();
      
      assert pathInfo != null;
      
      if (pathInfo.length() < 2) {
         buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
               MIME_TEXT_HTML, "No transaction identifier in path: pathInfo="
                     + pathInfo);
         return false;
      }

      // This should be the transaction identifier.
      final String s = pathInfo.substring(1/* beginIndex */);

      /*
       * Validate the transaction identifier syntactically.
       */
      for (int i = 0; i < s.length(); i++) {
         if (!Character.isDigit(s.charAt(i)) && s.charAt(i) != '-') {
            buildAndCommitResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
                  MIME_TEXT_HTML,
                  "Transaction identifier is not numeric: pathInfo=" + pathInfo);
            return false;
         }
      }

      final long txId = Long.valueOf(s);
      
      ref.set(txId);
      
      return true;

   }
   
   /**
    * ABORT-TX(txId)
    */
   private void doAbortTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      final AtomicLong txId = new AtomicLong();

      if (!getTxId(req, resp, txId))
         return;

      try {

         if (getIndexManager() instanceof IBigdataFederation) {

            ((IBigdataFederation<?>) getIndexManager()).getTransactionService()
                  .abort(txId.get());

         } else {

            // On the journal we can lookup the Tx.
            final ITx tx = ((Journal) getIndexManager())
                  .getTransactionManager().getTx(txId.get());

            if (tx == null) {

               // No such transaction.
               buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                     MIME_TEXT_PLAIN, "ABORT-TX: Transaction not found: txId="
                           + txId);
               return;

            }

            if (!tx.isEmptyWriteSet()) {
               // Dirty tx. Must be leader.
               if (!isWritable(getServletContext(), req, resp)) {
                  // Service must be writable.
                  return;
               }
            }

            ((Journal) getIndexManager()).abort(txId.get());

         }

         buildAndCommitResponse(resp, HttpServletResponse.SC_OK,
               MIME_TEXT_PLAIN, "Aborted: " + txId);

      } catch (Throwable t) {

         if (InnerCause.isInnerCause(t, IllegalStateException.class)) {
            
            /*
             * TODO This is pretty diagnostic for the Journal. For scale-out
             * there could be other root causes that might throw the same
             * exception. We could make this 100% diagnostic by subclassing
             * IllegalStateException and throwing a typed
             * TransactionNotFoundException. At which point this condition could
             * be pushed down inside of launderThrowabler()
             */

            buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                  MIME_TEXT_PLAIN, "ABORT-TX: Transaction not found: txId="
                        + txId);

            return;
            
         }

         // some other error.
         launderThrowable(t, resp, "ABORT-TX:: txId=" + txId);
         
      }

   }

   /**
    * COMMIT-TX(txId)
    */
   private void doCommitTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {
      
      final AtomicLong txId = new AtomicLong();

      if (!getTxId(req, resp, txId))
         return;

      try {

         if (getIndexManager() instanceof IBigdataFederation) {

            ((IBigdataFederation<?>) getIndexManager()).getTransactionService()
                  .commit(txId.get());

         } else {

            // On the journal we can lookup the Tx.
            final ITx tx = ((Journal) getIndexManager())
                  .getTransactionManager().getTx(txId.get());

            if (tx == null) {

               // No such transaction.
               buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                     MIME_TEXT_PLAIN, "COMMIT-TX: Transaction not found: txId="
                           + txId);
               return;

            }

            if (!tx.isEmptyWriteSet()) {
               // Dirty tx. Must be leader.
               if (!isWritable(getServletContext(), req, resp)) {
                  // Service must be writable.
                  return;
               }
            }

            ((Journal) getIndexManager()).commit(txId.get());

         }
         
         buildAndCommitResponse(resp, HttpServletResponse.SC_OK,
               MIME_TEXT_PLAIN, "Committed: " + txId);

      } catch (Throwable t) {

         if (InnerCause.isInnerCause(t, IllegalStateException.class)) {
            
            /*
             * TODO This is pretty diagnostic for the Journal. For scale-out
             * there could be other root causes that might throw the same
             * exception. We could make this 100% diagnostic by subclassing
             * IllegalStateException and throwing a typed
             * TransactionNotFoundException. At which point this condition could
             * be pushed down inside of launderThrowabler()
             */

            buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                  MIME_TEXT_PLAIN, "COMMIT-TX: Transaction not found: txId="
                        + txId);

            return;
            
         }

         // some other error.
         launderThrowable(t, resp, "COMMIT-TX:: txId=" + txId);
         
      }

   }

   /**
    * <code>PREPARE-TX(txId)</code>
    * 
    * FIXME Test suite for this at the Journal level. Make sure that there are
    * no undesired side-effects from validation. For example, the writeSet of
    * the tx is modified by validation if a conflict is resolved. Is that
    * modification Ok if we do not go ahead and commit? Should it be rolled
    * back? Can we have additional writes on the tx and reconcile additional
    * conflicts in another PREPARE or a COMMIT?
    */
   private void doPrepareTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      final long begin = System.nanoTime();

      final AtomicLong txId = new AtomicLong();

      if (!getTxId(req, resp, txId))
         return;

      final boolean ok;
      try {

         if (getIndexManager() instanceof IBigdataFederation) {

            // Scale-out does not have read/write transactions. This is a NOP.
            ok = true;

         } else {

            // On the journal we can lookup the Tx.
            final ITx tx = ((Journal) getIndexManager())
                  .getTransactionManager().getTx(txId.get());

            if (tx == null) {

               // No such transaction.
               buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                     MIME_TEXT_PLAIN,
                     "PREPARE-TX: Transaction not found: txId=" + txId);

               return;

            }

            if (!tx.isEmptyWriteSet()) {
               // Dirty tx. Must be leader.
               if (!isWritable(getServletContext(), req, resp)) {
                  // Service must be writable.
                  return;
               }
            }

            ok = ((Journal) getIndexManager()).prepare(txId.get());

         }

         final long elapsed = System.nanoTime() - begin;

         buildAndCommitBooleanResponse(resp, ok,
               TimeUnit.NANOSECONDS.toMillis(elapsed));

      } catch (Throwable t) {

         if (InnerCause.isInnerCause(t, IllegalStateException.class)) {

            /*
             * TODO This is pretty diagnostic for the Journal. For scale-out
             * there could be other root causes that might throw the same
             * exception. We could make this 100% diagnostic by subclassing
             * IllegalStateException and throwing a typed
             * TransactionNotFoundException. At which point this condition could
             * be pushed down inside of launderThrowabler()
             */

            buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                  MIME_TEXT_PLAIN, "PREPARE-TX: Transaction not found: txId="
                        + txId);

            return;

         }

         // some other error.
         launderThrowable(t, resp, "PREPARE-TX:: txId=" + txId);

      }

   }

   /**
    * <code>STATUS-TX</code>
    * 
    * TODO IFF GET sure that caching is disabled for this!
    */
   private void doStatusTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      final long begin = System.nanoTime();

      final AtomicLong txId = new AtomicLong();

      if (!getTxId(req, resp, txId))
         return;

      try {

         if (getIndexManager() instanceof IBigdataFederation) {

            // Scale-out does not have read/write transactions. This is a NOP.

         } else {

            // On the journal we can lookup the Tx.
            final ITx tx = ((Journal) getIndexManager())
                  .getTransactionManager().getTx(txId.get());

            if (tx == null) {

               // No such transaction.
               buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                     MIME_TEXT_PLAIN,
                     "STATUS-TX: Transaction not found: txId=" + txId);

               return;

            }

            final StringWriter w = new StringWriter();

            final XMLBuilder t = new XMLBuilder(w);

            t.root("tx")//
                  .attr("txId", txId)//
                  .attr("readsOnCommitTime", tx.getReadsOnCommitTime())//
                  .attr("readOnly", tx.isReadOnly())//
                  .attr("aborted", tx.isAborted())//
                  .attr("committed", tx.isCommitted())//
                  .close();

            buildAndCommitResponse(resp, HttpServletResponse.SC_OK,
                  MIME_APPLICATION_XML, w.toString());

         }

      } catch (Throwable t) {

         if (InnerCause.isInnerCause(t, IllegalStateException.class)) {

            /*
             * TODO This is pretty diagnostic for the Journal. For scale-out
             * there could be other root causes that might throw the same
             * exception. We could make this 100% diagnostic by subclassing
             * IllegalStateException and throwing a typed
             * TransactionNotFoundException. At which point this condition could
             * be pushed down inside of launderThrowabler()
             */

            buildAndCommitResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                  MIME_TEXT_PLAIN, "STATUS-TX: Transaction not found: txId="
                        + txId);

            return;

         }

         // some other error.
         launderThrowable(t, resp, "PREPARE-TX:: txId=" + txId);

      }

   }

   /**
    * <code>LIST-TX</code>
    */
   private void doListTx(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      final ITxState[] a;
      if (getIndexManager() instanceof IBigdataFederation) {

         // NOP
         a = new ITxState[] {};

      } else {

         // The Journal will self-report the active transactions.
         a = ((Journal) getIndexManager()).getTransactionManager()
               .getActiveTx();

      }

      final StringWriter w = new StringWriter();

      final XMLBuilder t = new XMLBuilder(w);

      final Node root = t.root("transactions");
      
      for (ITxState tx : a) {

         root.node("tx")//
               .attr("txId", tx.getStartTimestamp())//
               .attr("readsOnCommitTime", tx.getReadsOnCommitTime())//
               .attr("readOnly", tx.isReadOnly())//
               .attr("aborted", tx.isAborted())//
               .attr("committed", tx.isCommitted())//
               .close();

      }

      /*
       * TODO What is an appropriate cache strategy here?
       */
      buildAndCommitResponse(resp, HttpServletResponse.SC_OK,
            MIME_APPLICATION_XML, w.toString(), //
            // disable caching.
            new NV("Cache-Control", "no-cache")
            /*
             * Sets the cache behavior -- the data should be good for up to 60
             * seconds unless you change the query parameters. These cache
             * control parameters SHOULD indicate that the response is valid for
             * 60 seconds, that the client must revalidate, and that the
             * response is cachable even if the client was authenticated.
             */
//            new NV("Cache-Control", "max-age=60, must-revalidate, public")//
      );

   }

}
