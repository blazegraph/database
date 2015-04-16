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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;

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

//   /**
//    * The name of the parameter/attribute that contains the SPARQL query.
//    * <p>
//    * Note: This can be either a URL query parameter or a servlet request
//    * attribute. The latter is used to support chaining of a linked data GET as
//    * a SPARQL DESCRIBE query.
//    */
//   static final transient String ATTR_QUERY = "query";

//   /**
//    * The name of the URL query parameter which indicates the timestamp against
//    * which an operation will be carried out.
//    * 
//    * @see BigdataRDFServlet#getTimestamp(HttpServletRequest)
//    */
//   static final transient String ATTR_TIMESTAMP = "timestamp";

   public TxServlet() {

   }

   /**
    * Methods for transaction management (create, prepare, isActive, commit,
    * abort).
    */
   @Override
   protected void doPost(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      throw new UnsupportedOperationException();

   }

   /**
    * TODO Does it make sense to allow any GET requests? I can not think of a
    * reason why we would want to allow HTTP caching for transaction management.
    * Maybe for a list of open transactions? Maybe we can allow GET for the tx
    * status or list of open transactions if we explicitly disable caching in
    * the HTTP response headers?
    */
   @Override
   protected void doGet(final HttpServletRequest req,
         final HttpServletResponse resp) throws IOException {

      throw new UnsupportedOperationException();

   }

}
