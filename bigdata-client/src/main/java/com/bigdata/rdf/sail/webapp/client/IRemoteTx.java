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

/**
 * Interface for blazegraph transactions on the client.
 * 
 * @author bryan
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
 *      transactions in the REST API</a>
 */
public interface IRemoteTx extends IRemoteTxState0 {

   /**
    * Return <code>true</code> iff the client believes that transaction is
    * active (it exists and has not been aborted nor committed). Note that
    * the transaction may have been aborted on the server, in which case the
    * client will not know this until it tries to {@link #prepare()},
    * {@link #abort()}, or {@link #commit()}.
    */
   boolean isActive();

   /**
    * Return true if the write set of the transaction passes validation at
    * the time that the server processes this request. If a transaction fails
    * validation then {@link #commit()} will fail for that transaction. If it
    * passes validation, then {@link #commit()} is not known to fail at this
    * time.
    * <p>
    * Note: transactions always validate during {@link #commit()}. Invoking
    * this method explicitly is discouraged since it just adds overhead
    * unless you are actually going to gain something from the information.
    * 
    * @throws RemoteTransactionValidationException
    *            if the transaction was not found on the server.
    */
   boolean prepare() throws RemoteTransactionNotFoundException;

   /**
    * Aborts a read/write transaction (discarding its write set) -or-
    * deactivates a read-only transaction.
    * <p>
    * Note: You MUST always either {@link #abort()} or {@link #commit()} a
    * read-only transaction in order to release the resources on the server!
    * 
    * @throws RemoteTransactionValidationException
    *            if the transaction was not found on the server.
    */
   void abort() throws RemoteTransactionNotFoundException;
   
   /**
    * Prepares and commits a read/write transaction -or- deactivates a
    * read-only transaction.
    * <p>
    * Note: You MUST always either {@link #abort()} or {@link #commit()} a
    * read-only transaction in order to release the resources on the server!
    * 
    * @throws RemoteTransactionValidationException
    *            if the transaction was not found on the server.
    * @throws RemoteTransactionValidationException
    *            if the transaction exists but could not be validated.
    */
   void commit() throws RemoteTransactionNotFoundException;
 
}
