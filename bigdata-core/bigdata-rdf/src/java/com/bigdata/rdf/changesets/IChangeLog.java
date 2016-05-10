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
package com.bigdata.rdf.changesets;

/**
 * Provides detailed information on changes made to statements in the database.
 * Change records are generated for any statements that are used in
 * addStatement() or removeStatements() operations on the SAIL connection, as 
 * well as any inferred statements that are added or removed as a result of 
 * truth maintenance when the database has inference enabled.  Change records
 * will be sent to an instance of this class via the 
 * {@link #changeEvent(IChangeRecord)} method.  These events will
 * occur on an ongoing basis as statements are added to or removed from the
 * indices.  It is the change log's responsibility to collect change records.  
 * When the transaction is actually committed (or aborted), the change log will 
 * receive notification via {@link #transactionCommited()} or  
 * {@link #transactionAborted()}.
 */
public interface IChangeLog {
    
    /**
     * Occurs when a statement add or remove is flushed to the indices (but
     * not yet committed).
     * 
     * @param record
     *          the {@link IChangeRecord}
     */
    void changeEvent(final IChangeRecord record);

    /**
     * Message issued when a new transaction will begin.
     */
    void transactionBegin();
    
    /**
     * Message issued when preparing for a commit. The next message will be
     * either {@link #transactionCommited(long)} or
     * {@link #transactionAborted()}.
     * <p>
     * Note: The listener will have observed all updates by the time this
     * message is generated. Thus, this message can be used to validate
     * post-conditions for the transaction.
     */
    void transactionPrepare();
    
    /**
     * Occurs when the current SAIL transaction is committed.
     * 
     * @param commitTime
     *            The timestamp associated with the commit point.
     */
    void transactionCommited(final long commitTime);
    
    /**
     * Occurs if the current SAIL transaction is aborted.
     */
    void transactionAborted();
    
    /**
     * Close any open resources.
     */
    void close();
    
}
