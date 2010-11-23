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
     * Occurs when the current SAIL transaction is committed.
     */
    void transactionCommited();
    
    /**
     * Occurs if the current SAIL transaction is aborted.
     */
    void transactionAborted();
    
}
