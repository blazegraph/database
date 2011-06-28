package com.bigdata.rdf.changesets;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;

/**
 * Provides detailed information on changes made to statements in the database.
 * Change records are generated for any statements that are used in
 * addStatement() or removeStatements() operations on the SAIL connection, as 
 * well as any inferred statements that are added or removed as a result of 
 * truth maintenance when the database has inference enabled.
 * <p>
 * See {@link IChangeLog}.
 */
public interface IChangeRecord {

    /**
     * Attempting to add or remove statements can have a number of different 
     * effects.  This enum captures the different actions that can take place as 
     * a result of trying to add or remove a statement from the database.
     */
    public enum ChangeAction {
        
        /**
         * The focus statement was not in the database before and will be 
         * in the database after the commit.  This can be the result of either
         * explicit addStatement() operations on the SAIL connection, or from
         * new inferences being generated via truth maintenance when the 
         * database has inference enabled.  If the focus statement has a
         * statement type of explicit then it was added via an addStatement()
         * operation.  If the focus statement has a statement type of inferred
         * then it was added via truth maintenance.
         */
        INSERTED,
        
        /**
         * The focus statement was in the database before and will not 
         * be in the database after the commit. When the database has inference 
         * and truth maintenance enabled, the statement that is the focus of 
         * this change record was either an explicit statement that was the 
         * subject of a removeStatements() operation on the connection, or it 
         * was an inferred statement that was removed as a result of truth 
         * maintenance.  Either way, the statement is no longer provable as an 
         * inference using other statements still in the database after the 
         * commit.  If it were still provable, the explicit statement would have 
         * had its type changed to inferred, and the inferred statement would 
         * have remained untouched by truth maintenance.  If an inferred 
         * statement was the subject of a removeStatement() operation on the 
         * connection it would have resulted in a no-op, since inferences can 
         * only be removed via truth maintenance.
         */
        REMOVED,
        
        /**
         * This change action can only occur when inference and truth 
         * maintenance are enabled on the database.  Sometimes an attempt at 
         * statement addition or removal via an addStatement() or 
         * removeStatements() operation on the connection will result in a type 
         * change rather than an actual assertion or deletion.  When in 
         * inference mode, statements can have one of three statement types: 
         * explicit, inferred, or axiom (see {@link StatementEnum}).  There are 
         * several reasons why a statement will change type rather than be 
         * asserted or deleted:
         * <p>
         * <ul>
         * <li> A statement is asserted, but already exists in the database as 
         * an inference or an axiom.  The existing statement will have its type 
         * changed from inference or axiom to explicit. </li>
         * <li> An explicit statement is retracted, but is still provable by 
         * other means.  It will have its type changed from explicit to 
         * inference. </li>
         * <li> An explicit statement is retracted, but is one of the axioms 
         * needed for inference.  It will have its type changed from explicit to 
         * axiom. </li>
         * </ul>
         */
        UPDATED,
        
//        /**
//         * This change action can occur for one of two reasons:
//         * <p>
//         * <ul>
//         * <li> A statement is asserted, but already exists in the database as 
//         * an explicit statement. </li>
//         * <li> An inferred statement or an axiom is retracted.  Only explicit 
//         * statements can be retracted via removeStatements() operations. </li>
//         * </ul>
//         */
//        NO_OP
        
    }
    
    /**
     * Return the ISPO that is the focus of this change record.
     * 
     * @return
     *          the {@link ISPO}
     */
    ISPO getStatement();
    
    /**
     * Return the change action for this change record.
     * 
     * @return
     *          the {@link ChangeAction}
     */
    ChangeAction getChangeAction();
    
//    /**
//     * If the change action is {@link ChangeAction#TYPE_CHANGE}, this method
//     * will return the old statement type of the focus statement.  The
//     * new statement type is available on the focus statement itself.
//     * 
//     * @return
//     *          the old statement type of the focus statement
//     */
//    StatementEnum getOldStatementType();
    
}
