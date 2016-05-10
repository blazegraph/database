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
