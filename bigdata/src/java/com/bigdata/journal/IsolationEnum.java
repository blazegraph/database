/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Feb 27, 2006
 */
package com.bigdata.journal;

/**
 * Isolation levels for a transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum IsolationEnum {

    /**
     * A read-only transaction that will read any data successfully
     * committed on the database (the view provided by the transaction does
     * not remain valid as of the transaction start time but evolves as
     * concurrent transactions commit) (level 0).
     */
    ReadCommitted(0),

    /**
     * A fully isolated read-only transaction (level 1).
     */
    ReadOnly(1),

    /**
     * A fully isolated read-write transaction (level 2).
     */
    ReadWrite(2);

    private final int level;

    private IsolationEnum(int level) {

        this.level = level;
        
    }

    /**
     * The integer code for the isolation level.
     */
    public int getLevel() {
        
        return level;
        
    }
    
    /**
     * Convert an integer isolation level into an {@link IsolationEnum}.
     * 
     * @param level
     *            The isolation level.
     *            
     * @return The corresponding enum value.
     */
    public IsolationEnum get(int level) {

        switch (level) {
        case 0:
            return ReadCommitted;
        case 1:
            return ReadOnly;
        case 2:
            return ReadWrite;
        default:
            throw new IllegalArgumentException("level=" + level);
        }

    }
    
}
