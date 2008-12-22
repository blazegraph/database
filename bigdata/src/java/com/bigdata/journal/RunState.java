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
package com.bigdata.journal;

/**
 * Enum of transaction run states.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum RunState {
    
    Active("Active"),
    Prepared("Prepared"),
    Committed("Committed"),
    Aborted("Aborted");
    
    private final String name;
    
    RunState(String name) {
    
        this.name = name;
        
    }
    
    public String toString() {
    
        return name;

    }

    /**
     * Return <code>true</code> iff a transition is allowable from the current
     * {@link RunState} to the proposed {@link RunState}.
     * <p>
     * Note: Only certain state transitions are allowed. These are:
     * {Active->Prepared, Active->Aborted, Active->Committed;
     * Prepared->Committed, Prepared->Aborted}. Both Committed and Aborted are
     * absorbing states.
     * <p>
     * Note: A transition to the same state is always allowed.
     * 
     * @param newval
     *            The proposed {@link RunState}.
     * 
     * @return <code>true</code> iff that state transition is allowed.
     */
    public boolean isTransitionAllowed(final RunState newval) {

        if (newval == null)
            throw new IllegalArgumentException();

        if (this.equals(newval))
            return true;
        
        if (this.equals(Active)) {

            if (newval.equals(Prepared))
                return true;

            if (newval.equals(RunState.Aborted))
                return true;

            if (newval.equals(Committed))
                return true;

            return false;

        } else if (this.equals(RunState.Prepared)) {

            if (newval.equals(Aborted))
                return true;

            if (newval.equals(Committed))
                return true;

            return false;

        }

        return false;
        
    }

}