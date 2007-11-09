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
package com.bigdata.service.mapReduce;

import java.io.Serializable;
import java.util.UUID;

/**
 * Outcome for a task.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Outcome implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -8924200426202657189L;

    private final UUID task;
    private final Status status;
    private final String message;

    /**
     * The hash code for the task UUID.
     */
    public int hashCode() {
        
        return task.hashCode();
        
    }
    
    public String toString() {
        
        return super.toString()+"{ task="+task+", status="+status+", message="+message+"}";
        
    }
    
    /**
     * 
     * @param task
     *            The task identifier (required).
     * @param status
     *            The task status (required).
     * @param message
     *            The message associated with the outcome (required if the
     *            {@link Status#Error}).
     */
    public Outcome(UUID task,Status status,String message) {
        
        if (task == null)
            throw new IllegalArgumentException();

        if (status == null)
            throw new IllegalArgumentException();
        
        if(status==Status.Error && message==null)
            throw new IllegalArgumentException();
        
        this.task = task;
        
        this.status = status;
        
        this.message = message;
        
    }
    
    /**
     * The task identifier.
     */
    public UUID getTask() {
    
        return task;
        
    }

    /**
     * The task status.
     */
    public Status getStatus() {
        
        return status;
        
    }
    
    /**
     * The message associated with the {@link Outcome} (may be null).
     */
    public String getMessage() {
        
        return message;
        
    }
    
}
