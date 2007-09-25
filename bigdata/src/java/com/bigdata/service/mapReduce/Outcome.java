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
