package com.bigdata.rdf.changesets;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * This delegating change log allows change events to be propogated to multiple
 * delegates through a listener pattern.
 * 
 * @author mike
 *
 */
public class DelegatingChangeLog implements IChangeLog {

    protected static final Logger log = Logger.getLogger(DelegatingChangeLog.class);

    private Set<IChangeLog> delegates;
    
    public DelegatingChangeLog() {
    	this.delegates = new LinkedHashSet<IChangeLog>();
    }
    
    public synchronized void addDelegate(final IChangeLog delegate) {
    	this.delegates.add(delegate);
    }
    
    public synchronized void removeDelegate(final IChangeLog delegate) {
    	this.delegates.remove(delegate);
    }
    
    /**
     * See {@link IChangeLog#changeEvent(IChangeRecord)}.
     */
    public synchronized void changeEvent(final IChangeRecord record) {
        
        if (log.isInfoEnabled()) 
            log.info(record);
        
        for (IChangeLog delegate : delegates) {

            delegate.changeEvent(record);
            
        }
        
    }
    
    /**
     * See {@link IChangeLog#transactionCommited()}.
     */
    public synchronized void transactionCommited(final long commitTime) {
    
        if (log.isInfoEnabled()) 
            log.info("transaction committed");
        
        for (IChangeLog delegate : delegates) {
        
            delegate.transactionCommited(commitTime);
            
        }
        
    }
    
    /**
     * See {@link IChangeLog#transactionAborted()}.
     */
    public synchronized void transactionAborted() {

        if (log.isInfoEnabled()) 
            log.info("transaction aborted");
        
        for (IChangeLog delegate : delegates) {

            delegate.transactionAborted();
            
        }
        
    }
    
}
