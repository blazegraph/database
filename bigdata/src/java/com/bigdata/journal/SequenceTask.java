package com.bigdata.journal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A task comprised of a sequence of operations. All operations MUST run on
 * the same journal and task service (read service, write service, or
 * transaction service). The individual results are combined into a
 * {@link List} in the order in which they are executed and the {@link List}
 * is returned to the caller.
 * <p>
 * Note: This class facilitates the definition of operations can be readily
 * composed through reuse of pre-defined operations. However, in all cases,
 * a similar effect can be obtained by extended {@link AbstractTask}
 * and coding the behavior directly in {@link #doTask()}.
 * <p>
 * Some possible use cases are:
 * <ul>
 * 
 * <li> Compose an atomic operation comprised of unisolated writes on one or
 * more indices. The locks required by the composed operation will be the
 * sum of the locks required by the individual operations, thereby ensuring
 * that the operation has all necessary locks when it begins. For example,
 * this could be used to atomically create and populate index index.</li>
 * 
 * <li>Compose an atomic operation comprised of unisolated reads on one or
 * more indices. Unisolated read operations do not require or obtain any
 * locks.</li>
 * 
 * <li>Compose an atomic operation comprised of isolated operations on one
 * or more indices.</li>
 * 
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SequenceTask extends AbstractTask {

    private final AbstractTask[] tasks;
    
    /**
     * @param journal
     * @param startTime
     * @param readOnly
     * @param resource
     */
    protected SequenceTask(ConcurrentJournal journal, long startTime,
            boolean readOnly, String[] resource, AbstractTask[] tasks) {

        super(journal, startTime, readOnly, resource);

        if(tasks==null) throw new IllegalArgumentException();
        
        this.tasks = tasks;
        
    }

    /**
     * Factory for a sequence composed from a set of tasks sharing the same
     * journal, isolation level, etc.
     * 
     * @param tasks
     *            The tasks.
     *            
     * @return The {@link SequenceTask}.
     */
    public static SequenceTask newSequence(AbstractTask[] tasks) {
        
        if(tasks==null) throw new NullPointerException();
        
        if(tasks.length==0) throw new IllegalArgumentException();
        
        if(tasks[0]==null) throw new NullPointerException();
        
        final ConcurrentJournal journal = tasks[0].journal;
        
        final long startTime = tasks[0].startTime; 
        
        final boolean readOnly = tasks[0].readOnly;
        
        final Set<String> resources = new HashSet<String>();
        
        resources.addAll(Arrays.asList(tasks[0].getResource()));
        
        for(int i=1; i<tasks.length; i++) {
            
            AbstractTask task = tasks[i];
            
            if(task==null) throw new NullPointerException();
            
            if(task.journal!=journal) throw new IllegalArgumentException();

            if(task.startTime!=startTime) throw new IllegalArgumentException();
            
            if(task.readOnly!=readOnly) throw new IllegalArgumentException();
            
            resources.addAll(Arrays.asList(task.getResource()));
            
        }
        
        return new SequenceTask(journal, startTime, readOnly, resources
                .toArray(new String[resources.size()]), tasks);
        
    }
    
    /**
     * Return an Object[] comprising the individual results.
     */
    protected Object doTask() throws Exception {

        Object[] ret = new Object[tasks.length];
        
        for(int i=0; i<tasks.length; i++) {
            
            AbstractTask task = tasks[i];
            
            ret[i] = task.doTask();
 
            if(Thread.interrupted()) {
                
                throw new InterruptedException();
                
            }
            
        }
        
        return ret;
        
    }
    
}
