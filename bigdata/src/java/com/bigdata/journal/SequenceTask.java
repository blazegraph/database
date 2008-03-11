/*

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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;

/**
 * A task comprised of a sequence of operations. All operations MUST run on the
 * same journal and task service (read service, write service, or transaction
 * service). The individual results are combined into a {@link List} in the
 * order in which they are executed and the {@link List} is returned to the
 * caller.
 * <p>
 * Note: This class facilitates the definition of operations can be readily
 * composed through reuse of pre-defined operations. However, in all cases, a
 * similar effect can be obtained by extended {@link AbstractTask} and coding
 * the behavior directly in {@link #doTask()}.
 * <p>
 * Some possible use cases are:
 * <ul>
 * 
 * <li> Compose an atomic operation comprised of unisolated writes on one or
 * more indices. The locks required by the composed operation will be the sum of
 * the locks required by the individual operations, thereby ensuring that the
 * operation has all necessary locks when it begins. For example, this could be
 * used to atomically create and populate index index.</li>
 * 
 * <li>Compose an atomic operation comprised of unisolated reads on one or more
 * indices. Unisolated read operations do not require or obtain any locks.</li>
 * 
 * <li>Compose an atomic operation comprised of isolated operations on one or
 * more indices.</li>
 * 
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated The problem with this class is that it creates an array of
 *             {@link AbstractTask}s rather than a being a single
 *             {@link AbstractTask} that runs a bunch of {@link ITask}s. Fixing
 *             this will require re-working all of the {@link ITask}s to extend
 *             some innocent abstract {@link BaseTask} rather than
 *             {@link AbstractTask} itself. Another alternative is to modify the
 *             {@link AbstractTask} constructor to accept an array of
 *             {@link ITask} or {@link Callable} targets that it will run itself.
 */
public class SequenceTask extends AbstractTask {

    private final AbstractTask[] tasks;
    
    /**
     * The result of each task is appended to this vector.
     */
    final protected Vector<Object> results;

    protected SequenceTask(IConcurrencyManager concurrencyManager,
            long startTime, String[] resource, AbstractTask[] tasks) {

        super(concurrencyManager, startTime, resource);

        if (tasks == null)
            throw new IllegalArgumentException();
        
        this.tasks = tasks;
     
        results = new Vector<Object>(tasks.length);
        
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

        if (tasks == null)
            throw new NullPointerException();

        if (tasks.length == 0)
            throw new IllegalArgumentException();

        if (tasks[0] == null)
            throw new NullPointerException();

        final IConcurrencyManager concurrencyManager = tasks[0].concurrencyManager;

        final IResourceManager resourceManager = tasks[0].getResourceManager();
        
        final long startTime = tasks[0].timestamp; 
        
        final boolean readOnly = tasks[0].readOnly;
        
        final Set<String> resources = new HashSet<String>();
        
        resources.addAll(Arrays.asList(tasks[0].getResource()));
        
        for(int i=1; i<tasks.length; i++) {
            
            AbstractTask task = tasks[i];
            
            if (task == null)
                throw new NullPointerException();

            if (task.concurrencyManager != concurrencyManager)
                throw new IllegalArgumentException();

            if (task.getResourceManager() != resourceManager)
                throw new IllegalArgumentException();

            if (task.timestamp != startTime)
                throw new IllegalArgumentException();

            if (task.readOnly != readOnly)
                throw new IllegalArgumentException();
            
            resources.addAll(Arrays.asList(task.getResource()));
            
        }
        
        return new SequenceTask(concurrencyManager, startTime, resources
                .toArray(new String[resources.size()]), tasks);
        
    }
    
    /**
     * Return an Object[] comprising the individual results.
     */
    protected Object doTask() throws Exception {

        for(int i=0; i<tasks.length; i++) {
            
            AbstractTask task = tasks[i];
            
            results.add( task.doTask() );
 
            if(Thread.interrupted()) {
                
                throw new InterruptedException();
                
            }
            
        }
        
        return results.toArray();
        
    }
    
}
