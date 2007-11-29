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
package com.bigdata.service.mapred;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

/**
 * A map reduce job.
 * 
 * @todo Support description of a map/reduce using XML (parse and serialize).
 *       This will give us a handy means to describe jobs that are then executed
 *       on the command line using {@link Master}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MapReduceJob implements IMapReduceJob {

    /** The job identifier. */
    final public UUID uuid;

    /**
     * The #of map tasks to run concurrently (the #of map tasks is dynamically
     * determined based on the #of inputs available).
     */
    final public int m;

    /**
     * The #of reduce partitions (also the #of reduce tasks to run
     * concurrently).
     */
    final public int n;

    /**
     * The maximum #of map tasks to execute (0L for no limit).
     */
    private long maxMapTasks = 0L;
    
    /**
     * When non-zero, the timeout for map tasks (default is one minute).
     */
    private long mapTaskTimeout = 1 * 60 * 1000;

    /**
     * When non-zero, the timeout for reduce tasks (default is one minute).
     */
    private long reduceTaskTimeout = 1 * 60 * 1000;
    
    /**
     * The maximum #of times a map task may be retried (zero disables retry).
     */
    private int maxMapTaskRetry = 0;

    /**
     * The maximum #of times a map task may be retried (zero disables retry).
     */
    private int maxReduceTaskRetry = 0;
    
    /** The object responsible for enumerating the inputs to the map task. */
    private final IMapSource mapSource;

    private final Class<? extends IMapTask> mapTaskClass;

    private final Constructor<? extends IMapTask> mapTaskCtor;
    
    private final Class<? extends IReduceTask> reduceTaskClass;

    private final Constructor<? extends IReduceTask> reduceTaskCtor;
    
    private final IHashFunction hashFunction;

    public MapReduceJob(int m, int n,
            IMapSource mapSource,
            Class<? extends IMapTask> mapTaskClass,
            Class<? extends IReduceTask> reduceTaskClass,
            IHashFunction hashFunction
            ) {

        if (m <= 0)
            throw new IllegalArgumentException();

        if (n <= 0)
            throw new IllegalArgumentException();

        if (mapSource == null)
            throw new IllegalArgumentException();

        if (mapTaskClass == null)
            throw new IllegalArgumentException();

        if (reduceTaskClass == null)
            throw new IllegalArgumentException();

        if (hashFunction == null)
            throw new IllegalArgumentException();

        // assign a job UUID.
        this.uuid = UUID.randomUUID();

        this.m = m;

        this.n = n;

        this.mapSource = mapSource;

        this.mapTaskClass = mapTaskClass;

        try {
            this.mapTaskCtor = mapTaskClass.getConstructor(new Class[] {
                    UUID.class, Object.class, Integer.class, IHashFunction.class });
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } 
        
        this.reduceTaskClass = reduceTaskClass;

        try {
            this.reduceTaskCtor = reduceTaskClass
                    .getConstructor(new Class[] { UUID.class, UUID.class });
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } 

        this.hashFunction = hashFunction;

    }

    public UUID getUUID() {

        return uuid;

    }

    public int getMapParallelism() {

        return m;

    }

    public int getReducePartitionCount() {

        return n;

    }

    public long getMaxMapTasks() {
        
        return maxMapTasks;
        
    }
    
    public long setMaxMapTasks(long newValue) {
       
        long t = this.maxMapTasks;
        
        this.maxMapTasks = newValue;
        
        return t;
        
    }

    public long getMapTaskTimeout() {
        
        return mapTaskTimeout;
        
    }
    
    public long setMapTaskTimeout(long newValue) {
        
        long t = this.mapTaskTimeout;
        
        this.mapTaskTimeout = newValue;
        
        return t;
        
    }

    public long getReduceTaskTimeout() {
        
        return reduceTaskTimeout;
        
    }
    
    public long setReduceTaskTimeout(long newValue) {
        
        long t = this.reduceTaskTimeout;
        
        this.reduceTaskTimeout = newValue;
        
        return t;
        
    }

    public int getMaxMapTaskRetry() {
        
        return maxMapTaskRetry;
        
    }
    
    public int getMaxReduceTaskRetry() {
        
        return maxReduceTaskRetry;
        
    }
    
    public int setMaxMapTaskRetry(int newValue) {
        
        int t = maxMapTaskRetry;
        
        this.maxMapTaskRetry = newValue;
        
        return t;
        
    }
        
    public int setMaxReduceTaskRetry(int newValue) {
        
        int t = maxReduceTaskRetry;
        
        this.maxReduceTaskRetry = newValue;
        
        return t;
        
    }
        
    public IMapSource getMapSource() {

        return mapSource;

    }

    public IHashFunction getHashFunction() {

        return hashFunction;

    }

    public IMapTask getMapTask(Object source) {
        
        return getMapTask( UUID.randomUUID(), source );
       
    }
    
    public IMapTask getMapTask(UUID task, Object source ) {

        try {

            return mapTaskCtor.newInstance(new Object[] {
                    task,
                    source,
                    Integer.valueOf(getReducePartitionCount()),
                    getHashFunction() }
            );
            
        } catch (InstantiationException e) {
            
            throw new RuntimeException(e);
            
        } catch (IllegalAccessException e) {
            
            throw new RuntimeException(e);
            
        } catch (IllegalArgumentException e) {

            throw new RuntimeException(e);
            
        } catch (InvocationTargetException e) {
            
            throw new RuntimeException(e);

        }

    }

    public IReduceTask getReduceTask(UUID task, UUID dataService) {

        try {

            return reduceTaskCtor.newInstance(new Object[] {
                    task,
                    dataService}
            );
            
        } catch (InstantiationException e) {
            
            throw new RuntimeException(e);
            
        } catch (IllegalAccessException e) {
            
            throw new RuntimeException(e);
            
        } catch (IllegalArgumentException e) {

            throw new RuntimeException(e);
            
        } catch (InvocationTargetException e) {
            
            throw new RuntimeException(e);

        }

    }

}
