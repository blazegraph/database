package com.bigdata.rdf.load;

/**
 * A factory for {@link Runnable} tasks.
 */
public interface ITaskFactory<T extends Runnable> {
    
    public T newTask(String file) throws Exception;
    
}