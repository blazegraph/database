package com.bigdata.service.jini;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.export.Exporter;

/**
 * A helper object that provides the API of {@link Future} but whose methods
 * throw {@link IOException} and are therefore compatible with
 * {@link Remote} and {@link Exporter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
class RemoteFutureImpl<T> implements RemoteFuture {

    private final Future<T> future;

    public RemoteFutureImpl(Future<T> future) {

        if (future == null)
            throw new IllegalArgumentException();

        this.future = future;

    }

    public boolean cancel(boolean mayInterruptIfRunning) throws IOException {
        
        return future.cancel(mayInterruptIfRunning);
        
    }

    public T get() throws InterruptedException, ExecutionException,
            IOException {
        
        return future.get();
        
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException, IOException {
        
        return future.get(timeout, unit);
        
    }

    public boolean isCancelled() throws IOException {
        
        return future.isCancelled();
        
    }

    public boolean isDone() throws IOException {
        
        return future.isDone();
        
    }

}