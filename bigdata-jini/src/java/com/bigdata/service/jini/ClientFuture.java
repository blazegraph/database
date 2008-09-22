package com.bigdata.service.jini;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link Serializable} class wraps a {@link RemoteFuture} delegating
 * methods through to the {@link Future} on the remote service while
 * masquerading {@link IOException}s so that we can implement the
 * {@link Future} API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class ClientFuture<T> implements Future<T>, Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -910518634373204705L;
    
    private final RemoteFuture<T> future;

    public ClientFuture(RemoteFuture<T> future) {

        if (future == null)
            throw new IllegalArgumentException();

        this.future = future;

    }

    public boolean cancel(boolean mayInterruptIfRunning) {

        try {
            return future.cancel(mayInterruptIfRunning);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public T get() throws InterruptedException, ExecutionException {

        try {
            return future.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {

        try {
            return future.get(timeout, unit);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isCancelled() {

        try {
            return future.isCancelled();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isDone() {

        try {
            return future.isDone();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    
}