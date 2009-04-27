package com.bigdata.rdf.rio;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.openrdf.model.Statement;

import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * Factory interface for asynchronous writers on an {@link ITripleStore}.
 */
public interface IAsynchronousWriteBufferFactory<S extends Statement>
        extends IStatementBufferFactory<S> {

    /**
     * Return a new {@link IStatementBuffer} which may be used to bulk load
     * RDF data. The writes will proceed asynchronously using buffers shared
     * by all {@link IStatementBuffer}s returned by this factory for a
     * given instance of this class. Each {@link IStatementBuffer} MAY be
     * recycled using {@link IBuffer#reset()} or simply discarded after its
     * use.
     */
    public IStatementBuffer<S> newStatementBuffer();

    /**
     * Return <code>true</code> if the {@link Future} for any of the
     * asynchronous write buffers is done.
     * <p>
     * Note: This method should be invoked periodically to verify that no
     * errors have been encountered by the asynchronous write buffers. If
     * this method returns <code>true</code>, invoke {@link #awaitAll()},
     * which will detect any error(s), cancel the other {@link Future}s,
     * and throw an error back to you.
     */
    public boolean isDone();
    
    /**
     * Close the buffers. Once closed, the buffers will not accept further
     * input and the consumers will eventually drain the buffers and report
     * that they are exhausted.
     */
    public void close();
    
    /**
     * Cancel all {@link Future}s. The buffers will be automatically closed
     * when their {@link Future}s are cancelled.
     * 
     * @param mayInterruptIfRunning
     */
    public void cancelAll(final boolean mayInterruptIfRunning);

    /**
     * Close buffers and then await their {@link Future}s. Once closed, the
     * buffers will not accept further input and the consumers will
     * eventually drain the buffers and report that they are exhausted. The
     * {@link Future}s will become available once the iterators are
     * exhausted.
     * 
     * @throws ExecutionException
     *             if any {@link Future} fails.
     * 
     * @throws InterruptedException
     *             if interrupted while awaiting any of the {@link Future}s.
     */
    public void awaitAll() throws InterruptedException, ExecutionException;
    
}