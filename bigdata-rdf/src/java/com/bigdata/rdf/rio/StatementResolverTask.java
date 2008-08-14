package com.bigdata.rdf.rio;

import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AbstractStatementBuffer.StatementResolvingBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Resolves {@link Statement}s or {@link SPO}s into
 * {@link BigdataStatement}s using the lexicon for the specified database
 * (this can be used to perform bulk copy or existence testing).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class StatementResolverTask<E> implements Callable<Long> {

    protected static final Logger log = Logger.getLogger(StatementResolverTask.class);
    
    /**
     * The source iterator.
     */
    private final Iterator<? extends Statement> sourceIterator;

    /**
     * The output iterator.
     */
    private final Iterator<E> outputIterator;

    /**
     * The buffer used to bulk converts {@link Statement}s to
     * {@link BigdataStatement}s, resolving term (and optionally statement
     * identifiers) against the target database.
     */
    protected final AbstractStatementBuffer<Statement, BigdataStatement> sb;

    /**
     * @param source
     *            An iterator visiting {@link Statement}s (will be closed
     *            if implements a closable interface).
     * @param readOnly
     *            When <code>true</code>, terms will be resolved against
     *            the <i>target</i> but not written onto the <i>target</i>.
     * @param capacity
     *            The capacity of the buffer used to bulk convert
     *            statements.
     * @param target
     *            The target database.
     */
    public StatementResolverTask(Iterator<? extends Statement> source,
            boolean readOnly, int capacity, AbstractTripleStore target) {

        this.sourceIterator = source;

        this.sb = new StatementResolvingBuffer<Statement, BigdataStatement>(
                target, readOnly, capacity);

        this.outputIterator = getOutputIterator();

    }

    /**
     * Return the singleton that will be used as the output iterator. The
     * application will obtain this iterator from {@link #iterator()}. The
     * iterator will be closed when the task completes if it implements a
     * "closeable" interface.
     */
    abstract protected Iterator<E> getOutputIterator();

    /**
     * When the task runs it will consume {@link Statement}s from the
     * {@link #sourceIterator} iterator,
     * {@link AbstractStatementBuffer#add(Statement) adding} to an
     * {@link AbstractStatementBuffer}. Once the source iterator is exhausted,
     * the buffer is {@link AbstractStatementBuffer#flush()}ed and this task
     * is done.
     */
    public Long call() throws Exception {

        try {

            while (sourceIterator.hasNext()) {

                sb.add(sourceIterator.next());

            }

            final long n = sb.flush();

            return Long.valueOf(n);

        } finally {

            close(sourceIterator);

            close(outputIterator);

        }

    }

    /**
     * Close the iterator if it implements any of the "closeable"
     * interfaces.
     * 
     * @param itr
     */
    protected void close(Iterator itr) {

        try {

            if (itr instanceof ICloseableIterator) {

                ((ICloseableIterator) itr).close();

            } else if (itr instanceof CloseableIteration) {

                ((CloseableIteration) itr).close();

            } else if (itr instanceof Closeable) {

                ((Closeable) itr).close();

            }

        } catch (Throwable t) {

            log.error(t, t);

        }

    }

    /**
     * The output of the task may be read from this iterator.
     */
    public Iterator<E> iterator() {

        return outputIterator;

    }

}
