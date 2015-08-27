package com.bigdata.rdf.changesets;

import java.beans.Statement;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Value;

import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

import cutthecrap.utils.striterators.ICloseable;

/**
 * {@link IChangeLog} implementation reports inferences as RDF {@link Statement}
 * s. You install this change listener before writing on the sail connection.
 * After the commit, you use {@link #addedIterator()} (
 * {@link #removedIterator()}) to visit the inferences that were added to
 * (removed from) the KB by the transaction. If the transaction is aborted,
 * simply discard the {@link InferenceChangeLogReporter} object. Always use a
 * new instance of this object for each transaction.
 * 
 * TODO The present implementation uses a LinkedHashMap to store the ISPOs for
 * the inferences. This should be scalable up to millions of inferences, and
 * maybe the low 10s of millions. If very large sets of inferences will be
 * drawn, then we could substitute an {@link HTree} index for the
 * {@link LinkedHashSet}. The existing {@link NativeDistinctFilter} class
 * automatically converts from a JVM hash collection to an {@link HTree} and and
 * could be used trivially as a replacement for the {@link LinkedHashSet} in
 * this class. In this case, the native memory associated with the HTree needs
 * to be released, e.g., through an {@link ICloseable} protocol on the change
 * listener.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class InferenceChangeLogReporter implements IChangeLog {

    /**
     * The KB.
     */
    private final AbstractTripleStore kb;

    /** New inferences. */
    private final Set<ISPO> added = new LinkedHashSet<ISPO>();
    
    /** Removed inferences. */
    private final Set<ISPO> removed = new LinkedHashSet<ISPO>();

    /**
     * 
     * @param kb
     *            The KB (used to resolve {@link IV}s to {@link Value}s).
     */
    public InferenceChangeLogReporter(final AbstractTripleStore kb) {
        this.kb = kb;
    }

    /**
     * Clear the internal state. This may be used to reset the listener if
     * multiple commits are used for the same connection.
     * <p>
     * Note: It is faster to get a new {@link InferenceChangeLogReporter} than
     * to clear the internal maps, but you can not replace an {@link IChangeLog}
     * listener once established on a connection.
     */
    public void clear() {
        added.clear();
        removed.clear();
    }

    @Override
    public void changeEvent(IChangeRecord record) {
        final ISPO spo = record.getStatement();
        if (!spo.isInferred())
            return;
        switch (record.getChangeAction()) {
        case INSERTED:
            added.add(spo);
            break;
        case REMOVED:
            removed.add(spo);
            break;
        case UPDATED:
            // ignore. statement already exists.
            break;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public void transactionBegin() {
    }

    @Override
    public void transactionPrepare() {
    }

    @Override
    public void transactionCommited(long commitTime) {

    }

    @Override
    public void transactionAborted() {

    }

    @Override
    public void close() {
    }

    /**
     * Return iterator visiting the inferences that were added to the KB.
     */
    public BigdataStatementIterator addedIterator() {

        // Wrap as chunked iterator.
        final IChunkedOrderedIterator<ISPO> src = new ChunkedWrappedIterator<ISPO>(
                added.iterator());

        // Asynchronous conversion of ISPOs to Statements.
        return new BigdataStatementIteratorImpl(kb, src).start(kb
                .getExecutorService());

    }

    /**
     * Return iterator visiting the inferences that were removed from the KB.
     */
    public BigdataStatementIterator removedIterator() {

        // Wrap as chunked iterator.
        final IChunkedOrderedIterator<ISPO> src = new ChunkedWrappedIterator<ISPO>(
                removed.iterator());

        // Asynchronous conversion of ISPOs to Statements.
        return new BigdataStatementIteratorImpl(kb, src).start(kb
                .getExecutorService());

    }

}
