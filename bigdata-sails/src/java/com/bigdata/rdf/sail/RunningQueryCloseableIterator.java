package com.bigdata.rdf.sail;

import java.util.NoSuchElementException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Iteration construct wraps an {@link IRunningQuery} with logic to (a) verify
 * that the {@link IRunningQuery} has not encountered an error; and (b) to cancel
 * the {@link IRunningQuery} when the iteration is {@link #close() closed}.
 */
public class RunningQueryCloseableIterator<E extends IBindingSet> 
		implements ICloseableIterator<E> {

	private final IRunningQuery runningQuery;
	private final ICloseableIterator<E> src;
	private boolean checkedFuture = false;
	/**
	 * The next element is buffered so we can always return it if the
	 * {@link #runningQuery} was not aborted at the time that {@link #hasNext()}
	 * return <code>true</code>.
	 */
	private E current = null;

	public RunningQueryCloseableIterator(final IRunningQuery runningQuery,
			final ICloseableIterator<E> src) {

		this.runningQuery = runningQuery;
		this.src = src;

	}

	public void close() {
		runningQuery.cancel(true/* mayInterruptIfRunning */);
		src.close();
	}

	public boolean hasNext() {

		if (current != null) {
			// Already buffered.
			return true;
		}

		if (!src.hasNext()) {
			// Source is exhausted.
			return false;
		}

		// buffer the next element.
		current = src.next();

		// test for abnormal completion of the runningQuery. 
		if (!checkedFuture && runningQuery.isDone()) {
			try {
				runningQuery.get();
			} catch (InterruptedException e) {
				/*
				 * Interrupted while waiting on the Future (should not happen
				 * since the Future is already done).
				 */
				throw new RuntimeException(e);
			} catch (Throwable e) {
				/*
				 * Exception thrown by the runningQuery.
				 */
				if (runningQuery.getCause() != null) {
					// abnormal termination - wrap and rethrow.
					throw new RuntimeException(e);
				}
				// otherwise this is normal termination.
			}
			checkedFuture = true;
		}

		// the next element is now buffered.
		return true;

	}

	public E next() {

		if (!hasNext())
			throw new NoSuchElementException();
		
		final E tmp = current;
		
		current = null;
		
		return tmp;
		
	}

	/**
	 * Operation is not supported.
	 */
	public void remove() {

		// Not supported since we are buffering ahead.
		throw new UnsupportedOperationException();
		
	}

}
