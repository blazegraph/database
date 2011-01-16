package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.NoSuchElementException;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.bop.engine.IRunningQuery;

/**
 * Iteration construct wraps an {@link IRunningQuery} with logic to (a) verify
 * that the {@link IRunningQuery} has not encountered an error; and (b) to cancel
 * the {@link IRunningQuery} when the iteration is {@link #close() closed}.
 * @author thompsonbry
 *
 * @param <E>
 * @param <X>
 */
public class RunningQueryCloseableIteration<E extends BindingSet, X extends QueryEvaluationException>
		implements CloseableIteration<E, X> {

	private final IRunningQuery runningQuery;
	private final CloseableIteration<E, X> src;
	private boolean checkedFuture = false;
	/**
	 * The next element is buffered so we can always return it if the
	 * {@link #runningQuery} was not aborted at the time that {@link #hasNext()}
	 * return <code>true</code>.
	 */
	private E current = null;

	public RunningQueryCloseableIteration(final IRunningQuery runningQuery,
			final CloseableIteration<E, X> src) {

		this.runningQuery = runningQuery;
		this.src = src;

	}

	public void close() throws X {
		runningQuery.cancel(true/* mayInterruptIfRunning */);
		src.close();
	}

	public boolean hasNext() throws X {

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
				throw (X) new QueryEvaluationException(e);
			} catch (Throwable e) {
				/*
				 * Exception thrown by the runningQuery.
				 */
				if (runningQuery.getCause() != null) {
					// abnormal termination.
					throw (X) new QueryEvaluationException(runningQuery.getCause());
				}
				// otherwise this is normal termination.
			}
			checkedFuture = true;
		}

		// the next element is now buffered.
		return true;

	}

	public E next() throws X {

		if (!hasNext())
			throw new NoSuchElementException();
		
		final E tmp = current;
		
		current = null;
		
		return tmp;
		
	}

	/**
	 * Operation is not supported.
	 */
	public void remove() throws X {

		// Not supported since we are buffering ahead.
		throw new UnsupportedOperationException();
		
	}

}
