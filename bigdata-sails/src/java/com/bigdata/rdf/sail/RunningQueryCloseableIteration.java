package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.concurrent.ExecutionException;

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
		return src.hasNext();
	}

	public E next() throws X {
		if (!checkedFuture && runningQuery.isDone()) {
			try {
				runningQuery.get();
			} catch (InterruptedException e) {
				throw (X) new QueryEvaluationException(e);
			} catch (ExecutionException e) {
				throw (X) new QueryEvaluationException(e);
			}
			checkedFuture = true;
		}
		return src.next();
	}

	public void remove() throws X {
		src.remove();
	}

}
