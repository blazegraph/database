package com.bigdata.util.concurrent;

import java.util.concurrent.Future;

/**
 * Interface extends {@link Future} and provides an interface for managing the
 * termination of a process from within that process.
 * 
 * @param <V>
 *            The generic type of the computation to which the {@link Future}
 *            evaluates.
 */
public interface IHaltable<V> extends Future<V> {

    /**
     * Halt (normal termination).
     */
    void halt(V v);

	/**
	 * Halt (exception thrown). <strong>The caller is responsible for throwing
	 * their given <i>cause</i> out of their own context.</strong> As a
	 * convenience, this method returns the given <i>cause</>.
	 * 
	 * @param cause
	 *            The cause (required).
	 * 
	 * @return The argument.
	 */
	<T extends Throwable> T halt(T cause);

	/**
	 * Return the first {@link Throwable} which caused this process to halt, but
	 * only for abnormal termination.
	 * <p>
	 * {@link IHaltable} considers exceptions triggered by an interrupt to be
	 * normal termination of the process and will return <code>null</code> for
	 * such exceptions.
	 * 
	 * @return The first {@link Throwable} which caused this process to halt and
	 *         <code>null</code> if the process has not halted or if it halted
	 *         through normal termination.
	 */
	Throwable getCause();

}
