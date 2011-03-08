/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package com.bigdata.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;


/**
 * This is a flyweight utility class to be used as a direct replacement for
 * FutureTask in code where we may need to be able to discover the root cancel
 * request causing an interrupt.
 * 
 * @author Martyn Cutcher
 */
public class FutureTaskMon<T> extends FutureTask<T> {

	static private final transient Logger log = Logger
	.getLogger(FutureTaskMon.class);

	private volatile boolean didStart = false;
	
	public FutureTaskMon(Callable<T> callable) {
		super(callable);
	}

	public FutureTaskMon(Runnable runnable, T result) {
		super(runnable, result);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Hooked to notice when the task has been started.
	 */
	@Override
	public void run() {
		didStart = true;
		super.run();
	}
	
	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to conditionally log @ DEBUG if the caller caused the task to
	 * be interrupted. This can be used to search for sources of interrupts.
	 */
	@Override
	public boolean cancel(final boolean mayInterruptIfRunning) {

		final boolean didStart = this.didStart;
		
		final boolean ret = super.cancel(mayInterruptIfRunning);

		if (didStart && mayInterruptIfRunning && ret && log.isDebugEnabled()) {
			try {
				throw new RuntimeException("cancel call trace");
			} catch (RuntimeException re) {
				log.debug("May interrupt running task", re);
			}
		}

		return ret;

	}

}
