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

	
	public FutureTaskMon(Callable<T> callable) {
		super(callable);
	}

	public FutureTaskMon(Runnable runnable, T result) {
		super(runnable, result);
	}
	
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (mayInterruptIfRunning && log.isDebugEnabled()) {
			try {
				throw new RuntimeException("cancel call trace");
			} catch (RuntimeException re) {
				log.debug("May interrupt running task", re);
			}
		}
		
		return super.cancel(mayInterruptIfRunning);
	}

}
