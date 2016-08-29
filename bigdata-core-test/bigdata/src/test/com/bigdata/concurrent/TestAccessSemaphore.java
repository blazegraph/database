/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.bigdata.concurrent.AccessSemaphore.Access;
import com.bigdata.util.DaemonThreadFactory;

public class TestAccessSemaphore {

	@Test
	public void testSimpleAccess() throws InterruptedException {
        final ExecutorService execService = Executors.newFixedThreadPool(10,
                DaemonThreadFactory.defaultThreadFactory());
        
        final AccessSemaphore accessSemaphore = new AccessSemaphore(10/*max shared*/);
        
        final Access shared = accessSemaphore.acquireShared();
        
        final Future<?> r1 = execService.submit(new Runnable() {

			@Override
			public void run() {
				try {
					final Access access = accessSemaphore.acquireExclusive();
					access.release();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
        	
        });
        
        try {
			r1.get(2, TimeUnit.SECONDS);
			fail("Should not attain exclusive access");
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			// expected
		}
        
        // now release shared access
        shared.release();
        
        try {
 			r1.get(2, TimeUnit.SECONDS);
 		} catch (ExecutionException e) {
 			e.printStackTrace();
 		} catch (TimeoutException e) {
			fail("Should attain exclusive access");
 		}
        
	}


	@Test
	public void testStressAccess() throws InterruptedException,
			ExecutionException {
		// Run 20 threads with 10 shared
		final ExecutorService execService = Executors.newFixedThreadPool(20,
				DaemonThreadFactory.defaultThreadFactory());

		try {
			final AccessSemaphore accessSemaphore = new AccessSemaphore(10/* max shared */);

			final Random r = new Random();

			final int NTASKS = 100;
			final int NACQUIRES = 10000;

			final Future<?>[] futures = new Future<?>[NTASKS];

			for (int t = 0; t < NTASKS; t++) {
				final Future<?> r1 = execService.submit(new Runnable() {

					@Override
					public void run() {
						try {
							for (int i = 0; i < NACQUIRES; i++) {
								final int v = r.nextInt();
								final Access access = (v % 20) == 0 ? 
										accessSemaphore.acquireExclusive() : 
										accessSemaphore.acquireShared();
										
								access.release();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

				});
				
				futures[t] = r1;
			}

			for (int i = 0; i < NTASKS; i++) {
				futures[i].get(30, TimeUnit.SECONDS);
			}
		} catch (TimeoutException e) {
			fail("Test may be deadlocked");
		} finally {
			execService.shutdown();
		}
	}

}
