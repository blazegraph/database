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
/*
 * Created on Jan 3, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.util.InnerCause;
import com.bigdata.concurrent.AccessSemaphore.AccessSemaphoreNotReentrantException;

/**
 * Bootstrap test case for bringing up the {@link BigdataSail}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBootstrapBigdataSail extends TestCase2 {

    /**
     * 
     */
    public TestBootstrapBigdataSail() {
    }

    /**
     * @param arg0
     */
    public TestBootstrapBigdataSail(final String arg0) {
        super(arg0);
    }

    /**
     * Test create and shutdown of the default store.
     * 
     * @throws SailException
     * @throws IOException 
     */
    public void test_ctor_1() throws SailException, IOException {

        final File file = new File(BigdataSail.Options.DEFAULT_FILE);

        /*
         * If the default file exists, then delete it before creating the SAIL.
         */
        if (file.exists()) {

            if (!file.delete()) {

                throw new IOException("Unable to remove default file:" + file);

            }

        }
        
        final BigdataSail sail = new BigdataSail();
        
        try {

            if (!file.exists())
                fail("Expected file does not exist: " + file);
            
            sail.initialize();

            sail.shutDown();
            
		} finally {

			sail.getIndexManager().destroy();

        }

    }

    /**
     * Test create and shutdown of a named store.
     * 
     * @throws SailException
     */
    public void test_ctor_2() throws SailException {

        final File file = new File(getName() + Options.JNL);

        if (file.exists()) {

            if (!file.delete()) {

                fail("Could not delete file before test: " + file);

            }

        }
        
        final Properties properties = new Properties();

        properties.setProperty(Options.FILE, file.toString());

        final BigdataSail sail = new BigdataSail(properties);

        try {

            sail.initialize();
            
            sail.shutDown();

		} finally {

            if (!file.exists()) {

                fail("Could not locate store: " + file);

                if (!file.delete()) {

                    fail("Could not delete file after test: " + file);

                }

            }

        }

    }

    /**
     * Test creates a database, obtains a writable connection on the database,
     * and then closes the connection and shutdown the database.
     * 
     * @throws SailException
     */
    public void test_getConnection() throws SailException {

        final Properties properties = new Properties();

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();

			final SailConnection conn = sail.getConnection();

			conn.close();

			sail.shutDown();

		} finally {

			sail.getIndexManager().destroy();

		}

    }

	/**
	 * Unit test verifies that a thread may not obtain more than one instance of
	 * the unisolated connection at a time from the {@link BigdataSail} via a
	 * reentrant invocation. The reentrant request should immediately throw an
	 * exception to prevent an unbreakable deadlock (a thread can not wait on
	 * itself).
	 * 
	 * @throws SailException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void test_getConnectionAllowedExactlyOnce1_oneThread() throws SailException,
			InterruptedException, ExecutionException {

		final Properties properties = new Properties();

		properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		ExecutorService service = null;
		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();
			service = Executors.newSingleThreadExecutor();

			Future<Void> f = null;
			
			try {

				final Callable<Void> task = new Callable<Void>() {

					public Void call() throws Exception {

						SailConnection conn1 = null;
						SailConnection conn2 = null;

						try {

							log.info("Requesting 1st unisolated connection.");

							conn1 = sail.getUnisolatedConnection();

							log.info("Requesting 2nd unisolated connection.");

							try {
								conn2 = sail.getUnisolatedConnection();
								fail("Not expecting a 2nd unisolated connection");
							} catch (IllegalStateException ex) {
								if (log.isInfoEnabled())
									log.info("Ignoring expected exception: "
											+ ex);
							}

							return (Void) null;

						} finally {

							if (conn1 != null)
								conn1.close();

							if (conn2 != null)
								conn2.close();

						}
					}

				};

				// run task.
				f = service.submit(task);

				// should succeed quietly.
				f.get();

			} finally {

				if (f != null) {
					// Cancel task.
					f.cancel(true/* mayInterruptIfRunning */);
				}
				
				sail.shutDown();

			}

		} finally {

			if (service != null) {
				service.shutdownNow();
			}
			
			sail.getIndexManager().destroy();

		}

	}

	/**
	 * Unit test verifies that a thread may not obtain more than one instance of
	 * the unisolated connection at a time from the {@link BigdataSail} using
	 * two threads. The second thread should block until the first thread
	 * releases the connection, at which point the second thread should obtain
	 * the connection.
	 * 
	 * @throws SailException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void test_getConnectionAllowedExactlyOnce1_twoThreads() throws SailException,
			InterruptedException, ExecutionException, TimeoutException {

		final Properties properties = new Properties();

		properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		ExecutorService service = null;
		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();
			service = Executors.newFixedThreadPool(2/*nThreads*/);

			Future<Void> f = null;
			Future<Void> f2 = null;

			final ReentrantLock lock = new ReentrantLock();
			final AtomicBoolean haveFirst = new AtomicBoolean(false);
			final AtomicBoolean releaseFirst = new AtomicBoolean(false);
//			final AtomicBoolean haveSecond = new AtomicBoolean(false);
			final Condition haveFirstConnection = lock.newCondition();
			final Condition releaseFirstConnection = lock.newCondition();
//			final Condition haveSecondConnection = lock.newCondition();
			try {

				final Callable<Void> task1 = new Callable<Void>() {
					public Void call() throws Exception {
						SailConnection conn1 = null;
						try {
							log.info("Requesting 1st unisolated connection.");
							lock.lock();
							try {
								conn1 = sail.getUnisolatedConnection();
								haveFirst.set(true);
								haveFirstConnection.signal();
								// Wait on condition to release connection.
								while(!releaseFirst.get())
									releaseFirstConnection.await();
								log.info("Releasing 1st unisolated connection.");
							} finally {
								lock.unlock();
							}
							return (Void) null;
						} finally {
							if (conn1 != null)
								conn1.close();
						}
					}
				};

				final Callable<Void> task2 = new Callable<Void>() {
					public Void call() throws Exception {
						SailConnection conn2 = null;
						try {
							log.info("Requesting 2nd unisolated connection.");
							conn2 = sail.getUnisolatedConnection();
							log.info("Have 2nd unisolated connection");
							return (Void) null;
						} finally {
							if (conn2 != null)
								conn2.close();
						}
					}
				};

				/*
				 * Run task. It should obtain the unisolated connection and THEN
				 * block wait on our signal.
				 */
				f = service.submit(task1);
				lock.lock();
				try {
					while(!haveFirst.get()) {
						haveFirstConnection.await();
					}
					// start task2
					f2 = service.submit(task2);
					log.info("Will instruct to release 1st connection.");
					releaseFirst.set(true);
					releaseFirstConnection.signal();
				} finally {
					lock.unlock();
				}

				// wait on both tasks and verify outcomes. they should terminate quickly.
				f.get(1000,TimeUnit.MILLISECONDS);
				f2.get(1000,TimeUnit.MILLISECONDS);

			} finally {

				if (f != null) {
					// Cancel task.
					f.cancel(true/* mayInterruptIfRunning */);
				}

				if (f2 != null) {
					// Cancel task.
					f2.cancel(true/* mayInterruptIfRunning */);
				}

				sail.shutDown();

			}

		} finally {

			if (service != null) {
				service.shutdownNow();
			}

			sail.getIndexManager().destroy();

		}

	}

	/**
	 * Unit test verifies exactly one unisolated connection for two different
	 * {@link BigdataSail} instances for the same {@link AbstractTripleStore} on
	 * the same {@link Journal}.
	 * 
	 * @throws SailException
	 * @throws InterruptedException
	 */
	public void test_getConnectionAllowedExactlyOnce2() throws SailException,
			InterruptedException, ExecutionException {

		final Properties properties = new Properties();

		properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		ExecutorService service = null;
		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();
			service = Executors.newSingleThreadExecutor();

			// wrap a 2nd sail around the same namespace.
			final BigdataSail sail2 = new BigdataSail(sail.getNamespace(), sail.getIndexManager());
			sail2.initialize();

			Future<Void> f = null;

			try {

				final Callable<Void> task = new Callable<Void>() {

				    @Override
					public Void call() throws Exception {

						SailConnection conn1 = null;
						SailConnection conn2 = null;

						try {

							log.info("Requesting 1st unisolated connection.");

							conn1 = sail.getUnisolatedConnection();

							log.info("Requesting 2nd unisolated connection.");

							conn2 = sail2.getUnisolatedConnection();

							fail("Not expecting a 2nd unisolated connection");

							return (Void) null;

						} finally {

							if (conn1 != null)
								conn1.close();

							if (conn2 != null)
								conn2.close();

						}
					}

				};

				// run task. it should block when attempting to get the 2nd
				// connection.
				f = service.submit(task);

//				// wait up to a timeout to verify that the task blocked rather
//				// than acquiring the 2nd connection.
//				f.get(250, TimeUnit.MILLISECONDS);
				f.get();

			} catch (ExecutionException e) {

                if (InnerCause.isInnerCause(e, AccessSemaphoreNotReentrantException.class)) {
                    /*
                     * This is the expected outcome.
                     */
                    log.info(e);
                } else {
                    throw e;
			    }

			} finally {

				if (f != null) {
					// Cancel task.
					f.cancel(true/* mayInterruptIfRunning */);
				}

				if (sail2 != null)
					sail2.shutDown();

				sail.shutDown();

			}

		} finally {

			if (service != null) {
				service.shutdownNow();
			}

			sail.getIndexManager().destroy();

		}

	}

	/**
	 * Unit test verifying that exactly one unisolated connection is allowed at
	 * a time for two sails wrapping different {@link AbstractTripleStore}
	 * instances. (This guarantee is needed to preserve ACID semantics for the
	 * unisolated connection when there is more than one
	 * {@link AbstractTripleStore} on the same {@link Journal}. However,
	 * scale-out should not enforce this constraint since it is shard-wise ACID
	 * for unisolated operations.)
	 * 
	 * @throws SailException
	 * @throws InterruptedException
	 */
	public void test_getConnectionAllowedExactlyOnce3() throws SailException,
			InterruptedException, ExecutionException {

		final Properties properties = new Properties();

		properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		ExecutorService service = null;
		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();
			service = Executors.newFixedThreadPool(3/*threads*/);

			// wrap a 2nd sail around a different tripleStore.
			final BigdataSail sail2;
			{

				// tunnel through to the Journal.
				final Journal jnl = (Journal) sail.getIndexManager();

				// describe another tripleStore with a distinct namespace.
				final AbstractTripleStore tripleStore = new LocalTripleStore(
						jnl, "foo", ITx.UNISOLATED, properties);
				
				// create that triple store.
				tripleStore.create();

				// wrap a 2nd sail around the 2nd tripleStore.
				sail2 = new BigdataSail(tripleStore);
				sail2.initialize();

			}


			try {

				final Callable<SailConnection> task = new Callable<SailConnection>() {

					public SailConnection call() throws Exception {

						SailConnection conn1 = null;

							log.info("Requesting 1st unisolated connection.");

							conn1 = sail.getUnisolatedConnection();
							
							return conn1;
					}

				};

				// run task. it should block when attempting to get the 2nd
				// connection (on a different thread)
				Future<SailConnection> f1 = service.submit(task);
				Future<SailConnection> f2 = service.submit(task);

				// wait up to a timeout to verify that the task blocked rather
				// than acquiring the 2nd connection.
				try {
					f2.get(250, TimeUnit.MILLISECONDS);	
			} catch (TimeoutException e) {
				/*
				 * This is the expected outcome.
				 */
				log.info("timeout");
				}
				
				f1.get().close();
				
				// Now we can get the second connection
				try {
					f2.get(250, TimeUnit.MILLISECONDS).close();	
				} catch (TimeoutException e) {
					fail("Should have been able to get second connection");
				}

			} finally {

				if (sail2 != null)
					sail2.shutDown();

				sail.shutDown();

			}

		} finally {

			if (service != null) {
				service.shutdownNow();
			}

			sail.getIndexManager().destroy();

		}

	}

	/**
	 * Test creates a database, obtains a writable connection, writes some data
	 * on the store, verifies that the data can be read back from within the
	 * connection but that it is not visible in a read-committed view, commits
	 * the write set, and verifies that the data is now visible in a
	 * read-committed view.
	 * 
	 * TODO variant that writes, aborts the write, and verifies that the data
	 * was not made restart safe.
	 * 
	 * @throws SailException
	 * @throws InterruptedException
	 */
	public void test_isolationOfUnisolatedConnection() throws SailException,
			InterruptedException {

		final Properties properties = new Properties();

		properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		BigdataSailConnection conn = null;

		BigdataSailConnection readConn = null;

		final BigdataSail sail = new BigdataSail(properties);

		try {

			sail.initialize();

			// the unisolated connection
			conn = sail.getUnisolatedConnection();

			// a read-only transaction.
			readConn = sail.getReadOnlyConnection();

            final URI s = new URIImpl("http://www.bigdata.com/s");

            final URI p = new URIImpl("http://www.bigdata.com/p");

            final Value o = new LiteralImpl("o");

            // add a statement.
            conn.addStatement(s, p, o);

            // verify read back within the connection.
            {

                int n = 0;

                final CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                try {

                    while (itr.hasNext()) {

                        BigdataStatement stmt = (BigdataStatement) itr.next();

                        assertEquals("subject", s, stmt.getSubject());
                        assertEquals("predicate", p, stmt.getPredicate());
                        assertEquals("object", o, stmt.getObject());
                        // // @todo what value should the context have?
                        // assertEquals("context", null, stmt.getContext());

                        n++;

                    }

                } finally {

                    itr.close();

                }

                assertEquals("#statements visited", 1, n);
            }

            // verify NO read back in the read-committed view.
            {
                
                int n = 0;

                final CloseableIteration<? extends Statement, SailException> itr = readConn
                        .getStatements(s, p, o, false/* includeInferred */);

                try {
                
                while (itr.hasNext()) {

                    itr.next();
                    
                    n++;

                }
                
                } finally {
                    
                    itr.close();
                    
                }

                assertEquals("#statements visited", 0, n);
                
            }

            // commit the connection.
            conn.commit();

            // verify read back in the read-committed view.
            {

                int n = 0;

                final CloseableIteration<? extends Statement, SailException> itr = conn
                        .getStatements(s, p, o, false/* includeInferred */);

                try {

                    while (itr.hasNext()) {

                        BigdataStatement stmt = (BigdataStatement) itr.next();

                        assertEquals("subject", s, stmt.getSubject());
                        assertEquals("predicate", p, stmt.getPredicate());
                        assertEquals("object", o, stmt.getObject());
                        // // @todo what value should the context have?
                        // assertEquals("context", null, stmt.getContext());

                        n++;

                    }

                } finally {

                    itr.close();
                    
                }
                
                assertEquals("#statements visited", 1, n);
                
            }

        } finally {

            if (conn != null)
                conn.close();

            if (readConn != null)
                readConn.close();
            
			sail.getIndexManager().destroy();

        }

    }

//	/**
//	 * Unit test verifies that we can mix read/write transactions and the use
//	 * of the unisolated connection.
//	 */
//	public void test_readWriteTxAndUnisolatedConnection() {
//		fail("write this test");
//	}
	
}
