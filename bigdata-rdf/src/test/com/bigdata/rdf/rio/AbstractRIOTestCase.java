/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Apr 18, 2009
 */

package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Abstract base class for unit tests involving the RIO integration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRIOTestCase extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public AbstractRIOTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRIOTestCase(String name) {
        super(name);
    }

    /**
     * Test loads an RDF/XML resource into a database and then verifies by
     * re-parse that all expected statements were made persistent in the
     * database.
     * 
     * @param resource
     * 
     * @throws Exception
     */
    protected void doLoadAndVerifyTest(final String resource,
            final boolean parallel) throws Exception {

        AbstractTripleStore store = getStore();

        try {

            doLoad(store, resource, parallel);

            store.commit();

            if (store.isStable()) {

                store = reopenStore(store);

            }

            doVerify(store, resource, parallel);

        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * Implementation must load the data. Generally, you create an
     * {@link IStatementBufferFactory} and then invoke
     * {@link #doLoad(AbstractTripleStore, String, boolean, IStatementBufferFactory)}
     * 
     * @param store
     * @param resource
     * @param parallel
     *            when <code>true</code> multiple source files will be loaded
     *            and verified in parallel.
     */
    abstract protected void doLoad(AbstractTripleStore store, String resource,
            boolean parallel) throws Exception;

    /**
     * Load the classpath resource or file / directory.
     * <p>
     * Note: Normally we disable closure for this test, but that is not
     * critical. If you compute the closure of the data set then there will
     * simply be additional statements whose self-consistency among the
     * statement indices will be verified, but it will not verify the
     * correctness of the closure.
     * 
     * @param store
     *            The KB into which the data will be loaded.
     * @param resource
     *            A classpath resource, a file, or a directory (processed
     *            recursively). Hidden files are NOT loaded in order to skip
     *            .svn and CVS directories.
     * @param factory
     *            The factory under test.
     * 
     * @throws Exception
     */
    protected void doLoad(final AbstractTripleStore store,
            final String resource,
            final boolean parallel,
            final IStatementBufferFactory<BigdataStatement> factory)
            throws Exception {

        // tasks to load the resource or file(s)
        final List<Callable<Void>> tasks = getLoadTasks(resource, factory);

        if (log.isInfoEnabled())
            log.info("Will run " + tasks.size() + " load tasks.");

        if (parallel) {

            final List<Future<Void>> futures = store.getExecutorService()
                    .invokeAll(tasks);

            for (Future<Void> f : futures) {

                // look for error on each task.
                f.get();

            }

        } else {

            // run verify tasks in sequence.
            for (Callable<Void> t : tasks) {

                t.call();

            }

        }

    }

    /**
     * Returns a list containing either a single {@link LoadTask} for a
     * classpath resource or a file or a set of {@link LoadTask} for the files
     * in a directory.
     * 
     * @param resource
     * @param factory
     * @return
     */
    protected List<Callable<Void>> getLoadTasks(final String resource,
            final IStatementBufferFactory<BigdataStatement> factory) {
        
        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
        
        if (getClass().getResource(resource) != null) {

            // load a resource on the classpath
            tasks.add(new LoadTask(resource,factory));
            
            return tasks;
            
        }

        // try file system.
        final File file = new File(resource);

        if (!file.exists()) {

//            throw new RuntimeException("No such resource/file: " + resource);
            throw new AssertionFailedError("Resource not found: " + file
                    + ", test skipped: " + getName());
            
        }
        
        addFileLoadTask( file, tasks, factory );

        return tasks;
        
    }
    
    /**
     * Adds a {@link LoadTask} for the file or for all files in a directory
     * (recursively).
     */
    private void addFileLoadTask(final File file,
            final List<Callable<Void>> tasks,
            final IStatementBufferFactory<BigdataStatement> factory) {

        if (file.isHidden()) {

            log.warn("Skipping hidden file: " + file);

            return;

        }

        if (!file.canRead()) {

            log.warn("Can not read file: " + file);

            return;

        }

        if (file.isDirectory()) {

            if (log.isInfoEnabled())
                log.info("Loading directory: " + file);

            final File[] files = file.listFiles();

            for (File t : files) {

                addFileLoadTask(t, tasks, factory);

            }

            // done.
            return;

        } else {

            // load a file.
            tasks.add(new LoadTask(file.toString(), factory));

        }

    }
    
    /**
     * Load a file from the classpath or the file system.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class LoadTask implements Callable<Void> {

        private final String resource;

        private final IStatementBufferFactory<BigdataStatement> factory;

        public LoadTask(final String resource,
                final IStatementBufferFactory<BigdataStatement> factory) {

            if (resource == null)
                throw new IllegalArgumentException();
            
            if (factory == null)
                throw new IllegalArgumentException();
         
            this.resource = resource;
            
            this.factory = factory;
            
        }
        
        public Void call() throws Exception {

            loadOne(resource, factory);

            // done.
            return null;
            
        }
        
        /**
         * Load a resource from the classpath or the file system.
         * 
         * @param resource
         *            A resource on the class path, a file, or a directory.
         *            
         * @param factory
         * 
         * @throws IOException
         * @throws URISyntaxException 
         */
        protected void loadOne(final String resource,
                IStatementBufferFactory<? extends BigdataStatement> factory)
                throws IOException, URISyntaxException {

            if (log.isInfoEnabled())
                log.info("Loading: " + resource + " using " + factory);

            String baseURI = null;
            
            InputStream rdfStream = null;
            try {

                // try the classpath
                rdfStream = getClass().getResourceAsStream(resource);

                if (rdfStream != null) {
                    
                    // set for resource on classpath.
                    baseURI = getClass().getResource(resource).toURI().toString();
                    
                } else {

                    // try file system.
                    final File file = new File(resource);

//                    if (file.isHidden() || !file.canRead()
//                            || file.isDirectory()) {
//
//                        log.warn("Ignoring file: " + file);
//
//                        // Done.
//                        return;
//                        
//                    }
                    
                    if (file.exists()) {

                        rdfStream = new FileInputStream(file);

                        // set for file as URI.
                        baseURI = file.toURI().toString();
                        
                    } else {
                        
                        fail("Could not locate resource: " + resource);
                    
                    }
                    
                }

                /*
                 * Obtain a buffered reader on the input stream.
                 */
                final Reader reader = new BufferedReader(new InputStreamReader(
                        rdfStream));

                try {

                    // guess at the RDF Format, assume RDF/XML.
                    final RDFFormat rdfFormat = RDFFormat.forFileName(resource,
                            RDFFormat.RDFXML);
                    
                    final RDFParserOptions options = new RDFParserOptions();
                    
                    // verify RDF/XML syntax.
                    options.setVerifyData(true);

                    // Setup the loader.
                    final PresortRioLoader loader = new PresortRioLoader(factory
                            .newStatementBuffer());

                    // add listener to log progress.
                    loader.addRioLoaderListener(new RioLoaderListener() {

                        public void processingNotification(RioLoaderEvent e) {

                            if (log.isInfoEnabled())
                                log.info(e.getStatementsProcessed() + " stmts added in "
                                        + (e.getTimeElapsed() / 1000d) + " secs, rate= "
                                        + e.getInsertRate());

                        }

                    });

                    loader.loadRdf((Reader) reader, baseURI, rdfFormat, options);

                    if (log.isInfoEnabled())
                        log.info("Done: " + resource);
//                    + " : tps="
//                                + loader.getInsertRate() + ", elapsed="
//                                + loader.getInsertTime() + ", statementsAdded="
//                                + loader.getStatementsAdded());

                } catch (Exception ex) {

                    throw new RuntimeException("While loading: " + resource, ex);

                } finally {

                    try {
                        reader.close();
                    } catch (Throwable t) {
                        log.error(t);
                    }

                }

            } finally {

                if (rdfStream != null) {

                    try {
                        rdfStream.close();
                    } catch (Throwable t) {
                        log.error(t);
                    }

                }

            }

        }

    }

    /**
     * Verify the KB contains all explicit statements read from the resource.
     * 
     * @param store
     *            The KB.
     * @param resource
     *            A classpath resource, file, or directory (processed
     *            recursively).
     * @param parallel
     *            When <code>true</code>, multiple source files will be
     *            verified in parallel.
     */
    protected void doVerify(final AbstractTripleStore store,
            final String resource, final boolean parallel) {

        // tasks to verify the loaded resource or file(s)
        final List<Callable<Void>> tasks = getVerifyTasks(resource, store);

        if (log.isInfoEnabled())
            log.info("Will run " + tasks.size() + " verify tasks.");

        try {

            if (parallel) {
                
                // run parallel.
                final List<Future<Void>> futures = store.getExecutorService()
                        .invokeAll(tasks);

                for (Future<Void> f : futures) {

                    // look for error on each task.
                    f.get();

                }
                
            } else {
                
                // run verify tasks in sequence.
                for (Callable<Void> t : tasks) {

                    t.call();

                }
                
            }

        } catch (Throwable t) {

            // rethrow
            throw new RuntimeException(t);

        }

    }

    /**
     * Return a list of tasks which will verify the statements contained in the
     * specified classpath resource, file, or directory (recursive) against the
     * KB.
     * 
     * @param resource
     * @param store
     * @return
     */
    protected List<Callable<Void>> getVerifyTasks(final String resource,
            final AbstractTripleStore store) {

        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

        if (getClass().getResource(resource) != null) {

            // load a resource on the classpath
            tasks.add(new VerifyTask(resource, store));

            return tasks;

        }

        // try file system.
        final File file = new File(resource);

        if (!file.exists()) {

            throw new RuntimeException("No such resource/file: " + resource);

        }

        addFileVerifyTask(file, tasks, store);

        return tasks;

    }

    private void addFileVerifyTask(final File file,
            final List<Callable<Void>> tasks, final AbstractTripleStore store) {

        if (file.isHidden()) {

            log.warn("Skipping hidden file: " + file);

            return;

        }

        if (!file.canRead()) {

            log.warn("Can not read file: " + file);

            return;

        }

        if (file.isDirectory()) {

            if (log.isInfoEnabled())
                log.info("Loading directory: " + file);

            final File[] files = file.listFiles();

            for (File t : files) {

                addFileVerifyTask(t, tasks, store);

            }

            // done.
            return;

        } else {

            // load a file.
            tasks.add(new VerifyTask(file.toString(), store));

        }

    }
    
    /**
     * Verify that the contents of a classpath resource or a file were loaded
     * into the KB.
     * <p>
     * What is actually verified is that all statements that are re-parsed are
     * found in the KB, that the lexicon is self-consistent, and that the
     * statement indices are self-consistent. The test does NOT reject a KB
     * which has statements not found during the re-parse since there can be
     * axioms and other stuff in the KB.
     * 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class VerifyTask implements Callable<Void> {

        private final String resource;
        private final AbstractTripleStore store;

        public VerifyTask(final String resource, final AbstractTripleStore store) {

            if (resource == null)
                throw new IllegalArgumentException();
            
            if (store == null)
                throw new IllegalArgumentException();
         
            this.resource = resource;
            
            this.store = store;
            
        }
        
        public Void call() throws Exception {

            if (log.isInfoEnabled())
                log.info("Will verify: " + resource);
            
            verify();

            // done.
            return null;
            
        }

        /**
         * Verify that the explicit statements given by the resource are present
         * in the KB.
         * 
         * @throws FileNotFoundException
         * @throws Exception
         * 
         * @todo test based on this method will probably fail if the source data
         *       contains bnodes since it does not validate bnodes based on
         *       consistent RDF properties but only based on their Java fields.
         */
        private void verify() throws FileNotFoundException, Exception {

            if (log.isInfoEnabled()) {
                log.info("computing predicate usage...");
                log.info("\n" + store.predicateUsage());
            }

            /*
             * re-parse and verify all statements exist in the db using each
             * statement index.
             */
            final AtomicInteger nerrs = new AtomicInteger(0);
            final int maxerrors = 20;
            {

                log.info("Verifying all statements found using reparse: file="
                        + resource);

                final String baseURI; ;
                if (getClass().getResource(resource) != null) {
                    
                    baseURI = getClass().getResource(resource).toURI()
                            .toString();
                    
                } else {
                    
                    baseURI = new File(resource).toURI().toString();
                    
                }
                
                // buffer capacity (#of statements per batch).
                final int capacity = 100000;

                final IRioLoader loader = new StatementVerifier(store,
                        capacity, nerrs, maxerrors);

                final RDFFormat rdfFormat = RDFFormat.forFileName(resource,
                        RDFFormat.RDFXML);

                final RDFParserOptions options = new RDFParserOptions();
                
                options.setVerifyData(false);

                loader.loadRdf(new BufferedReader(new InputStreamReader(
                        new FileInputStream(resource))), baseURI, rdfFormat,
                        options);

                if(log.isInfoEnabled())
                	log.info("End of reparse: nerrors=" + nerrs + ", file="
                        + resource);

            }

            assertEquals("nerrors", 0, nerrs.get());

            assertStatementIndicesConsistent(store, maxerrors);

        }

    }

}
