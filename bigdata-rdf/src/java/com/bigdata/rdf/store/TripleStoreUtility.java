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
 * Created on Nov 14, 2008
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;

import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rio.AbstractStatementBuffer.StatementBuffer2;
import com.bigdata.rdf.rules.BackchainAccessPath;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility class for comparing graphs for equality, bulk export, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TripleStoreUtility {
    
    protected static final Logger log = Logger.getLogger(TripleStoreUtility.class);

    private static final long NULL = IRawTripleStore.NULL;
    
    /**
     * Compares two RDF graphs for equality (same statements).
     * <p>
     * Note: This does NOT handle bnodes, which much be treated as variables for
     * RDF semantics.
     * <p>
     * Note: Comparison is performed in terms of the externalized RDF
     * {@link Statement}s rather than {@link SPO}s since different graphs use
     * different lexicons.
     * <p>
     * Note: If the graphs differ in which entailments they are storing in their
     * data and which entailments are backchained then you MUST make them
     * consistent in this regard. You can do this by exporting one or both using
     * {@link #bulkExport(AbstractTripleStore)}, which will cause all
     * entailments to be materialized in the returned {@link TempTripleStore}.
     * 
     * @param expected
     *            One graph.
     * 
     * @param actual
     *            Another graph <strong>with a consistent policy for forward and
     *            backchained entailments</strong>.
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     */
    public static boolean modelsEqual(AbstractTripleStore expected,
            AbstractTripleStore actual) throws Exception {

        //        int actualSize = 0;
        int notExpecting = 0;
        int expecting = 0;
        boolean sameStatements1 = true;
        {

            final ICloseableIterator<BigdataStatement> it = notFoundInTarget(actual, expected);

            try {

                while (it.hasNext()) {

                    final BigdataStatement stmt = it.next();

                    sameStatements1 = false;

                    log("Not expecting: " + stmt);

                    notExpecting++;

                    //                    actualSize++; // count #of statements actually visited.

                }

            } finally {

                it.close();

            }

            log("all the statements in actual in expected? " + sameStatements1);

        }

        //        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            final ICloseableIterator<BigdataStatement> it = notFoundInTarget(expected, actual);

            try {

                while (it.hasNext()) {

                    final BigdataStatement stmt = it.next();

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);

                    expecting++;

                    //                    expectedSize++; // counts statements actually visited.

                }

            } finally {

                it.close();

            }

            //          BigdataStatementIterator it = expected.asStatementIterator(expected
            //          .getInferenceEngine().backchainIterator(
            //                  expected.getAccessPath(NULL, NULL, NULL)));
            //
            //            try {
            //
            //                while(it.hasNext()) {
            //
            //                BigdataStatement stmt = it.next();
            //
            //                if (!hasStatement(actual,//
            //                        (Resource)actual.getValueFactory().asValue(stmt.getSubject()),//
            //                        (URI)actual.getValueFactory().asValue(stmt.getPredicate()),//
            //                        (Value)actual.getValueFactory().asValue(stmt.getObject()))//
            //                        ) {
            //
            //                    sameStatements2 = false;
            //
            //                    log("    Expecting: " + stmt);
            //                    
            //                    expecting++;
            //
            //                }
            //                
            //                expectedSize++; // counts statements actually visited.
            //
            //                }
            //                
            //            } finally {
            //                
            //                it.close();
            //                
            //            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        //        final boolean sameSize = expectedSize == actualSize;
        //        
        //        log("size of 'expected' repository: " + expectedSize);
        //
        //        log("size of 'actual'   repository: " + actualSize);

        log("# expected but not found: " + expecting);

        log("# not expected but found: " + notExpecting);

        return /*sameSize &&*/sameStatements1 && sameStatements2;

    }

    public static void log(String s) {

        System.err.println(s);

    }

    /**
     * Visits <i>expected</i> {@link BigdataStatement}s not found in <i>actual</i>.
     * 
     * @param expected
     * @param actual
     * 
     * @return An iterator visiting {@link BigdataStatement}s present in
     *         <i>expected</i> but not found in <i>actual</i>.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static ICloseableIterator<BigdataStatement> notFoundInTarget(//
            final AbstractTripleStore expected,//
            final AbstractTripleStore actual //
    ) throws InterruptedException, ExecutionException {

        /*
         * The source access path is a full scan of the SPO index.
         */
        final IAccessPath<ISPO> expectedAccessPath = expected.getAccessPath(
                NULL, NULL, NULL);

        /*
         * Efficiently convert SPOs to BigdataStatements (externalizes
         * statements).
         */
        final BigdataStatementIterator itr2 = expected
                .asStatementIterator(expectedAccessPath.iterator());

        final int capacity = 100000;

        final BlockingBuffer<BigdataStatement> buffer = new BlockingBuffer<BigdataStatement>(
                capacity);

        final StatementBuffer2<Statement, BigdataStatement> sb = new StatementBuffer2<Statement, BigdataStatement>(
                actual, true/* readOnly */, capacity) {

            /**
             * Statements not found in [actual] are written on the
             * BlockingBuffer.
             * 
             * @return The #of statements that were not found.
             */
            @Override
            protected int handleProcessedStatements(final BigdataStatement[] a) {

                if (log.isInfoEnabled())
                    log.info("Given " + a.length + " statements");

                // bulk filter for statements not present in [actual].
                final IChunkedOrderedIterator<ISPO> notFoundItr = actual
                        .bulkFilterStatements(a, a.length, false/* present */);

                int nnotFound = 0;

                try {

                    while (notFoundItr.hasNext()) {

                        final ISPO notFoundStmt = notFoundItr.next();

                        if (log.isInfoEnabled())
                            log.info("Not found: " + notFoundStmt);

                        buffer.add((BigdataStatement) notFoundStmt);

                        nnotFound++;

                    }

                } finally {

                    notFoundItr.close();

                }

                if (log.isInfoEnabled())
                    log.info("Given " + a.length + " statements, " + nnotFound
                            + " of them were not found");

                return nnotFound;

            }

        };

        /**
         * Run task. The task consumes externalized statements from [expected]
         * and writes statements not found in [actual] onto the blocking buffer.
         */
        buffer.setFuture(actual.getExecutorService().submit(
                new Callable<Void>() {

                    public Void call() throws Exception {

                        try {

                            while (itr2.hasNext()) {

                                // a statement from the source db.
                                final BigdataStatement stmt = itr2.next();

                                // if (log.isInfoEnabled()) log.info("Source: "
                                // + stmt);

                                // add to the buffer.
                                sb.add(stmt);

                            }

                        } finally {

                            itr2.close();

                        }

                        /*
                         * Flush everything in the StatementBuffer so that it
                         * shows up in the BlockingBuffer's iterator().
                         */

                        final long nnotFound = sb.flush();

                        if (log.isInfoEnabled())
                            log.info("Flushed: #notFound=" + nnotFound);

                        return null;

                    }

                }));

        /*
         * Return iterator reading "not found" statements from the blocking
         * buffer.
         */

        return buffer.iterator();

    }

    /**
     * Exports all statements found in the data and all backchained entailments
     * for the <i>db</i> into a {@link TempTripleStore}. This may be used to
     * compare graphs purely in their data by pre-generation of all backchained
     * entailments.
     * <p>
     * Note: This is not a general purpose bulk export as it uses only a single
     * access path, does not store justifications, and does retain the
     * {@link Axioms} model of the source graph. This method is specifically
     * designed to export "just the triples", e.g., for purposes of comparison.
     * 
     * @param db
     *            The source database.
     * 
     * @return The {@link TempTripleStore}.
     */
    static public TempTripleStore bulkExport(AbstractTripleStore db) {
    
        final Properties properties = new Properties();
        
        properties.setProperty(Options.ONE_ACCESS_PATH, "true");
        
        properties.setProperty(Options.JUSTIFY, "false");
        
        properties.setProperty(Options.AXIOMS_CLASS,
                NoAxioms.class.getName());
        
        final TempTripleStore tmp = new TempTripleStore(properties);
    
        final StatementBuffer sb = new StatementBuffer(tmp, 100000/* capacity */);
    
        final IChunkedOrderedIterator<ISPO> itr1 = new BackchainAccessPath(db,
                db.getAccessPath(NULL, NULL, NULL)).iterator();
    
        final BigdataStatementIterator itr2 = db.asStatementIterator(itr1);
    
        try {
    
            while (itr2.hasNext()) {
    
                final BigdataStatement stmt = itr2.next();
    
                sb.add(stmt);
    
            }
    
        } finally {
    
            try {
                itr2.close();
                
            } catch (SailException ex) {
                
                throw new RuntimeException(ex);
                
            }
    
        }
    
        sb.flush();
    
        return tmp;
    
    }

    /**
     * Compares two {@link LocalTripleStore}s
     * 
     * @param args
     *            filename filename (namespace)
     * 
     * @throws Exception
     *  
     * @todo namespace for each, could be the same file, and timestamp for each.
     * 
     * @todo handle other database modes.
     */
    public static void main(String[] args) throws Exception {
        
        if (args.length < 2 || args.length > 3) {

            usage();
            
        }

        final File file1 = new File(args[0]);

        final File file2 = new File(args[1]);

        final String namespace = args.length == 3 ? args[2] : "kb";
        
        if (!file1.exists())
            throw new FileNotFoundException(file1.toString());

        if (!file2.exists())
            throw new FileNotFoundException(file2.toString());

        Journal j1 = null, j2 = null;

        try {

            final Properties p = new Properties();

            p.setProperty(com.bigdata.journal.Options.READ_ONLY, "true");

            final AbstractTripleStore ts1;
            {
                Properties properties = new Properties(p);

                properties.setProperty(com.bigdata.journal.Options.FILE, file1
                        .toString());

                j1 = new Journal(properties);

                ts1 = (AbstractTripleStore) j1.getResourceLocator().locate(
                        namespace,
                        TimestampUtility.asHistoricalRead(j1
                                .getLastCommitTime()));

            }

            final AbstractTripleStore ts2;
            {
                Properties properties = new Properties(p);

                properties.setProperty(com.bigdata.journal.Options.FILE, file2
                        .toString());

                j2 = new Journal(properties);

                ts2 = (AbstractTripleStore) j2.getResourceLocator().locate(
                        namespace,
                        TimestampUtility.asHistoricalRead(j2
                                .getLastCommitTime()));

            }

            modelsEqual(ts1, ts2);
            
        } finally {
            
            if (j1 != null)
                j1.close();

            if (j2 != null)
                j2.close();
            
        }
        
    }

    private static void usage() {
        
        System.err.println("usage: filename filename (namespace)");

        System.exit(1);
        
    }
    
}
