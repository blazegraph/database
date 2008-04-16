/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Jan 4, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Unit tests for high-level query.
 * 
 * @todo add unit tests for the join optimization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQuery extends AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestQuery() {
    }

    /**
     * @param arg0
     */
    public TestQuery(String arg0) {
        super(arg0);
    }

    /**
     * The namespace used when the LUBM data set was generated.
     */
    final String ub = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
    
    /**
     * Load the data set (LUBM with 1 university).
     */
    protected void setUp() throws Exception {

        super.setUp();

        File dir = new File("src/resources/U1");

        String[] filenames = dir.list(new FilenameFilter() {

            public boolean accept(File parent, String name) {

                return name.endsWith(".owl");

            }
        }

        );

        final int n = filenames.length;

        String[] resource = new String[n];

        String[] baseURL = new String[n];

        RDFFormat[] rdfFormat = new RDFFormat[n];

        for (int i = 0; i < n; i++) {

            final File file = new File(dir, filenames[i]);
            
            resource[i] = file.toString();

            baseURL[i] = file.toURI().toString();

            rdfFormat[i] = RDFFormat.RDFXML;

        }

        log.info("Loading " + n + " files from " + dir);

        DataLoader dataLoader = sail.database.getDataLoader();

        dataLoader.loadData(resource, baseURL, rdfFormat);

    }
    
    /**
     * A hand-compiled high-level query based on query#1 from LUBM
     * 
     * <pre>
     *           SELECT X
     *           from
     *            ub:GraduateStudent {X}. ub:takesCourse {Y}
     *           where Y=http://www.Department0.University0.edu/GraduateCourse0
     *        using namespace ub=http://www.lehigh.edu/&tilde;zhp2/2004/0401/univ-bench.owl#
     * </pre>
     * 
     * The ground truth result is 4 binding solutions for X:
     * 
     * <pre>
     * http://www.Department0.University0.edu/GraduateStudent101
     * http://www.Department0.University0.edu/GraduateStudent124
     * http://www.Department0.University0.edu/GraduateStudent142
     * http://www.Department0.University0.edu/GraduateStudent44
     * </pre>
     * 
     * @throws SailException
     * @throws QueryEvaluationException
     */
    public void test_query() throws SailException, QueryEvaluationException {

//        assertEquals(new URIImpl(
//                "http://www.Department0.University0.edu/GraduateStudent44"),
//                new BigdataURIImpl("http://www.Department0.University0.edu/GraduateStudent44",IRawTripleStore.NULL));

        SailConnection conn = sail.getConnection();

        final URI graduateStudent = new URIImpl(ub+"GraduateStudent");

        final URI takesCourse = new URIImpl(ub+"takesCourse");
        
        final URI graduateCourse0 = new URIImpl("http://www.Department0.University0.edu/GraduateCourse0");
       
        try {

            /*
             * This is the hand-coded query.
             * 
             * Note: The 1st statement pattern is less selective in the data and
             * the join optimized should re-order the arguments to the join such
             * that the order of evaluation is reversed. You can verify this by
             * checking the log.
             */
            
            TupleExpr tupleExpr = new Join(//
                    new StatementPattern(//
                            new Var("X"), // unbound
                            new Var("1", RDF.TYPE),//
                            new Var("2", graduateStudent)),//
                    new StatementPattern(//
                            new Var("X"), // unbound
                            new Var("3", takesCourse),//
                            new Var("4", graduateCourse0)));

            /*
             * Create a data set consisting of the contexts to be queried.
             * 
             * Note: a [null] DataSet will cause context to be ignored when the
             * query is processed.
             */
            DatasetImpl dataSet = null; //new DatasetImpl();
            
            BindingSet bindingSet = new QueryBindingSet();
            
            CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);

            log.info("Verifying query.");
            
            /*
             * These are the expected results for the query (the bindings for X).
             */

            final Set<URI> expected = new HashSet<URI>();

            expected
                    .add(new URIImpl(
                            "http://www.Department0.University0.edu/GraduateStudent101"));
            expected
                    .add(new URIImpl(
                            "http://www.Department0.University0.edu/GraduateStudent124"));
            expected
                    .add(new URIImpl(
                            "http://www.Department0.University0.edu/GraduateStudent142"));
            expected
                    .add(new URIImpl(
                            "http://www.Department0.University0.edu/GraduateStudent44"));
            
            /*
             * Verify that the query result is the correct solutions.
             */
            
            try {

                int i = 0;
                
                while (itr.hasNext()) {

                    BindingSet solution = itr.next();
                    
                    System.out.println("solution["+i+"] : "+solution);
                    
                    final Value actual = solution.getValue("X");
                    
                    final boolean found = expected.remove(actual);
                    
                    assertTrue("Not expecting X=" + actual, found);
                    
                    i++;
                    
                }
                
                assertEquals("#results", 4, i);

            } finally {

                itr.close();

            }

        }

        finally {

            conn.close();

        }

    }
    
}
