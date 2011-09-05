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
/*
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.TestASTNamedSubqueryEvaluation;
import com.bigdata.rdf.sparql.ast.optimizers.BadlyDesignedLeftJoinsUtil.BadlyDesignedLeftJoinIteratorException;

/**
 * Test suite for {@link BadlyDesignedLeftJoinsUtil}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBadlyDesignedLeftJoinUtil extends AbstractASTEvaluationTestCase {

    private static final Logger log = Logger
            .getLogger(TestBadlyDesignedLeftJoinUtil.class);

    /**
     * 
     */
    public TestBadlyDesignedLeftJoinUtil() {
    }

    /**
     * @param name
     */
    public TestBadlyDesignedLeftJoinUtil(String name) {
        super(name);
    }

    /**
     * Unit test with an optional nested within another optional where there is
     * no shared variable in the outer optional but the inner optional shares a
     * variable with the outer most group.
     */
    public void test_badlyDesigned() throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX : <http://www.bigdata.com/>"+
        		"select *" +
        		" WHERE {" +
        		" ?X :name :paul" + // X in the outer level
        		" OPTIONAL {" +
        		" ?Y :name :george" + // X is not present here.
        		" OPTIONAL {" +
        		" ?X :email ?Z" + // X in the inner level
        		" }}" +
        		"}";
        
        final String baseURI = null;//"http://www.bigdata.com/";
        
        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        if (log.isInfoEnabled())
            log.info("\n" + BOpUtility.toString(queryRoot));
        
        try {

            BadlyDesignedLeftJoinsUtil
                    .checkForBadlyDesignedLeftJoin(queryRoot);
            
            fail("Expecting: "
                    + BadlyDesignedLeftJoinIteratorException.class);
            
        } catch (BadlyDesignedLeftJoinIteratorException ex) {
            
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
            
        }
        
    }
    
    public void test_wellDesigned_usingSharedVar() throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX : <http://www.bigdata.com/>"+
                "select *" +
                " WHERE {" +
                " ?X :name :paul" + // X in the outer level
                " OPTIONAL {" +
                " ?X :name :george" + // X is not present here.
                " OPTIONAL {" +
                " ?X :email ?Z" + // X in the inner level
                " }}" +
                "}";
        
        final String baseURI = null;//"http://www.bigdata.com/";
        
        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        if (log.isInfoEnabled())
            log.info("\n" + BOpUtility.toString(queryRoot));

        BadlyDesignedLeftJoinsUtil.checkForBadlyDesignedLeftJoin(queryRoot);
        
    }

    /**
     * Slight variation on the query in the test above. In this case the
     * optionals are siblings within the same join group (in the test above they
     * are nested within one another).
     * 
     * @throws MalformedQueryException
     */
    public void test_wellDesigned() throws MalformedQueryException {
    
        final String queryStr = "" +
                "PREFIX : <http://www.bigdata.com/>"+
                "select *" +
                " WHERE {" +
                " ?X :name :paul" +
                " OPTIONAL {" +
                " ?Y :name :george" +
                " } OPTIONAL {" +
                " ?X :email ?Z" +
                " }" +
                "}";
        
        final String baseURI = null;//"http://www.bigdata.com/";
        
        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        if (log.isInfoEnabled())
            log.info("\n" + BOpUtility.toString(queryRoot));
        
        // The query is Ok. Nothing should be thrown.
        BadlyDesignedLeftJoinsUtil.checkForBadlyDesignedLeftJoin(queryRoot);

    }

    /*
     * TODO I am not sure what the right answer is here. There is an
     * intermediate join group with no variables in it, which is why the fixture
     * under test reports this as badly designed. However, I am not sure that it
     * really is badly designed in that sense since each place where a join
     * occurs shares the variable ?X. It is just that there is an intermediate
     * level with an otherwise empty join group.
     */
//    /**
//     * Slight variation on the query in the test above. In this case there is
//     * a shared variable.
//     * 
//     * @throws MalformedQueryException
//     */
//    public void test_wellDesigned2() throws MalformedQueryException {
//
//        final String queryStr = "" +
//                "PREFIX : <http://www.bigdata.com/>"+
//                "select *" +
//                " WHERE {" +
//                " ?X :name :paul" + // X at the top level
//                " OPTIONAL {{" +
//                " ?X :name :george" + // X in the middle level
//                " } OPTIONAL {" +
//                " ?X :email ?Z" + // X at the bottom level.
//                " }}" +
//                "}";
//        
//        final String baseURI = null;//"http://www.bigdata.com/";
//        
//        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
//                .parseQuery2(queryStr, baseURI);
//
//        if (log.isInfoEnabled())
//            log.info("\n" + BOpUtility.toString(queryRoot));
//
//        // The query is Ok. Nothing should be thrown.
//        BadlyDesignedLeftJoinsUtil.checkForBadlyDesignedLeftJoin(queryRoot);
//        
//    }
    
}
