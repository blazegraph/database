/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 14, 2006
 * 
 * $Id$
 */
package com.bigdata.concurrent;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.TxDag.Edge;

/**
 * Test suite for online transaction deadlock algorithm.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo Write tests for the batch oriented methods
 *       {@link TxDag#addEdges(Object, Object[])} and
 *       {@link TxDag#removeEdges(Object, boolean)}. In particular, addEdges()
 *       must be atomic and removeEdges() must be tested both when the
 *       transactions is and is not known to be waiting on one or more other
 *       transactions. Also verify that getOrder() and resetOrder() are invoked
 *       properly.
 * 
 * FIXME We need to verify that the implementation of the update closure
 * algorithm is correct since it has significant complexity in order to minimize
 * its computational cost. One option is to make sure that the optimized
 * algorithm has the same behavior as the algorithm without any optimizations by
 * comparing expected and actual path count matrices. Another option is to write
 * a brute force algorithm to compute the closure of W without regard to the
 * path count matrix and verify that the predications made that algorithm are
 * indeed observed.
 * 
 * @todo In order to verify that the implemention is correct we also need to
 *       verify whether or not a deadlock is correctly reported after each
 *       insert (removal never results in a deadlock). The easiest way to do
 *       this is to implement an alternative algorithm to search for cycles in
 *       W. Such an algorithm will be more expensive to run, but will use a
 *       different logic and can therefore be used to verify that deadlocks are
 *       being correctly predicted by the {@link TxDag} implementation.
 * 
 * @todo Write tests of getEdges() when the closure is requested.
 * 
 * @todo Write a pure performance test either at this level or in terms of the
 *       2PL logic that will interact with the DAG. The
 *       {@link #testSymmetricOperations()} test is not appropriate since it is
 *       making copies of large amounts of data in order to verify that the
 *       operations are correctly reversed. Run the performance test at various
 *       concurrency levels so that we can characterize the actual cost of the
 *       implementation as a function of the concurrency. Note that while
 *       concurrency drives the size of the matrices, only the "in-use" indices
 *       from the matrices are actually visited when the closure of W* is
 *       updated.
 */

public class TestTxDag extends TestCase
{

    public static final Logger log = Logger.getLogger
	( TestTxDag.class
	  );

    public TestTxDag()
    {
    }

    public TestTxDag( String name )
    {
        super( name );
    }
    
    /**
     * Constructor tests.
     */
    public void test_ctor()
    {
        try {
            new TxDag(-1);
            fail( "Expecting: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            log.info( "Expected exception: "+ex);
        }
        try {
            new TxDag(0);
            fail( "Expecting: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            log.info( "Expected exception: "+ex);
        }
        try {
            new TxDag(1);
            fail( "Expecting: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            log.info( "Expected exception: "+ex);
        }
        new TxDag(2);
        new TxDag(20);
        new TxDag(2000);
    }
    
    /**
     * Test ability to generate unique transaction identifier used by
     * {@link TxDag}to index into its internal arrays. This test verifies that
     * insert is conditional, that lookup fails if tx was not registered, and
     * that we can insert and then lookup a transaction in the DAG. The test
     * also verifies that the {@link TxDag#size()}updates as vertices are added
     * to the graph and does not update when the vertex already exists in the
     * graph.
     */
    
    public void test_lookup_001()
    {
        
        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );

        assertEquals( "capacity", CAPACITY, dag.capacity() );
        assertEquals( "size", 0, dag.size() );

        /*
         * Lookup a vertex that does not exist and verify that the vertex was
         * not added.
         */
        assertEquals( TxDag.UNKNOWN, dag.lookup( "v1", false ) );
        assertEquals( TxDag.UNKNOWN, dag.lookup( "v1", false ) );
        assertEquals( "size", 0, dag.size() );

        /*
         * Insert a vertex.
         */
        final int t1 = dag.lookup( "v1", true );
        assertTrue(TxDag.UNKNOWN != t1 );
        assertEquals( "size", 1, dag.size() );

        /*
         * Lookup that vertex again.
         */
        assertEquals( t1, dag.lookup( "v1", false ) );
        assertEquals( "size", 1, dag.size() );

        log.info( dag.toString() );
        
    }

    /**
     * Correct rejection test when transaction object is <code>null</code>.
     */
    public void test_lookup_002()
    {
        try {
            TxDag dag = new TxDag( 1 );
            dag.lookup(null, false );
            fail( "Expecting: "+IllegalArgumentException.class );
        }
        catch( IllegalArgumentException ex ) {
            log.info( "Ignoring expected exception: "+ex);
        }
    }

    /**
     * Test capacity limits. This test verifies that insert fails if capacity
     * the graph (the maximum #of vertices) would be exceeded.
     */
    public void test_capacity_001()
    {
        
        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );

        Object[] tx = new String[ CAPACITY ];

        assertEquals( "capacity", 0, dag.size() );
        assertEquals( "size", 0, dag.size() );

        for( int i=0; i<CAPACITY; i++ ) {

            tx[ i ] = ""+i;
            dag.lookup( tx[i], true );
            assertEquals( "capacity", CAPACITY, dag.capacity() );
            assertEquals( "size", i+1, dag.size() );

        }

        assertEquals( "capacity", CAPACITY, dag.size() );
        assertEquals( "size", CAPACITY, dag.size() );
        
        try {
            dag.lookup( ""+CAPACITY, true );
            fail( "Expecting: "+IllegalStateException.class );
        }
        catch( IllegalStateException ex ) {
            log.info( "Ignoring expected exception: "+ex);
        }
    }
    
    /**
     * Simple tests of {@link TxDag#addEdge(Object, Object)},
     * {@link TxDag#hasEdge(Object, Object)}and friends.
     */

    public void test_addEdge_001()
    {

        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
        
        Object tx1 = "tx1";
        Object tx2 = "tx2";
        Object tx3 = "tx3";
        
        assertEquals("size",0,dag.size());
        assertSameValuesAnyOrder(new int[]{}, dag.getOrder());
        
        dag.addEdge(tx1,tx2); // tx1 -> tx2 (aka tx1 WAITS_FOR tx2)
        
        assertEquals("size",2,dag.size());
        assertTrue( "tx1->tx2", dag.hasEdge(tx1,tx2) );
        assertFalse("tx2->tx1", dag.hasEdge(tx2, tx1));
        assertSameValuesAnyOrder(new int[]{dag.lookup(tx1, false),dag.lookup(tx2,false)}, dag.getOrder());
        assertSameEdges(new Edge[]{new Edge(tx1,tx2,true)}, dag.getEdges(false));

        dag.addEdge(tx2,tx3);
        assertEquals("size",3,dag.size());
        assertTrue( "tx1->tx2", dag.hasEdge(tx1,tx2) );
        assertTrue( "tx2->tx3", dag.hasEdge(tx2,tx3) );
        assertSameValuesAnyOrder(new int[]{dag.lookup(tx1, false),dag.lookup(tx2,false),dag.lookup(tx3,false)}, dag.getOrder());
        assertSameEdges(new Edge[]{new Edge(tx1,tx2,true),new Edge(tx2,tx3,true)}, dag.getEdges(false));
        
    }

    /**
     * Test for correct rejection of addEdge() when edge already exists.
     */

    public void test_addEdge_correctRejection_001()
    {

        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
        
        Object tx1 = "tx1";
        Object tx2 = "tx2";
        
        dag.addEdge(tx1,tx2); // tx1 -> tx2 (aka tx1 WAITS_FOR tx2)
        try {
        	dag.addEdge(tx1,tx2);
        	fail( "Expecting exception: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
        	log.info("Expected exception: "+ex);
        }
        
    }
    
    /**
     * Test for correct rejection of addEdge() when either parameter is
     * null or when both parameters are the same.
     */

    public void test_addEdge_correctRejection_002()
    {

        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
        
        Object tx1 = "tx1";
        
        try {
            dag.addEdge(tx1,tx1);
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }
        
        try {
            dag.addEdge(null,tx1);
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }

        try {
            dag.addEdge(tx1,null);
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }

    }

    /**
     * Test for correct rejection of addEdges() when an edge already exists.
     */

    public void test_addEdges_correctRejection_001()
    {

        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
        
        Object tx1 = "tx1";
        Object tx2 = "tx2";
        
        dag.addEdges(tx1,new Object[]{tx2}); // tx1 -> tx2 (aka tx1 WAITS_FOR tx2)
        try {
        	dag.addEdges(tx1,new Object[]{tx2});
        	fail( "Expecting exception: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
        	log.info("Expected exception: "+ex);
        }
        
    }
    
    /**
     * Test for correct rejection of addEdge() when either parameter is
     * null, when one of the targets is null, or when one of the targets
     * is given more than once.
     */

    public void test_addEdges_correctRejection_002()
    {

        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
        
        Object tx1 = "tx1";
        Object tx2 = "tx2";
        
        try {
            dag.addEdges(null,new Object[]{tx1});
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }

        try {
            dag.addEdges(tx1,null);
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }

        try {
            dag.addEdges(tx1,new Object[]{tx1,tx2,tx1});
        	fail( "Expecting exception: "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
        	log.info("Expected exception: "+ex);
        }

    }

    /**
	 * Verify that {@link TxDag#lookup(Object, boolean)} does not cause
	 * {@link TxDag#getOrder()} to include the new vertex until an edge has been
	 * asserted for that vertex.
	 * <p>
	 * This also tests {@link TxDag#getOrder(int t, int u)}, which is similar
	 * to {@link TxDag#getOrder()} but it always includes the specified vertices
	 * in the returned int[].
	 * 
	 * @see TxDag#lookup(Object, boolean)
	 * @see TxDag#getOrder()
	 */

    public void test_getOrder_001()
    {
        
        final int CAPACITY = 5;
        
        TxDag dag = new TxDag( CAPACITY );
    
        assertSameValuesAnyOrder( new int[]{}, dag.getOrder() );

        final String tx1 = "tx1";
        final int tx1_id = dag.lookup(tx1,true);
        assertSameValuesAnyOrder( new int[]{}, dag.getOrder() );

        final String tx2 = "tx2";
        final int tx2_id = dag.lookup(tx2,true);
        assertSameValuesAnyOrder( new int[]{}, dag.getOrder() );
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder(tx1_id,tx2_id) );
        assertEquals("inbound(tx1)",0,dag.inbound[tx1_id]);
        assertEquals("outbound(tx1)",0,dag.outbound[tx1_id]);
        assertEquals("inbound(tx2)",0,dag.inbound[tx2_id]);
        assertEquals("outbound(tx2)",0,dag.outbound[tx2_id]);
        
        dag.addEdge(tx1, tx2);
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder() );
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder(tx1_id,tx2_id) );
        assertEquals("inbound(tx1)",0,dag.inbound[tx1_id]);
        assertEquals("outbound(tx1)",1,dag.outbound[tx1_id]);
        assertEquals("inbound(tx2)",1,dag.inbound[tx2_id]);
        assertEquals("outbound(tx2)",0,dag.outbound[tx2_id]);

        dag.removeEdge(tx1, tx2);
        assertSameValuesAnyOrder( new int[]{}, dag.getOrder() );
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder(tx1_id,tx2_id) );
        assertEquals("inbound(tx1)",0,dag.inbound[tx1_id]);
        assertEquals("outbound(tx1)",0,dag.outbound[tx1_id]);
        assertEquals("inbound(tx2)",0,dag.inbound[tx2_id]);
        assertEquals("outbound(tx2)",0,dag.outbound[tx2_id]);

        dag.addEdge(tx2, tx1);
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder() );
        assertSameValuesAnyOrder( new int[]{tx1_id,tx2_id}, dag.getOrder(tx1_id,tx2_id) );
        assertEquals("inbound(tx1)",1,dag.inbound[tx1_id]);
        assertEquals("outbound(tx1)",0,dag.outbound[tx1_id]);
        assertEquals("inbound(tx2)",0,dag.inbound[tx2_id]);
        assertEquals("outbound(tx2)",1,dag.outbound[tx2_id]);

    }
    
    /**
     * Verifies that <i>actual </i> contains all of the same values as
     * <i>expected </i> in the same order.
     * 
     * @param expected
     *            An integer array.
     * 
     * @param actual
     *            Another integer array.
     */
    
    public void assertSameValues( int[] expected, int[] actual )
    {
        assertEquals("length",expected.length,actual.length);
        final int len = expected.length;
        for( int i=0; i<len; i++ ) {
            assertEquals("position="+i, expected[i], actual[i] );
        }
    }
    
    /**
     * Verifies that <i>actual </i> contains all of the same values as
     * <i>expected </i> without regard to order.
     * 
     * @param expected
     *            An integer array.
     * 
     * @param actual
     *            Another integer array.
     */
    
    public void assertSameValuesAnyOrder( int[] expected, int[] actual )
    {
        assertEquals("length",expected.length,actual.length);
        final int len = expected.length;
        Set values = new HashSet();
        for( int i=0; i<len; i++ ) {
            values.add( new Integer( expected[ i ] ) );
        }
        if( values.size() != expected.length ) {
            throw new AssertionError("duplicate values in 'expected'.");
        }
        for( int i=0; i<len; i++ ) {
            int value = actual[ i ];
            if( ! values.remove( new Integer( value ) ) ) {
                fail( "actual["+i+"]="+value+", but that value is not in expected[].");
            }
        }
    }
    
    /**
	 * Tests of the update to the internal matrix M[u,v]. This matrix maintains
	 * the #of distinct paths from u to v based on the edges in the directed
	 * graph, W.
	 * <p>
	 * Note: These tests are written directly using the
	 * {@link TxDag#updateClosure(int, int, boolean)} method, so the matrix W is
	 * not actually updated.
	 */

    public void test_updateClosure_001()
    {

        final int CAPACITY = 2;
        
        TxDag dag = new TxDag( CAPACITY );
        
        // declare tx0 and tx1 and verify expected index assignments.
        final int tx0 = dag.lookup("tx0",true);
        final int tx1 = dag.lookup("tx1",true);
        assertEquals("tx0",0,tx0);
        assertEquals("tx1",1,tx1);
        // force display of all vertices.
        dag.inbound[tx0] = 1;
        dag.inbound[tx1] = 1;
        dag.resetOrder();

        /* W: (empty)
         * 
         * M || 0 | 1 
         * --++---+---
         * 0 || 0 | 0 
         * 1 || 0 | 0 
         */
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,0},
                {0,0}},
                dag.getPathCountMatrix()
        	);
        
        /* W:
         * 0 -> 1
         * 
         * M || 0 | 1 
         * --++---+---
         * 0 || 0 | 1 
         * 1 || 0 | 0 
         */
        assertTrue( "addEdge", dag.updateClosure(tx0,tx1,true) ); // tx0 -> tx1 (aka tx0 WAITS_FOR tx1)
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,1},
                {0,0}},
                dag.getPathCountMatrix()
                );
        
        /*
		 * Add another edge which results in a deadlock. Verify that at least
		 * one cell on the diagonal in M is now positive.
		 */
        assertFalse( "addEdge", dag.updateClosure(tx1,tx0,true) ); // tx1 -> tx0
        log.info( dag.toString() );
        int nnzero = 0;
        for( int i=0; i<CAPACITY; i++ ) {
        	if( dag.getPathCount(i,i) > 0 ) {
        		nnzero++;
        	}
        }
        if( nnzero == 0 ) {
        	fail( "No non-zero elements were found on the diagonal of M.");
        }
        
    }
    
    /**
	 * A sequence of tests of the internal state of the {@link TxDag} with a
	 * capacity of <code>4</code> after adding an edge.
	 */

    public void test_updateClosure_002()
    {

        final int CAPACITY = 4;
        
        TxDag dag = new TxDag( CAPACITY );
        
        // declare transactions and verify expected index assignments.
        final int tx0 = dag.lookup("tx0",true);
        final int tx1 = dag.lookup("tx1",true);
        final int tx2 = dag.lookup("tx2",true);
        final int tx3 = dag.lookup("tx3",true);
        assertEquals("tx0",0,tx0);
        assertEquals("tx1",1,tx1);
        assertEquals("tx2",2,tx2);
        assertEquals("tx3",3,tx3);
        // force display of all vertices.
        dag.inbound[tx0] = 1;
        dag.inbound[tx1] = 1;
        dag.inbound[tx2] = 1;
        dag.inbound[tx3] = 1;
        dag.resetOrder();

        /* W: (empty)
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 0 | 0 | 0 
         * 1 || 0 | 0 | 0 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
        	);
        
        /* W:
         * 0 -> 1
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 1 | 0 | 0 
         * 1 || 0 | 0 | 0 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "addEdge", dag.updateClosure(tx0,tx1,true) ); // tx0 -> tx1 (aka tx0 WAITS_FOR tx1)
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,1,0,0},
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );
        
        /* W:
         * 0 -> 1
         * 1 -> 2
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 1 | 1 | 0 
         * 1 || 0 | 0 | 1 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "addEdge", dag.updateClosure(tx1,tx2,true) );
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,1,1,0},
                {0,0,1,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );
        
        /* Remove the tx1->tx2 edge and verify that the prior state of M is
         * recovered.
         * 
         * W:
         * 0 -> 1
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 1 | 0 | 0 
         * 1 || 0 | 0 | 0 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "removeEdge", dag.updateClosure(tx1,tx2,false) );
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,1,0,0},
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );
        
        /* Add tx1->tx2 back in.
         * 
         * W:
         * 0 -> 1
         * 1 -> 2
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 1 | 1 | 0 
         * 1 || 0 | 0 | 1 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "addEdge", dag.updateClosure(tx1,tx2,true) );
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,1,1,0},
                {0,0,1,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );
        
        /* Remove the tx0->tx1 edge and verify the expected state for M.
         * 
         * W:
         * 1 -> 2
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 0 | 0 | 0 
         * 1 || 0 | 0 | 1 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "removeEdge", dag.updateClosure(tx0,tx1,false) );
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,0,0,0},
                {0,0,1,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );
        
        /* Remove both tx1->tx2 and verify that the M is all zeros.
         * 
         * W: (empty)
         * 
         * M || 0 | 1 | 2 | 3  
         * --++---+---+---+---
         * 0 || 0 | 0 | 0 | 0 
         * 1 || 0 | 0 | 0 | 0 
         * 2 || 0 | 0 | 0 | 0 
         * 3 || 0 | 0 | 0 | 0 
         */
        assertTrue( "removeEdge", dag.updateClosure(tx1,tx2,false) );
        log.info( dag.toString() );
        assertSamePathCounts( new int[][]{
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0},
                {0,0,0,0}},
                dag.getPathCountMatrix()
                );

        
    }
    
	/**
	 * Helper class represents the internal state of a {@link TxDag}
	 * instance and supports methods to compare the saved state with another
	 * {@link TxDag} instance.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id$
	 */
    class State
	{
		// The explicitly asserted edges for the produced state.
		public final TxDag.Edge[] edges;
		// The path count matrix for the produced state.
		public final int[][] M;
		public final int[] inbound;
		public final int[] outbound;
		public final Object[] transactions;
		/**
		 * Constructor clones the internal state of the {@link TxDag}.
		 * 
		 * @param dag The graph.
		 */
		public State( final TxDag dag )
		{
			this.edges = dag.getEdges(false);
			this.M = dag.getPathCountMatrix();
			this.inbound = (int[])dag.inbound.clone();
			this.outbound = (int[])dag.outbound.clone();
			this.transactions = (Object[])dag.transactions.clone();
		}
		/**
		 * Verify that <i>dag</i> has a state consistent with this
		 * historical state.
		 */
		public void assertSameState( TxDag dag )
		{
			assertSameEdges( edges, dag.getEdges(false) );
			assertSamePathCounts(M, dag.getPathCountMatrix());
			assertSameValues(inbound, dag.inbound);
			assertSameValues(outbound, dag.outbound);
			assertEquals("#transactions", transactions.length, dag.transactions.length );
			for( int i=0; i<transactions.length; i++ ) {
				assertEquals("transactions["+i+"]", transactions[i], dag.transactions[i]);
			}
		}
	
	} // class State.

	/**
	 * Implements the performance test for {@link #testSymmetricOperations()}.
	 * <p>
	 * Performs random additive operations on the DAG until a deadlock results.
	 * The initial state and the state after each additive operation is stored.
	 * Once a deadlock is reached, verifies that the last stored state is still
	 * valid (deadlock should not update the state of the DAG) and then performs
	 * the inverse of each of the addition operations in the reverse order in
	 * which they were applied. After each inverse operation, verifies that the
	 * state of the DAG corresponds to the state before the corresponding
	 * additive operation.
	 * <p>
	 * The additive operations and their inverses are:
	 * <ul>
	 * <li>insert/remove vertex</li>
	 * <li>insert/remove edge</li>
	 * </ul>
	 * A vertex corresponds to a transaction. Creating a vertex therefore
	 * corresponds to the action of starting a new transaction. Likewise
	 * removing a vertex corresponds to the action of terminating a transaction
	 * (either by aborting the transaction or committing the transaction).
	 * 
	 * @param r
	 *            A random number generator.
	 * @param dag
	 *            A dag with a maximum capacity.
	 */
    
    public void doSymmetricOperationsTest( Random r, TxDag dag )
    {
    	// Code for "no action".
    	final int NO_ACTION = -1;
    	// Code for action that creates a new vertex.
    	final int INSERT_VERTEX = 0;
    	// Code for action that creates an edge between two existing vertices.
    	final int INSERT_EDGE = 1;
    	// Capacity of the DAG.
    	final int capacity = dag.capacity();
    	/**
		 * Helper class records an action and the state that it produced.
		 */
    	class ActionState extends State
    	{
    		// The action that produced the state.
    		public final int action;
    		// Vertex inserted by an INSERT_VERTEX action.
    		public final Object vertex;
    		// Source and target of an INSERT_EDGE action.
    		public final Object src, tgt;
    		/**
    		 * Constructor used for the initial state (no action).
    		 */
    		ActionState( TxDag dag )
    		{
    			super(dag);
    			this.action = NO_ACTION;
    			this.vertex = null;
    			this.src = null;
    			this.tgt = null;
    		}    		
    		/**
    		 * Constructor used for INSERT_VERTEX action.
    		 * @param action INSERT_VERTEX
    		 * @param vertex The new vertex.
    		 * @param dag
    		 */
    		ActionState( int action, Object vertex, TxDag dag )
    		{
    			super(dag);
    			this.action = action;
    			this.vertex = vertex;
    			this.src = null;
    			this.tgt = null;
    		}
    		/**
    		 * Constructor used for INSERT_EDGE action.
    		 * @param action INSERT_EDGE
    		 * @param src The source of the edge.
    		 * @paramm tgt The target of the edge.
    		 * @param dag
    		 */
    		ActionState( int action, Object src, Object tgt, TxDag dag )
    		{
    			super(dag);
    			this.action = action;
    			this.vertex = null;
    			this.src = src;
    			this.tgt = tgt;
    		}
    	};
    	// Vector of states for the DAG together with the action which produced that state.
    	
    	/*
		 * Run the state machine forward adding vertices and edges randomly
		 * until a deadlock results.
		 */
    	Vector history = new Vector();
    	Vector vertices = new Vector();
    	boolean done = false; // set true to terminate this loop.
    	history.add( new ActionState( dag ) ); // record initial state ("NO_ACTION").
    	while( ! done ) {
        	/*
    		 * Probability of inserting an edge, which corresponds to the
    		 * probability of one transaction waiting on another.
    		 * 
    		 * P(insertVertex) := 1 - p(insertEdge).
    		 */
    		final int size = dag.size();
    		// Note: This could be an inverse function of the #of vertices.
			final float pInsertEdge = .3f;
			// Random number used to choose the action to take.
    		float rand = r.nextFloat();
    		if( ( size == capacity ) || ( dag.size() >= 2 && rand < pInsertEdge ) ) {
    			/*
				 * Insert edge. We always insert an edge if the DAG is at
				 * capacity (no more vertices may be declared). We never insert
				 * an edge unless at least two vertices have been defined.
				 * 
				 * Choose two vertices randomly from those currently defined.
				 * The first will be the source of the edge and the 2nd will be
				 * its target. If source == target, then choose another target
				 * (since a transaction may not wait on itself).
				 * 
				 * This is done repeatedly until either a deadlock results or an
				 * edge is added. (If the edge described already exists then we
				 * redo the selection and attempt to add another edge.)
				 */
    			while (true) {
    				// Choose source vertex.
					Object src = vertices.get(r.nextInt(size));
					// Choose target vertex (target != source).
					Object tgt;
					do {
						tgt = vertices.get(r.nextInt(size));
					} while (tgt == src);
					// Add edge.
					try {
						/*
						 * Attempt to add the edge. This will either succeed or
						 * fail. There are two failure conditions: (1) the edge
						 * would result in a deadlock; and (2) the edge already
						 * exists.
						 */
						log.info("adding edge: src="+src+", tgt="+tgt);
						dag.addEdge(src, tgt);
						history.add(new ActionState(INSERT_EDGE, src, tgt, dag));
						break; // exit inner loop.
					} catch (IllegalStateException ex) {
						// Choose new source and target since this edge already
						// exists.
						log.warn("edge exists: src="+src+", tgt="+tgt);
						continue; // repeat inner loop.
					} catch (DeadlockException ex) {
						/*
						 * Adding this edge results in a deadlock. Verify that
						 * the state of the DAG was NOT modified and then break
						 * out of the additive loop so that we can start the
						 * subtractive loop.
						 */
						log.warn("deadlock results: src="+src+", tgt="+tgt);
						// verify no change in DAG state.
						((ActionState)history.get(history.size()-1)).assertSameState(dag);
						done = true; // exit outer loop.
						break; // exit inner loop.
					}
				}
    		} else {
    			/*
				 * Insert vertex. We always take this action if there are less
				 * than two vertices since we must have two vertices defined to
				 * insert an edge.
				 */
    			String tx = "tx"+size;
    			log.info("adding vertex: vertex="+tx);
    			dag.lookup(tx, true);
    			vertices.add( tx );
    			history.add( new ActionState(INSERT_VERTEX,tx,dag));
    		}
    	}
    	log.info("created history of " + (history.size() - 1)
				+ " actions resulting in " + dag.size() + " vertices and "
				+ dag.getEdges(false).length + " edges.");
    	/*
		 * Now take each action that we ran in reverse and run the action which
		 * is its inverse. E.g., if we added a vertex, then remove that vertex
		 * and if we added an edge, then remove that edge. After each inverse
		 * action verify that the new state of the DAG is consistent with the
		 * historical state of the DAG before we took the action whose effects
		 * were just undone by the inverse action.
		 */
    	for (int i = history.size()-1; i > 0; i--) {
			ActionState current = (ActionState) history.get(i);
			ActionState prior = (ActionState) history.get(i - 1);
			switch (current.action) {
			case NO_ACTION:
				throw new AssertionError();
			case INSERT_VERTEX: {
				log.info("removing vertex: "+current.vertex);
				dag.removeEdges(current.vertex, false );
				break;
			}
			case INSERT_EDGE: {
				log.info("removing edge: src="+current.src+", tgt="+current.tgt);
				dag.removeEdge(current.src, current.tgt);
				break;
			}
			}
			// verify that the inverse action restored the expected prior state.
			prior.assertSameState(dag);
		}
    }
    
    /**
     * Compares two path count matrices for equality.
     * 
     * @param expected
     *            An int[][] matrix with at least two rows and two columns.
     * @param actual
     *            Another int[][] matrix with the same dimensions.
     */
    
    public void assertSamePathCounts( int[][] expected, int[][] actual )
    {
        int nrows = expected.length;
        int ncols = expected[ 0 ].length;
        assertEquals("rows", nrows, actual.length );
        assertEquals("cols", ncols, actual[0].length );
        for( int i=0; i<nrows; i++ ) {
            for( int j=0; j<ncols; j++ ) {
                assertEquals( "M["+i+","+j+"]", expected[i][j], actual[i][j] );
            }
        }
    }
    
    /**
	 * Compares two Edge[]s and verifies that the same edges are defined without
	 * regard to order.
	 * 
	 * @param expected The expected Edge[].
	 * @param actual The actual Edge[].
	 */

    public void assertSameEdges( Edge[] expected, Edge[] actual )
    {
    	// verify arguments.
    	if( expected == null ) {
    		throw new IllegalArgumentException("expected is null");
    	}
    	if( actual == null ) {
    		fail("actual is null.");
    	}
    	// clone since we will modify expected[].
    	expected = (Edge[])expected.clone();
    	// verify length.
    	assertEquals("length",expected.length,actual.length);
        final int len = expected.length;
        // make sure there are no null elements.
        for( int i=0; i<len; i++ ) {
        	if( expected[ i ] == null ) {
        		throw new IllegalArgumentException("expected["+i+"] is null." );
        	}
        	if( actual[ i ] == null ) {
        		fail("actual["+i+"] is null." );
        	}
        }
        /*
		 * For each element in [actual], scan the [expected] edges. If no match
		 * is found, then fail. Otherwise clear the match from [expected] and
		 * repeat for the next element in [actual].
		 */
        for( int i=0; i<len; i++ ) {
            Edge actualEdge = actual[ i ];
            boolean matched = false;
            for( int j=0; j<len; j++ ) {
            	Edge expectedEdge = expected[ j ];
            	if( expectedEdge == null ) continue; // already matched.
            	if( expectedEdge.src == actualEdge.src && expectedEdge.tgt == actualEdge.tgt ) {
            		expected[ j ] = null; // clear from expected since we just matched.
            		matched = true;
            		break; // next actual edge.
            	}
            }
            if( ! matched ) {
            	fail("unexpected edge: src="+actualEdge.src+", tgt="+actual[i].tgt);
            }
        }
    }

    /**
     * Some tests to verify {@link #assertSameEdges(Edge[], Edge[])}.
     */
    public void testAssertSameEdges()
    {
    	/*
		 * Some vertices. Note that vertices get compared by _reference_ and
		 * MUST be the same instance.
		 */
    	String tx1 = "tx1";
    	String tx2 = "tx2";
    	String tx3 = "tx3";
    	String tx4 = "tx4";

    	/*
    	 * Some edges.
    	 */
    	Edge e1 = new Edge(tx1,tx2,false);
    	Edge e2 = new Edge(tx2,tx1,false);
    	Edge e3 = new Edge(tx4,tx3,false);
    	Edge e4 = new Edge(tx1,tx3,false);
    	Edge e5 = new Edge(tx2,tx3,false);
    	Edge e6 = new Edge(tx4,tx1,false);

    	// Test correct rejection of illegal arguments.
    	try {
    		assertSameEdges( null, new Edge[]{} );
    		fail("Expecting exception: "+IllegalArgumentException.class);
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Expected exception: "+ex);
    	}

    	// Test correct rejection of illegal arguments.
    	try {
    		assertSameEdges( new Edge[]{null}, new Edge[]{e1} );
    		fail("Expecting exception: "+IllegalArgumentException.class);
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Expected exception: "+ex);
    	}

    	// Test for correct acceptance when edge[]s are consistent.    	
    	assertSameEdges( new Edge[]{}, new Edge[]{} );
    	assertSameEdges( new Edge[]{e1}, new Edge[]{e1} );
    	assertSameEdges( new Edge[]{e2}, new Edge[]{e2} );
    	assertSameEdges( new Edge[]{e1,e2}, new Edge[]{e1,e2} );
    	assertSameEdges( new Edge[]{e2,e1}, new Edge[]{e1,e2} );
    	assertSameEdges( new Edge[]{e1,e2}, new Edge[]{e2,e1} );
    	assertSameEdges( new Edge[]{e1,e2,e3,e4,e5,e6}, new Edge[]{e2,e3,e6,e4,e1,e5} );

    	// Test correct failure for actual not consistent with expected.
    	try {
    		assertSameEdges( new Edge[]{e1,e2}, new Edge[]{e1} );
    		throw new RuntimeException("Expecting "+AssertionFailedError.class);
    	}
    	catch( AssertionFailedError ex ) {
    		log.info("Expected exception: "+ex);
    	}
    	
    	try {
    		assertSameEdges( new Edge[]{e1}, new Edge[]{e2} );
    		throw new RuntimeException("Expecting "+AssertionFailedError.class);
    	}
    	catch( AssertionFailedError ex ) {
    		log.info("Expected exception: "+ex);
    	}

    }
    
    /**
	 * Verify that we can recycle the internal transaction identifiers when a
	 * transaction is removed from the DAG (either through abort or commit
	 * actions).
	 * <p>
	 * Transaction objects are application defined. They are mapped into indices
	 * for the internal arrays by {@link TxDag#lookup(Object, boolean)}. Those
	 * indices must be released for reuse by {@link TxDag#releaseVertex(Object)}
	 * of the capacity of the graph will be exhausted. This test verifies that
	 * they are.
	 * <p>
	 * The test creates and removes vertices repeatedly and verifies that we can
	 * create more vertices than would be allowed for by the capacity (therefore
	 * suggesting that vertices are being recycled correctly).
	 */
    
    public void test_recyclingIndices()
    {
    	final int CAPACITY = 10;
    	final TxDag dag = new TxDag(CAPACITY);
    	for( int i=0; i<CAPACITY*2; i++ ) {
    		String tx = "tx"+i;
    		dag.lookup(tx, true );
    		dag.removeEdges(tx, false );
    	}
    }
    
    /**
     * Verify that the DAG state is correctly updated when adding a variety of
     * WAITS_FOR relationships that do NOT form cycles.
     */
    
    public void test_noCycles_001()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        final String tx2 = "tx2";
        final String tx3 = "tx3";
        final String tx4 = "tx4";
        // add edges of the form: src WAITS_FOR tgt.
        dag.addEdge(tx0, tx1);
        dag.addEdge(tx1, tx2);
        dag.addEdge(tx3, tx2);
        dag.addEdge(tx4, tx1);
        dag.addEdge(tx4, tx3);
        assertSameEdges(new Edge[] { new Edge(tx0, tx1, false),
				new Edge(tx1, tx2, false), new Edge(tx3, tx2, false),
				new Edge(tx4, tx1, false), new Edge(tx4, tx3, false) }, dag
				.getEdges(false));
    }

    /**
	 * Verify that the DAG state is correctly updated when adding a variety of
	 * WAITS_FOR relationships that do NOT form cycles (using the batch
	 * operation to add edges).
	 */
    
    public void test_noCycles__batch_001()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        final String tx2 = "tx2";
        final String tx3 = "tx3";
        final String tx4 = "tx4";
        // add edges of the form: src WAITS_FOR {tgt}.
        dag.addEdges(tx0, new Object[]{tx1,tx2,tx4});
        dag.addEdges(tx4, new Object[]{tx3});
    }

    /**
	 * The first in a series of simple tests which verify that the DAG is
	 * correctly detecting updates when a set of new edges would result in a
	 * cycle.
	 */

    public void test_deadlock_001()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        // add edges of the form: src WAITS_FOR tgt.
        dag.addEdge(tx0, tx1);
        try {
        	dag.addEdge(tx1, tx0);
        	fail("Expecting exception: "+DeadlockException.class);
        }
        catch( DeadlockException ex ) {
        	log.info("Expected exception: "+ex);
        }
    }

    public void test_deadlock_002()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        final String tx2 = "tx2";
        final String tx3 = "tx3";
        // add edges of the form: src WAITS_FOR tgt.
        dag.addEdge(tx0, tx1);
        dag.addEdge(tx2, tx1);
        dag.addEdge(tx3, tx2);
        try {
            dag.addEdge(tx1, tx3);
        	fail("Expecting exception: "+DeadlockException.class);
        }
        catch( DeadlockException ex ) {
        	log.info("Expected exception: "+ex);
        }
    }

    public void test_deadlock_003()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        final String tx2 = "tx2";
        final String tx3 = "tx3";
        // add edges of the form: src WAITS_FOR tgt.
        dag.addEdge(tx0, tx1);
        dag.addEdge(tx1, tx2);
        dag.addEdge(tx2, tx3);
        try {
            dag.addEdge(tx3, tx1);
        	fail("Expecting exception: "+DeadlockException.class);
        }
        catch( DeadlockException ex ) {
        	log.info("Expected exception: "+ex);
        }
    }

    public void test_deadlock_batch_001()
    {
    	final int CAPACITY = 5;
        final TxDag dag = new TxDag( CAPACITY );
        final String tx0 = "tx0";
        final String tx1 = "tx1";
        final String tx2 = "tx2";
        final String tx3 = "tx3";
        // add edges of the form: src WAITS_FOR {tgt}.
//        dag.addEdges(tx0, new Object[]{tx3,tx1,tx2});
        dag.addEdge(tx0,tx3);
        dag.addEdge(tx0,tx1);
        dag.addEdge(tx0,tx2);
        assertSameEdges(new Edge[] { new Edge(tx0, tx3, true),
				new Edge(tx0, tx1, true), new Edge(tx0, tx2, true) }, dag
				.getEdges(false));
        try {
            dag.addEdges(tx3, new Object[]{tx0});
//        	System.err.println(""+dag);
//        	dag.addEdge(tx3, tx0);
//        	System.err.println(""+dag);
        	fail("Expecting exception: "+DeadlockException.class);
        }
        catch( DeadlockException ex ) {
        	log.info("Expected exception: "+ex);
        }
    }

    /**
	 * <p>
	 * Test adds N random edges to the graph and then removes them and verifies
	 * that removal correctly reproduces each intermediate state following an
	 * edge addition. Edges are added until a deadlock results. We verify that
	 * the deadlock did not update the internal matrix M and then backup state
	 * by state removing each edge in the reverse order and verifying that the
	 * correct state is reproduced as the edge is removed.
	 * </p>
	 * <p>
	 * Note: This test uses "small" matrices (20 vertices) to keep the memory
	 * footprint down since it makes a copy of the state of the DAG after each
	 * action.  As the capacity of the graph goes up, this test will begin to
	 * stress the garbage collector so more trials and moderate capacity makes
	 * more sense.
	 * </p>
	 * 
	 * @see #doSymmetricOperationsTest(Random, TxDag)
	 */
    
    public void testSymmetricOperations()
    {
    	final int NTRIALS = 30;
    	final int CAPACITY = 20;
    	Random r = new Random();
    	for( int i=0; i<NTRIALS; i++ ) {
    		doSymmetricOperationsTest( r, new TxDag( CAPACITY ) );
    	}
    }

}
