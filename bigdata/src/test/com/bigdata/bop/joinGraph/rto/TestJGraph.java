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
 * Created on Jan 18, 2011
 */

package com.bigdata.bop.joinGraph.rto;

import junit.framework.TestCase2;

/**
 * Test suite for {@link JGraph}, which is the core implementation of the
 * runtime query optimizer logic.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There are some operations which depend on equality or hash code
 *       behavior for vertices and perhaps edges so those things should also be
 *       tested.
 * 
 * @todo Test (re-)sampling for a vertex.
 * 
 * @todo Test sampling of the initial edge(s), including when there are
 *       constraints which can be applied to those edges and when one of the
 *       edges does not have any solutions (true zero).
 * 
 * @todo Test sampling of an edge when extending a path and resampling of the
 *       path.
 */
public class TestJGraph extends TestCase2 {

    /**
     * 
     */
    public TestJGraph() {
    }

    /**
     * @param name
     */
    public TestJGraph(String name) {
        super(name);
    }

    public void test_something() {
        
        fail("write tests");
        
    }

//    /**
//     * Test ability to recognize when there is a predicate without any shared
//     * variables.
//     */
//    public void test_noSharedVariables() {
//        fail("write test");
//    }


    
//    public void test_getMinimumCardinalityEdge() {
//        fail("write test");
//    }
//
//    public void test_moreEdgesToExecute() {
//        fail("write test");
//    }
//
//    // and also getEdgeCount()
//    public void test_getEdges() {
//        fail("write test");
//    }
//    
//    public void test_getSelectedJoinPath() {
//        fail("write test");
//    }
//
//    public void test_getBestAlternativeJoinPath() {
//        fail("write test");
//    }
//
//    public void test_getVertex() {
//        fail("write test");
//    }
//    
//    // getEdge(v1,v2)
//    public void test_getEdge() {
//        fail("write test");
//    }
//    
//    // test ability to obtain a Path which extends another path.
//    public void test_Path_addEdge() {
//        fail("write test");
//    }

//    @Override
//    public Properties getProperties() {
//
//        final Properties p = new Properties(super.getProperties());
//
//        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//                .toString());
//
//        return p;
//        
//    }
//
//    static private final String namespace = "ns";
//    
//    Journal jnl;
//    
//    R rel;
//    
//    public void setUp() throws Exception {
//        
//        jnl = new Journal(getProperties());
//
//    }
//    
//    /**
//     * Create and populate relation in the {@link #namespace}.
//     * 
//     * @return The #of distinct entries.
//     */
//    private int loadData(final int scale) {
//
//      final String[] names = new String[] { "John", "Mary", "Saul", "Paul",
//              "Leon", "Jane", "Mike", "Mark", "Jill", "Jake", "Alex", "Lucy" };
//
//      final Random rnd = new Random();
//      
//      // #of distinct instances of each name.
//      final int populationSize = Math.max(10, (int) Math.ceil(scale / 10.));
//      
//      // #of trailing zeros for each name.
//      final int nzeros = 1 + (int) Math.ceil(Math.log10(populationSize));
//      
////        System.out.println("scale=" + scale + ", populationSize="
////                + populationSize + ", nzeros=" + nzeros);
//
//      final NumberFormat fmt = NumberFormat.getIntegerInstance();
//      fmt.setMinimumIntegerDigits(nzeros);
//      fmt.setMaximumIntegerDigits(nzeros);
//      fmt.setGroupingUsed(false);
//      
//        // create the relation.
//        final R rel = new R(jnl, namespace, ITx.UNISOLATED, new Properties());
//        rel.create();
//
//        // data to insert.
//      final E[] a = new E[scale];
//
//      for (int i = 0; i < scale; i++) {
//
//          final String n1 = names[rnd.nextInt(names.length)]
//                  + fmt.format(rnd.nextInt(populationSize));
//
//          final String n2 = names[rnd.nextInt(names.length)]
//                  + fmt.format(rnd.nextInt(populationSize));
//
////            System.err.println("i=" + i + ", n1=" + n1 + ", n2=" + n2);
//          
//          a[i] = new E(n1, n2);
//          
//        }
//
//      // sort before insert for efficiency.
//      Arrays.sort(a,R.primaryKeyOrder.getComparator());
//      
//        // insert data (the records are not pre-sorted).
//        final long ninserts = rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));
//
//        // Do commit since not scale-out.
//        jnl.commit();
//
//        // should exist as of the last commit point.
//        this.rel = (R) jnl.getResourceLocator().locate(namespace,
//                ITx.READ_COMMITTED);
//
//        assertNotNull(rel);
//
//        return (int) ninserts;
//        
//    }
//
//    public void tearDown() throws Exception {
//
//        if (jnl != null) {
//            jnl.destroy();
//            jnl = null;
//        }
//        
//        // clear reference.
//        rel = null;
//
//    }

//    public void test_something() {

////        final int scale = 10000;
////        
////        final int nrecords = loadData(scale);
//        
//        final IVariable<?> x = Var.var("x");
//
//      final IVariable<?> y = Var.var("y");
//
//      final IPredicate<E> p1 = new Predicate<E>(new BOp[] { x, y },
//              new NV(IPredicate.Annotations.RELATION_NAME,
//                      new String[] { namespace }),//
//              new NV(IPredicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED)//
//      );
//
//      final IPredicate<E> p2 = new Predicate<E>(new BOp[] { x, y },
//              new NV(IPredicate.Annotations.RELATION_NAME,
//                      new String[] { namespace }),//
//              new NV(IPredicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED)//
//      );
//
//      final IPredicate<E> p3 = new Predicate<E>(new BOp[] { x, y },
//              new NV(IPredicate.Annotations.RELATION_NAME,
//                      new String[] { namespace }),//
//              new NV(IPredicate.Annotations.TIMESTAMP, ITx.READ_COMMITTED)//
//      );
//
//      new JoinGraph(//
//              new NV(BOp.Annotations.BOP_ID, 1),//
//              new NV(JoinGraph.Annotations.VERTICES,new IPredicate[]{}),//
//              new NV(JoinGraph.Annotations.SAMPLE_SIZE, 100)//
//              );
//}

}
