/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 27, 2010
 */

package com.bigdata.bop;

import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link BOpUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBOpUtility extends TestCase2 {

    /**
     * 
     */
    public TestBOpUtility() {
    }

    /**
     * @param name
     */
    public TestBOpUtility(String name) {
        super(name);
    }

    /**
     * Unit test for {@link BOpUtility#getArgumentVariables(BOp)} and
     * {@link BOpUtility#getArgumentVariableCount(BOp)}.
     * 
     * @todo unit test for iterator over arguments first, then over annotations
     *       which are bops, then over bop with annotations and finally over
     *       depth first recursion of (bop) and (bop+annotation bops).
     *       Everything else should just build on that. Declare what order is
     *       obeyed by those iterators, e.g., visitation order is pre-order or
     *       post-order for the argument hierarchy.
     * 
     * @todo test methods which do recursion to verify annotations handled if
     *       null or never null.
     */
    public void test_getArgumentVariables() {

        {
            final BOp op1 = new BOpBase(new BOp[] { Var.var("y") }, null/* annotations */);

            assertEquals(1, op1.arity());

            assertSameIterator(new Object[] { Var.var("y") }, op1.args()
                    .iterator());

            assertSameIterator(new Object[] { Var.var("y") }, BOpUtility
                    .getArgumentVariables(op1));

            assertEquals(1, BOpUtility.getArgumentVariableCount(op1));
        }

        {
            final BOp op2 = new BOpBase(
                    new BOp[] { Var.var("x"), Var.var("y") }, null/* annotations */);

            assertEquals(2,op2.arity());
            
            assertSameIterator(new Object[] { Var.var("x"), Var.var("y") }, op2
                    .args().iterator());

            assertSameIterator(new Object[] { Var.var("x"), Var.var("y") },
                    BOpUtility.getArgumentVariables(op2));

            assertEquals(2, BOpUtility.getArgumentVariableCount(op2));
        }

        {
            final BOp op3 = new BOpBase(new BOp[] { new Constant<String>("x"),
                    Var.var("y") }, null/* annotations */);

            assertSameIterator(new Object[] { new Constant<String>("x"),
                    Var.var("y") }, op3.args().iterator());

            assertSameIterator(new Object[] { Var.var("y") }, BOpUtility
                    .getArgumentVariables(op3));

            assertEquals(1, BOpUtility.getArgumentVariableCount(op3));

        }
        
    }

    /**
     * Unit test for {@link BOpUtility#preOrderIterator(BOp)}.
     */
    public void test_preOrderIterator() {

        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, null/* annotations */);

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("x"), Var.var("y"), op2 }, null/* annotations */);

        final Object[] expected = new Object[]{//
                root,//
                new Constant<String>("x"),//
                Var.var("y"),//
                op2,//
                Var.var("x"),//
        };
        int i = 0;
        final Iterator<BOp> itr = BOpUtility.preOrderIterator(root);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            System.out.println(i + " : " + t);
            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
                    + t, expected[i].equals(t));
            i++;
        }
        assertEquals(i, expected.length);

        assertSameIterator(expected, BOpUtility.preOrderIterator(root));
       
    }
    
    /**
     * Unit test for {@link BOpUtility#postOrderIterator(BOp)}.
     */
    public void test_postOrderIterator() {

        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, null/* annotations */);

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("x"), Var.var("y"), op2 }, null/* annotations */);

        final Object[] expected = new Object[]{//
                new Constant<String>("x"),//
                Var.var("y"),//
                Var.var("x"),//
                op2,//
                root,//
        };
        int i = 0;
        final Iterator<BOp> itr = BOpUtility.postOrderIterator(root);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            System.out.println(i + " : " + t);
            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
                    + t, expected[i].equals(t));
            i++;
        }
        assertEquals(i, expected.length);

        assertSameIterator(expected, BOpUtility.postOrderIterator(root));
        
    }

    /**
     * Unit test for {@link BOpUtility#annotationOpIterator(BOp)}.
     * <p>
     * Note: This test depends on the LinkedHashMap imposing the ordering in
     * which the annotations are declared.
     */
    public void test_annotationOpIterator() {

        /*
         * Verify that we get an empty iterator for an operator without any
         * annotations.
         */
        {

            final BOp a1 = new BOpBase(new BOp[] { Var.var("a") }, null/* annotations */);

            assertFalse(BOpUtility.annotationOpIterator(a1).hasNext());
            
        }

        /*
         * Verify that we get an empty iterator for an operator without any
         * annotations which are themselves operators.
         */
        {

            final BOp a1 = new BOpBase(new BOp[] { Var.var("a") }, // annotations
                    NV.asMap(new NV[] {//
                            new NV("baz", "3"),//
                            }));

            assertFalse(BOpUtility.annotationOpIterator(a1).hasNext());
            
        }

        /*
         * Verify that we get we visit the annotations which are themselves
         * operators, but not non-operator annotations.
         */
        {

            final BOp op = new BOpBase(//
                    // children
                    new BOp[] {},//
                    // annotations
                    NV.asMap(new NV[] {//
                            new NV("foo", Var.var("x")),//
                                    new NV("bar", new Constant<String>("2")),//
                                    new NV("baz", "3"),//
                            }));

            final BOp[] expected = new BOp[] {//
            Var.var("x"),//
                    new Constant<String>("2"),//
            };
            int i = 0;
            final Iterator<BOp> itr = BOpUtility.annotationOpIterator(op);
            while (itr.hasNext()) {
                final BOp t = itr.next();
                System.out.println(i + " : " + t);
                assertTrue("index=" + i + ", expected=" + expected[i]
                        + ", actual=" + t, expected[i].equals(t));
                i++;
            }
            assertEquals(i, expected.length);

            assertSameIterator(expected, BOpUtility.annotationOpIterator(op));

        }

    }

//    /**
//     * Unit test for {@link BOpUtility#annotationOpPreOrderIterator(BOp)}
//     * (pre-order traversal of the operator annotations of the given operator
//     * without recursion through the children of the given operator)).
//     * <p>
//     * Note: This test depends on the LinkedHashMap imposing the ordering in
//     * which the annotations are declared.
//     */
//    public void test_annotationOpPreOrderIterator() {
//
//        final BOp a1 = new BOpBase(new BOp[]{Var.var("a")},null/*annotations*/);
//        final BOp a2 = new BOpBase(new BOp[]{Var.var("b")},null/*annotations*/);
//        // Note: [a3] tests recursion (annotations of annotations).
//        final BOp a3 = new BOpBase(new BOp[] { Var.var("z") }, NV
//                .asMap(
//                        new NV[] { //
//                                new NV("baz", a2),//
//                                new NV("baz2", "skip")//
//                                }//
//                        ));
//        
//        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, NV.asMap(new NV[]{//
//                new NV("foo1",a1),//
//                new NV("foo2",a3),//
//                new NV("foo3", "skip"),//
//        }));
//
//        final Object[] expected = new Object[]{//
//                op2,//
//                a1,//
//                Var.var("a"),//
//                a3,//
//                a2,//
//                Var.var("b"),//
//                Var.var("z"),//
//                Var.var("x"),//
//        };
//        int i = 0;
//        final Iterator<BOp> itr = BOpUtility
//                .annotationOpPreOrderIterator(op2);
//        while (itr.hasNext()) {
//            final BOp t = itr.next();
//            System.out.println(i + " : " + t);// @todo uncomment
////            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
////                    + t, expected[i].equals(t));
//            i++;
//        }
//        assertEquals(i, expected.length);
//
//        assertSameIterator(expected, BOpUtility
//                .annotationOpPreOrderIterator(op2));
//
//    }
    
    /**
     * Unit test for {@link BOpUtility#preOrderIteratorWithAnnotations(BOp)}.
     * <p>
     * Note: This test depends on the LinkedHashMap imposing the ordering in
     * which the annotations are declared.
     */
    public void test_preOrderIteratorWithAnnotations() {

        final BOp a1 = new BOpBase(new BOp[]{Var.var("a")},null/*annotations*/);
        final BOp a2 = new BOpBase(new BOp[]{Var.var("b")},null/*annotations*/);
        // Note: [a3] tests recursion (annotations of annotations).
        final BOp a3 = new BOpBase(new BOp[] { Var.var("z") }, NV
                .asMap(
                        new NV[] { //
                                new NV("baz", a2),//
                                new NV("baz2", "skip")//
                                }//
                        ));
        
        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, NV.asMap(new NV[]{//
                new NV("foo1",a1),//
                new NV("foo2",a3),//
                new NV("foo3", "skip"),//
        }));

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("12"), Var.var("y"), op2 }, null/* annotations */);

        final Object[] expected = new Object[]{//
                root,//
                new Constant<String>("12"),//
                Var.var("y"),//
                op2,//
                a1,//
                Var.var("a"),//
                a3,//
                a2,//
                Var.var("b"),//
                Var.var("z"),//
                Var.var("x"),//
        };
        int i = 0;
        final Iterator<BOp> itr = BOpUtility
                .preOrderIteratorWithAnnotations(root);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            System.out.println(i + " : " + t);
            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
                    + t, expected[i].equals(t));
            i++;
        }
        assertEquals(i, expected.length);

        assertSameIterator(expected, BOpUtility
                .preOrderIteratorWithAnnotations(root));

    }

    /**
     * Unit test for {@link BOpUtility#getSpannedVariables(BOp)}.
     */
    public void test_getSpannedVariables() {

        final BOp a1 = new BOpBase(new BOp[]{Var.var("a")},null/*annotations*/);
        final BOp a2 = new BOpBase(new BOp[]{Var.var("b")},null/*annotations*/);
        // Note: [a3] tests recursion (annotations of annotations).
        final BOp a3 = new BOpBase(new BOp[] { Var.var("z") }, NV
                .asMap(
                        new NV[] { //
                                new NV("baz", a2),//
                                new NV("baz2", "skip")//
                                }//
                        ));
        
        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, NV.asMap(new NV[]{//
                new NV("foo1",a1),//
                new NV("foo2",a3),//
                new NV("foo3", "skip"),//
        }));

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("12"), Var.var("y"), op2 }, null/* annotations */);

        final Object[] expected = new Object[]{//
                Var.var("y"),//
                Var.var("a"),//
                Var.var("b"),//
                Var.var("z"),//
                Var.var("x"),//
        };
        int i = 0;
        final Iterator<IVariable<?>> itr = BOpUtility
                .getSpannedVariables(root);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            System.out.println(i + " : " + t);
            assertTrue("index=" + i + ", expected=" + expected[i] + ", actual="
                    + t, expected[i].equals(t));
            i++;
        }
        assertEquals(i, expected.length);

        assertSameIterator(expected, BOpUtility
                .getSpannedVariables(root));
        
    }

    /**
     * Unit test for {@link BOpUtility#getIndex(BOp)}.
     * 
     * @todo test for correct detection of duplicates.
     */
    public void test_getIndex() {

        final BOp a1 = new BOpBase(new BOp[]{Var.var("a")},NV.asMap(new NV[]{//
                new NV(BOp.Annotations.BOP_ID,1),//
        }));
        final BOp a2 = new BOpBase(new BOp[]{Var.var("b")},NV.asMap(new NV[]{//
                new NV(BOp.Annotations.BOP_ID,2),//
        }));
        // Note: [a3] tests recursion (annotations of annotations).
        final BOp a3 = new BOpBase(new BOp[] { Var.var("z") }, NV
                .asMap(
                        new NV[] { //
                                new NV("baz", a2),//
                                new NV("baz2", "skip")//
                                }//
                        ));
        
        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, NV.asMap(new NV[]{//
                new NV("foo1",a1),//
                new NV("foo2",a3),//
                new NV("foo3", "skip"),//
                new NV(BOp.Annotations.BOP_ID,3),//
        }));

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("12"), Var.var("y"), op2 }, NV.asMap(new NV[]{//
                        new NV(BOp.Annotations.BOP_ID,4),//
                }));

        // index the operator tree.
        final Map<Integer,BOp> map = BOpUtility.getIndex(root);
        
        assertTrue(a1 == map.get(1));
        assertTrue(a2 == map.get(2));
        assertTrue(op2 == map.get(3));
        assertTrue(root == map.get(4));
        assertNull(map.get(5));
        assertFalse(map.containsValue(a3));
        assertEquals(4, map.size());

    }

    /**
     * Unit test for {@link BOpUtility#getIndex(BOp)} in which we verify that
     * it rejects operator trees having duplicate operator ids.
     */
    public void test_getIndex_rejectsDuplicateIds() {

        final BOp op2 = new BOpBase(new BOp[] { Var.var("x") }, NV.asMap(new NV[]{//
                new NV(BOp.Annotations.BOP_ID,4),//
        }));

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("12"), Var.var("y"), op2 }, NV.asMap(new NV[]{//
                        new NV(BOp.Annotations.BOP_ID,4),//
                }));

        try {
            BOpUtility.getIndex(root);
            fail("Expecting: "+DuplicateBOpIdException.class);
        } catch (DuplicateBOpIdException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
    }

    /**
     * Unit test for {@link BOpUtility#getIndex(BOp)} in which we verify that it
     * rejects operator trees operator ids which are not {@link Integer}s.
     */
    public void test_getIndex_rejectsNonIntegerIds() {

        // root
        final BOp root = new BOpBase(new BOp[] { // root args[]
                new Constant<String>("12"), Var.var("y") }, NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, "4"),//
                        }));

        try {
            BOpUtility.getIndex(root);
            fail("Expecting: " + BadBOpIdTypeException.class);
        } catch (BadBOpIdTypeException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

}
