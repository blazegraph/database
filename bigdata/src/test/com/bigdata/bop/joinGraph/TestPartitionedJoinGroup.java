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
 * Created on Jan 19, 2011
 */

package com.bigdata.bop.joinGraph;

import java.util.Arrays;
import java.util.Iterator;

import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.MathExpr.MathOp;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Unit tests for {@link PartitionedJoinGroup}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPartitionedJoinGroup extends TestCase2 {

    /**
     * 
     */
    public TestPartitionedJoinGroup() {
    }

    /**
     * @param name
     */
    public TestPartitionedJoinGroup(String name) {
        super(name);
    }

    public void test_ctor_correctRejection() {

        // null source predicate[].
        try {
            new PartitionedJoinGroup(null/* sourcePreds */, null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // empty source predicate[].
        try {
            new PartitionedJoinGroup(new IPredicate[0], null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // null element in the source predicate[].
        try {
            new PartitionedJoinGroup(new IPredicate[1], null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * A test based loosely on LUBM Q2. There are no RDF specific constructions
     * used here.
     */
    public void test_requiredJoins() {
       
        final String rdfType = "rdfType";
        final String graduateStudent = "graduateStudent";
        final String university = "university";
        final String department = "department";
        final String memberOf = "memberOf";
        final String subOrganizationOf = "subOrganizationOf";
        final String undergraduateDegreeFrom = "undergraduateDegreeFrom";

        final IPredicate<?>[] preds;
        final IPredicate<?> p0, p1, p2, p3, p4, p5;
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");
        {

            // The name space for the SPO relation.
            final String[] relation = new String[] { "spo" };

            final long timestamp = System.currentTimeMillis();

            int nextId = 0;

            // ?x a ub:GraduateStudent .
            p0 = new Predicate(new BOp[] { x,
                    new Constant<String>(rdfType),
                    new Constant<String>(graduateStudent) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?y a ub:University .
            p1 = new Predicate(new BOp[] { y,
                    new Constant<String>(rdfType),
                    new Constant<String>(university) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?z a ub:Department .
            p2 = new Predicate(new BOp[] { z,
                    new Constant<String>(rdfType),
                    new Constant<String>(department) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?x ub:memberOf ?z .
            p3 = new Predicate(new BOp[] { x,
                    new Constant<String>(memberOf), z },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?z ub:subOrganizationOf ?y .
            p4 = new Predicate(new BOp[] { z,
                    new Constant<String>(subOrganizationOf), y },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?x ub:undergraduateDegreeFrom ?y
            p5 = new Predicate(new BOp[] { x,
                    new Constant<String>(undergraduateDegreeFrom), y },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // the vertices of the join graph (the predicates).
            preds = new IPredicate[] { p0, p1, p2, p3, p4, p5 };
        }        

        // Test w/o any constraints.
        {

            final IConstraint[] constraints = new IConstraint[] {

            };

            final PartitionedJoinGroup fixture = new PartitionedJoinGroup(
                    preds, constraints);

            // all variables are bound within the join graph.
            assertSameIteratorAnyOrder("joinGraphVars", new IVariable[] { x, y,
                    z }, fixture.getJoinGraphVars().iterator());

            // verify all predicates were placed into the join graph.
            assertSameIteratorAnyOrder("joinGraph", preds, Arrays.asList(
                    fixture.getJoinGraph()).iterator());

            // there are no constraints.
            assertEquals("joinGraphConstraints.size", new IConstraint[] {},
                    fixture.getJoinGraphConstraints());

            // there is no tail plan.
            assertEquals("tailPlan", new IPredicate[] {}, fixture.getTailPlan());
       
        }

        // Test w/ constraint(s) on the join graph.
        {

            final IConstraint c1 = new NEConstant(x,
                    new Constant<String>("Bob"));
            
            final IConstraint c2 = new NEConstant(y,
                    new Constant<String>("UNCG"));

            final IConstraint[] constraints = new IConstraint[] { c1, c2 };

            final PartitionedJoinGroup fixture = new PartitionedJoinGroup(
                    preds, constraints);

            // all variables are bound within the join graph.
            assertSameIteratorAnyOrder("joinGraphVars", new IVariable[] { x, y,
                    z }, fixture.getJoinGraphVars().iterator());

            // verify all predicates were placed into the join graph.
            assertSameIteratorAnyOrder("joinGraph", preds, Arrays.asList(
                    fixture.getJoinGraph()).iterator());

            // verify all constraints were place on the join graph.
            assertSameIteratorAnyOrder("joinGraphConstraints", constraints,
                    Arrays.asList(fixture.getJoinGraphConstraints()).iterator());

            /*
             * Verify the placement of each constraint for a variety of join
             * paths.
             */
            {
//                final int[] pathIds = BOpUtility.getPredIds(new IPredicate[] {
//                        p0, p1, p2, p3, p4, p5 });
//                final IConstraint[] actual = fixture
//                        .getJoinGraphConstraints(pathIds);
//                System.out.println(Arrays.toString(actual));

                // c1 is applied when x is bound. x is bound by p0.
                assertEquals(new IConstraint[] { c1 }, fixture
                        .getJoinGraphConstraints(new int[] { p1.getId(),
                                p0.getId() }));

                /*
                 * c1 is applied when x is bound. x is bound by p0. p0 is the
                 * last predicate in this join path, so c1 is attached to p0.
                 */
                assertEquals(new IConstraint[] { c1 }, fixture
                        .getJoinGraphConstraints(new int[] { p0.getId()}));

                /*
                 * c2 is applied when y is bound. y is bound by p1. p1 is the
                 * last predicate in this join path, p1 is the last predicate in
                 * this join path so c2 is attached to p1.
                 */
                assertEquals(new IConstraint[] { c2 }, fixture
                        .getJoinGraphConstraints(new int[] { p0.getId(),
                                p1.getId() }));
                
            }
            
            // there is no tail plan.
            assertEquals("tailPlan", new IPredicate[] {}, fixture.getTailPlan());
       
        }

    }

    /**
     * A test when there are optional joins involved. In this test, we again
     * start with LUBM Q2, but the predicates which would bind <code>z</code>
     * are both marked as optional. This should shift the constraint on [z] into
     * the tail plan as well.
     */
    public void test_withOptionalJoins() {
        
        final String rdfType = "rdfType";
        final String graduateStudent = "graduateStudent";
        final String university = "university";
        final String department = "department";
        final String memberOf = "memberOf";
        final String subOrganizationOf = "subOrganizationOf";
        final String undergraduateDegreeFrom = "undergraduateDegreeFrom";

        final IPredicate<?>[] preds;
        final IPredicate<?> p0, p1, p2, p3, p4, p5;
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");
        {

            // The name space for the SPO relation.
            final String[] relation = new String[] { "spo" };

            final long timestamp = System.currentTimeMillis();

            int nextId = 0;

            // ?x a ub:GraduateStudent .
            p0 = new Predicate(new BOp[] { x,
                    new Constant<String>(rdfType),
                    new Constant<String>(graduateStudent) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?y a ub:University .
            p1 = new Predicate(new BOp[] { y,
                    new Constant<String>(rdfType),
                    new Constant<String>(university) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?z a ub:Department .  (optional)
            p2 = new Predicate(new BOp[] { z,
                    new Constant<String>(rdfType),
                    new Constant<String>(department) },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.OPTIONAL, true),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?x ub:memberOf ?z . (optional).
            p3 = new Predicate(new BOp[] { x,
                    new Constant<String>(memberOf), z },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.OPTIONAL, true),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?z ub:subOrganizationOf ?y . (optional).
            p4 = new Predicate(new BOp[] { z,
                    new Constant<String>(subOrganizationOf), y },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.OPTIONAL, true),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // ?x ub:undergraduateDegreeFrom ?y
            p5 = new Predicate(new BOp[] { x,
                    new Constant<String>(undergraduateDegreeFrom), y },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, relation)//
            );

            // the vertices of the join graph (the predicates).
            preds = new IPredicate[] { p0, p1, p2, p3, p4, p5 };
        }        

        // Test w/o any constraints.
        {

            final IConstraint[] constraints = new IConstraint[] {

            };

            final PartitionedJoinGroup fixture = new PartitionedJoinGroup(
                    preds, constraints);

            // only {x,y} are bound within the join graph.
            assertSameIteratorAnyOrder("joinGraphVars",
                    new IVariable[] { x, y }, fixture.getJoinGraphVars()
                            .iterator());

            // verify predicates placed into the join graph.
            assertSameIteratorAnyOrder("joinGraph", new IPredicate[] { p0, p1,
                    p5 }, Arrays.asList(fixture.getJoinGraph())
                    .iterator());

            // there are no constraints on the join graph predicates.
            assertEquals("joinGraphConstraints.size", 0, fixture
                    .getJoinGraphConstraints().length);

            // {p2, p3,p4} are in the tail plan.
            assertEquals("tailPlan", new IPredicate[] { p2, p3, p4 }, fixture
                    .getTailPlan());

            // no constraints were assigned to optional predicate [p2].
            assertEquals("", 0,
                    fixture.getTailPlanConstraints(p2.getId()).length);

            // no constraints were assigned to optional predicate [p3].
            assertEquals("", 0,
                    fixture.getTailPlanConstraints(p3.getId()).length);

            // no constraints were assigned to optional predicate [p4].
            assertEquals("", 0,
                    fixture.getTailPlanConstraints(p4.getId()).length);

        }

        // Test w/ constraint(s) on the join graph.
        {

            final IConstraint c1 = new NEConstant(x,
                    new Constant<String>("Bob"));
            
            final IConstraint c2 = new NEConstant(y,
                    new Constant<String>("UNCG"));

            final IConstraint c3 = new NEConstant(z,
                    new Constant<String>("Physics"));

            final IConstraint[] constraints = new IConstraint[] { c1, c2, c3 };

            final PartitionedJoinGroup fixture = new PartitionedJoinGroup(
                    preds, constraints);

            // only {x,y} are bound within the join graph.
            assertSameIteratorAnyOrder("joinGraphVars",
                    new IVariable[] { x, y }, fixture.getJoinGraphVars()
                            .iterator());

            // verify predicates placed into the join graph.
            assertSameIteratorAnyOrder("joinGraph", new IPredicate[] { p0, p1,
                    p5 }, Arrays.asList(fixture.getJoinGraph())
                    .iterator());

            // verify constraints on the join graph.
            assertSameIteratorAnyOrder("joinGraphConstraints",
                    new IConstraint[] { c1, c2 }, Arrays.asList(
                            fixture.getJoinGraphConstraints()).iterator());

            // {p2,p3,p4} are in the tail plan.
            assertEquals("tailPlan", new IPredicate[] { p2, p3, p4 }, fixture
                    .getTailPlan());

            // no constraints were assigned to optional predicate [p2].
            assertEquals("", new IConstraint[] {}, fixture
                    .getTailPlanConstraints(p2.getId()));

            // no constraints were assigned to optional predicate [p3].
            assertEquals("", new IConstraint[] {}, fixture
                    .getTailPlanConstraints(p3.getId()));

            // the constraint on [z] was assigned to optional predicate [p4].
            assertEquals("", new IConstraint[] { c3 }, fixture
                    .getTailPlanConstraints(p4.getId()));

        }

    }
    
    /**
     * @todo test with headPlan.
     */
    public void test_something() {
        fail("write tests");
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * 
     * @todo raise into the AbstractTestCase (e.g, TestCase2/3).
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final Object[] expected,
            final Iterator actual) {

        assertSameIteratorAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final String msg,
            final Object[] expected, final Iterator actual) {

        // Populate a map that we will use to realize the match and
        // selection without replacement logic.

        final int nrange = expected.length;

        java.util.Map range = new java.util.HashMap();

        for (int j = 0; j < nrange; j++) {

            range.put(expected[j], expected[j]);

        }

        // Do selection without replacement for the objects visited by
        // iterator.

        for (int j = 0; j < nrange; j++) {

            if (!actual.hasNext()) {

                fail(msg + ": Index exhausted while expecting more object(s)"
                        + ": index=" + j);

            }

            Object actualObject = actual.next();

            if (range.remove(actualObject) == null) {

                fail("Object not expected" + ": index=" + j + ", object="
                        + actualObject);

            }

        }

        if (actual.hasNext()) {

            fail("Iterator will deliver too many objects.");

        }

    }

}
