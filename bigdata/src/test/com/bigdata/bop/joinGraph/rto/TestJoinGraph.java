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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.joinGraph.rto;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.constraint.NEConstant;

/**
 * Unit tests for the {@link JoinGraph} operator.
 * <p>
 * Note: A lot of the guts the {@link JoinGraph} operator evaluation are tested
 * by {@link TestJGraph}. There is also an RDF specific test suite which runs
 * against various RDF data sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJoinGraph extends TestCase2 {

    /**
     * 
     */
    public TestJoinGraph() {
    }

    /**
     * @param name
     */
    public TestJoinGraph(String name) {
        super(name);
    }

    public void test_ctor() {

        // correct acceptance.
        {
            final IPredicate[] vertices = new IPredicate[] {
                    new Predicate(new BOp[]{Var.var("x"),Var.var("y")}),//
                    new Predicate(new BOp[]{Var.var("y"),Var.var("z")}),//
            };
            final IConstraint[] constraints = null;
            final JoinGraph joinGraph = new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            assertEquals("vertices", vertices, joinGraph.getVertices());
            assertEquals("constraints", constraints, joinGraph.getConstraints());
            assertEquals("limit", JoinGraph.Annotations.DEFAULT_LIMIT,
                    joinGraph.getLimit());
            assertEquals("nedges", JoinGraph.Annotations.DEFAULT_NEDGES,
                    joinGraph.getNEdges());
        }

        // correct acceptance, different arguments.
        {
            final IPredicate[] vertices = new IPredicate[] {
                    new Predicate(new BOp[]{Var.var("x"),Var.var("y")}),//
                    new Predicate(new BOp[]{Var.var("y"),Var.var("z")}),//
            };
            final IConstraint[] constraints = new IConstraint[] { //
            new NEConstant(Var.var("x"), new Constant<Long>(12L)) //
            };
            final JoinGraph joinGraph = new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    new NV(JoinGraph.Annotations.CONSTRAINTS, constraints),//
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            assertEquals("vertices", vertices, joinGraph.getVertices());
            assertEquals("constraints", constraints, joinGraph.getConstraints());
            assertEquals("limit", JoinGraph.Annotations.DEFAULT_LIMIT,
                    joinGraph.getLimit());
            assertEquals("nedges", JoinGraph.Annotations.DEFAULT_NEDGES,
                    joinGraph.getNEdges());
        }

        // correct acceptance, different arguments.
        {
            final IPredicate[] vertices = new IPredicate[] {
                    new Predicate(new BOp[]{Var.var("x"),Var.var("y")}),//
                    new Predicate(new BOp[]{Var.var("y"),Var.var("z")}),//
            };
            final IConstraint[] constraints = new IConstraint[] { //
            new NEConstant(Var.var("x"), new Constant<Long>(12L)) //
            };
            final int limit = 50;
            final int nedges = 1;
            final JoinGraph joinGraph = new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    new NV(JoinGraph.Annotations.CONSTRAINTS, constraints),//
                    new NV(JoinGraph.Annotations.LIMIT, limit),//
                    new NV(JoinGraph.Annotations.NEDGES, nedges),//
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            assertEquals("vertices", vertices, joinGraph.getVertices());
            assertEquals("constraints", constraints, joinGraph.getConstraints());
            assertEquals("limit", limit, joinGraph.getLimit());
            assertEquals("nedges", nedges, joinGraph.getNEdges());
        }

    }
    
    public void test_ctor_correct_rejection() {
        
        /*
         * Correct rejection when required argument is not given (the VERTICES
         * annotation is required).
         */
        try {
            new JoinGraph(new BOp[0], //
                    new NV("foo", "bar"), //
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // correct rejection when vertices array is null.
        try {
            final IPredicate[] vertices = null;
            new JoinGraph(new BOp[0], //
                    new NV(JoinGraph.Annotations.VERTICES, vertices), //
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // correct rejection when vertices array is empty.
        try {
            final IPredicate[] vertices = new IPredicate[] {

            };
            new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // correct rejection when CONTROLLER is not specified.
        try {
            final IPredicate[] vertices = new IPredicate[] {
                    new Predicate(new BOp[]{Var.var("x"),Var.var("y")}),//
                    new Predicate(new BOp[]{Var.var("y"),Var.var("z")}),//
            };
            new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    //new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER)//
            );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
        // correct rejection when EVALUATION_CONTEXT is not specified/wrong.
        try {
            final IPredicate[] vertices = new IPredicate[] {
                    new Predicate(new BOp[]{Var.var("x"),Var.var("y")}),//
                    new Predicate(new BOp[]{Var.var("y"),Var.var("z")}),//
            };
            new JoinGraph(new BOp[0],//
                    new NV(JoinGraph.Annotations.VERTICES, vertices),//
                    new NV(JoinGraph.Annotations.CONTROLLER, true), //
                    new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.ANY)//
            );
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // Correct rejection [limit].
        {
            try {
                final IPredicate[] vertices = new IPredicate[] {
                        new Predicate(new BOp[] { Var.var("x"), Var.var("y") }),//
                        new Predicate(new BOp[] { Var.var("y"), Var.var("z") }),//
                };
                final IConstraint[] constraints = new IConstraint[] { //
                new NEConstant(Var.var("x"), new Constant<Long>(12L)) //
                };
                final int limit = 0;
                final int nedges = 1;
                new JoinGraph(new BOp[0],//
                        new NV(JoinGraph.Annotations.VERTICES, vertices),//
                        new NV(JoinGraph.Annotations.CONSTRAINTS, constraints),//
                        new NV(JoinGraph.Annotations.LIMIT, limit),//
                        new NV(JoinGraph.Annotations.NEDGES, nedges),//
                        new NV(JoinGraph.Annotations.CONTROLLER, true), //
                        new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER)//
                );
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
        }

        // Correct rejection [nedges].
        {
            try {
                final IPredicate[] vertices = new IPredicate[] {
                        new Predicate(new BOp[] { Var.var("x"), Var.var("y") }),//
                        new Predicate(new BOp[] { Var.var("y"), Var.var("z") }),//
                };
                final IConstraint[] constraints = new IConstraint[] { //
                new NEConstant(Var.var("x"), new Constant<Long>(12L)) //
                };
                final int limit = 10;
                final int nedges = 0;
                new JoinGraph(new BOp[0],//
                        new NV(JoinGraph.Annotations.VERTICES, vertices),//
                        new NV(JoinGraph.Annotations.CONSTRAINTS, constraints),//
                        new NV(JoinGraph.Annotations.LIMIT, limit),//
                        new NV(JoinGraph.Annotations.NEDGES, nedges),//
                        new NV(JoinGraph.Annotations.CONTROLLER, true), //
                        new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER)//
                );
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
        }

    }

}
