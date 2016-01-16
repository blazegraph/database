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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase;

import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.Chunkerator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Test suite for both {@link HTreeHashJoinUtility} and {@link JVMHashJoinUtility}.
 * 
 * TODO Unit test of optional solutions
 * 
 * TODO Verify application of constraints, but only to the non-optional
 * solutions (so this means a test with optionals and also we need a test w/o
 * constraints).
 * 
 * TODO See {@link TestHTreeHashJoinOp} and some of the other join test suites for
 * some of these variations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractHashJoinUtilityTestCase extends TestCase {
    
    public AbstractHashJoinUtilityTestCase() {
    }
    
    public AbstractHashJoinUtilityTestCase(String name) {
        super(name);
    }

    /**
     * Setup for a problem used by many of the join test suites.
     */
    static public class JoinSetup {

        public final String namespace;

        public final IV<?, ?> brad, john, fred, mary, paul, leon;

        public JoinSetup(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();
            
            this.namespace = namespace;

            brad = makeIV(new LiteralImpl("Brad"));
            
            john = makeIV(new LiteralImpl("John"));

            fred = makeIV(new LiteralImpl("Fred"));

            mary = makeIV(new LiteralImpl("Mary"));

            paul = makeIV(new LiteralImpl("Paul"));

            leon = makeIV(new LiteralImpl("Leon"));
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        /**
         * <pre>
         * {x=brad, y=fred}
         * {x=mary}
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getLeft1() {

            final IVariable<?> x = Var.var("x");
            final IVariable<?> y = Var.var("y");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(brad));
            tmp.set(y, new Constant<IV>(fred));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(x, new Constant<IV>(mary));
            left.add(tmp);

            return left;
        }

        /**
         * <pre>
         * {a=paul, x=mary}
         * {a=paul, x=brad}
         * {a=john, x=mary}
         * {a=john, x=brad}
         * {a=mary, x=brad}
         * {a=brad, x=fred}
         * {a=brad, x=leon}
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getRight1() {

            final IVariable<?> a = Var.var("a");
            final IVariable<?> x = Var.var("x");

            // The right solutions (the hash index).
            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(paul));
            tmp.set(x, new Constant<IV>(mary));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(paul));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(john));
            tmp.set(x, new Constant<IV>(mary));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(john));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(mary));
            tmp.set(x, new Constant<IV>(brad));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(brad));
            tmp.set(x, new Constant<IV>(fred));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(a, new Constant<IV>(brad));
            tmp.set(x, new Constant<IV>(leon));
            right.add(tmp);

            return right;

        }

    }

    /**
     * <pre>
     * @prefix  :       <http://example/> .
     * @prefix  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  foaf:   <http://xmlns.com/foaf/0.1/> .
     * 
     * :alice  rdf:type   foaf:Person .
     * :alice  foaf:name  "Alice" .
     * :bob    rdf:type   foaf:Person .
     * </pre>
     */
    static public class ExistsSetup {

        public final String namespace;

        public final IV<?, ?> alice, bob, rdfType, foafName, foafPerson, aliceLabel;

        public ExistsSetup(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;

            alice = makeIV(new URIImpl("http://example/alice"));

            bob = makeIV(new URIImpl("http://example/bob"));

            rdfType = makeIV(RDF.TYPE);

            foafName = makeIV(FOAFVocabularyDecl.name);

            foafPerson = makeIV(FOAFVocabularyDecl.Person);

            aliceLabel = makeIV(new LiteralImpl("Alice"));
            
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        /**
         * Reading from access path
         * 
         * <pre>
         * ?person rdf:type  foaf:Person
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getLeft1() {

            final IVariable<?> person = Var.var("person");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(person, new Constant<IV>(alice));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(person, new Constant<IV>(bob));
            left.add(tmp);

            return left;
        }

        /**
         * Reading from access path
         * 
         * <pre>
         * ?person foaf:name ?name
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getRight1() {

            final IVariable<?> person = Var.var("person");
            final IVariable<?> name = Var.var("name");

            // The right solutions (the hash index).
            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(person, new Constant<IV>(alice));
            tmp.set(name, new Constant<IV>(aliceLabel));
            right.add(tmp);

            return right;

        }

    }

    /**
     * Setup for NOT EXISTS problem.
     * 
     * <pre>
     * @prefix : <http://example/> .
     * 
     * :a :p 1 ; :q 1, 2 .
     * :b :p 3.0 ; :q 4.0, 5.0 .
     * </pre>
     */
    static public class NotExistsSetup {

        public final String namespace;

        public final IV<?, ?> a, b, one, two, three, four, five;

        public NotExistsSetup(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;

            a = makeIV(new LiteralImpl("a"));

            b = makeIV(new LiteralImpl("b"));

            one = makeIV(new LiteralImpl("1", XSD.INTEGER));

            two = makeIV(new LiteralImpl("2", XSD.INTEGER));

            three = makeIV(new LiteralImpl("3.0", XSD.DECIMAL));

            four = makeIV(new LiteralImpl("4.0", XSD.DECIMAL));

            five = makeIV(new LiteralImpl("5.0", XSD.DECIMAL));
            
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        /**
         * Reading from access path having this data and binding <code>?a</code>
         * and <code>?n</code>
         * 
         * <pre>
         * :a :p 1 .
         * :b :p 3.0 .
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getLeft1() {

            final IVariable<?> avar = Var.var("a");
            final IVariable<?> nvar = Var.var("n");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(a));
            tmp.set(nvar, new Constant<IV>(one));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(b));
            tmp.set(nvar, new Constant<IV>(three));
            left.add(tmp);

            return left;
        }

        /**
         * Reading from access path having this data and binding <code>?a</code>
         * and <code>?m</code>
         * 
         * <pre>
         * :a :q 1 .
         * :a :q 2 .
         * :b :q 4.0 .
         * :b :q 5.0 .
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getRight1(final IVariable<?> ovar) {

            final IVariable<?> avar = Var.var("a");
//            final IVariable<?> mvar = Var.var("m");

            // The right solutions (the hash index).
            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(a));
            tmp.set(ovar, new Constant<IV>(one));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(a));
            tmp.set(ovar, new Constant<IV>(two));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(b));
            tmp.set(ovar, new Constant<IV>(four));
            right.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(avar, new Constant<IV>(b));
            tmp.set(ovar, new Constant<IV>(five));
            right.add(tmp);

            return right;

        }

    }

    @SuppressWarnings("deprecation")
    protected static void assertSameSolutionsAnyOrder(
            final IBindingSet[] expected, final Iterator<IBindingSet> actual) {

        AbstractQueryEngineTestCase.assertSameSolutionsAnyOrder(expected,
                actual);

    }
    
    /**
     * A buffer which absorbs solutions and let's us replay them via an
     * iterator.
     */
    protected static class TestBuffer<E> implements IBuffer<E> {
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private final List<E> a = new LinkedList();
        
        public Iterator<E> iterator() {
            return a.iterator();
        }

        public int size() {
            return a.size();
        }

        public boolean isEmpty() {
            return a.isEmpty();
        }

        public void add(E e) {
            a.add(e);
        }

        public long flush() {
            return 0;
        }

        public void reset() {
        }
        
    }

    protected class MockPipelineOp extends PipelineOp {

        public MockPipelineOp(final BOp[] args, final NV... anns) {

            super(args, NV.asMap(anns));

        };

        private static final long serialVersionUID = 1L;

        @Override
        public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Test helper for required or optional join tests.
     * 
     * @param joinType
     * @param joinVars
     * @param selectVars
     * @param left
     * @param right
     * @param expected
     */
    protected void doHashJoinTest(//
            final JoinTypeEnum joinType,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final List<IBindingSet> left, //
            final List<IBindingSet> right,//
            final IBindingSet[] expected//
            ) {

        // Setup a mock PipelineOp for the test.
        final PipelineOp op = new MockPipelineOp(BOp.NOARGS, 
                new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                        new String[] { getName() }),//
                new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                new NV(JoinAnnotations.SELECT, selectVars),//
                new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                );

        final IHashJoinUtility state = newHashJoinUtility(op, joinType);

        try {

            // Load the right solutions into the HTree.
            {

                final BOpStats stats = new BOpStats();

                state.acceptSolutions(
                        new Chunkerator<IBindingSet>(right.iterator()), stats);

                assertEquals(right.size(), state.getRightSolutionCount());

                assertEquals(right.size(), stats.unitsIn.get());

                // Verify all expected solutions are in the hash index.
                assertSameSolutionsAnyOrder(
                        right.toArray(new IBindingSet[right.size()]),
                        state.indexScan());

            }

            /*
             * Run the hash join.
             */
            {

//                final ICloseableIterator<IBindingSet> leftItr = new CloseableIteratorWrapper<IBindingSet>(
//                        left.iterator());

                final ICloseableIterator<IBindingSet[]> leftItr = new Chunkerator<IBindingSet>(
                        left.iterator(), 100/*chunkSize*/, IBindingSet.class);

                // Buffer used to collect the solutions.
                final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();
                
                // Compute the "required" solutions.
                state.hashJoin(leftItr, null/* stats */, outputBuffer);

                switch (joinType) {
                case Normal:
                    break;
                case Optional:
                case NotExists:
                    // Output the optional solutions.
                    state.outputOptionals(outputBuffer);
                    break;
                case Exists:
                    // Output the join set.
                    state.outputJoinSet(outputBuffer);
                    break;
                default:
                    throw new AssertionError();
                }

                // Verify the expected solutions.
                assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());
                
            }

        } finally {

            state.release();

        }

    }
    
    /**
     * Factory for {@link IHashJoinUtility} instances under test.
     * 
     * @param op
     *            The operator from which much of the state will be initialized.
     * @param joinType
     *            The type of operation to be performed.
     * @return
     */
    abstract protected IHashJoinUtility newHashJoinUtility(PipelineOp op,
            final JoinTypeEnum joinType);

    /**
     * Empty lhs and rhs with non-optional join.
     */
    public void test_hashJoin01() {

        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> left = new LinkedList<IBindingSet>();

        // The right solutions (the hash index).
        final List<IBindingSet> right = new LinkedList<IBindingSet>();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[0];

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }

    /**
     * Empty lhs and rhs with optional join.
     */
    public void test_hashJoin02() {

        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> left = new LinkedList<IBindingSet>();

        // The right solutions (the hash index).
        final List<IBindingSet> right = new LinkedList<IBindingSet>();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[0];
        
        doHashJoinTest(JoinTypeEnum.Optional, joinVars, selectVars,
                constraints, left, right, expected);

    }
    
    /**
     * Non-optional join.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin03() {

        final JoinSetup setup = new JoinSetup(getName());
        
        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{x};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = new IVariable[]{x,y};

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }

    /**
     * Variant of a previous non-optional join with no join variables.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin04() {

        final JoinSetup setup = new JoinSetup(getName());
        
        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = new IVariable[]{x,y};

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { x },//
                        new IConstant[] { new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { x, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }

    /**
     * Variant of the previous non-optional join without select variables.
     */
    @SuppressWarnings("rawtypes")
    public void test_hashJoin05() {

        final JoinSetup setup = new JoinSetup(getName());

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;

        // the join constraints.
        final IConstraint[] constraints = new IConstraint[] { Constraint
                .wrap(new EQConstant(a, new Constant<IV>(setup.john))) };

        // The left solutions (the pipeline).
        final List<IBindingSet> left = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> right = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred),
                                }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }

    /**
     * Unit test of a required MERGE JOIN involving two sources. The join
     * variable is <code>?a</code>. One source has <code>(?a,?x)</code>. The
     * other has <code>(?a,?y)</code>. For this test, all solutions having the
     * same value for the join variable will join (there are no join constraints
     * and the solutions can not conflict as they do not bind the same variables
     * for the non-join variables).
     * <p>
     * The following represents the solutions for each source organized by the
     * join variable <code>?a</code>.
     * <pre>
     * (?a,     ?x)   (?a,     ?y)
     * (john, mary)   (john, brad)   // many-to-many join
     * (john, leon)   (john, fred) 
     *                (john, leon) 
     * (mary, john)   (mary, brad)   // 1-1 join.
     * (fred, brad)                  // does not join (would join if OPTIONAL).   
     *                (brad, fred)   // does not join.
     * (leon, john)   (leon, brad)   // many-1 join.
     * (leon, mary)                  // 
     * (paul, leon)   (paul, leon)   // 1-many join.
     *                (paul, brad)
     * </pre>
     */
    public void test_mergeJoin01() {
        
        final JoinSetup setup = new JoinSetup(getName());

        final boolean optional = false;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // The join variables. This must be the same for each source.
        final IVariable<?>[] joinVars = new IVariable[]{a};

        // The variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;
        
        // The join constraints.
        final IConstraint[] constraints = null;

        /**
         * Setup the solutions for [first].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] firstSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
        };
        assertEquals(7,firstSolutions.length);

        /**
         * Setup the source solutions for [other].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] otherSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.brad) }//
                ),//
        };
        assertEquals(8,otherSolutions.length);
        
        /**
         * The expected solutions to the join.
         * 
         * The source solutions are:
         * <pre>
         * (?a,     ?x)   (?a,     ?y)
         * (john, mary)   (john, brad)   // many-to-many join
         * (john, leon)   (john, fred) 
         *                (john, leon) 
         * (mary, john)   (mary, brad)   // 1-1 join.
         * (fred, brad)                  // does not join.   
         *                (brad, fred)   // does not join.
         * (leon, john)   (leon, brad)   // many-1 join.
         * (leon, mary)                  // 
         * (paul, leon)   (paul, leon)   // 1-many join.
         *                (paul, brad)
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
            // many-to-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            // 1-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // many-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // 1-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
        };

        IHashJoinUtility first = null;
        IHashJoinUtility other = null;
        try {

            // Setup a mock PipelineOp for the test.
            final PipelineOp firstOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp otherOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            first = newHashJoinUtility(firstOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            other = newHashJoinUtility(otherOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            // Load into [first].
            {

                final BOpStats stats = new BOpStats();

                first.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(firstSolutions).iterator()), stats);

                assertEquals(firstSolutions.length, first.getRightSolutionCount());

                assertEquals(firstSolutions.length, stats.unitsIn.get());

            }
            
            // Load into [other].
            {

                final BOpStats stats = new BOpStats();

                other.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(otherSolutions).iterator()), stats);

                assertEquals(otherSolutions.length, other.getRightSolutionCount());

                assertEquals(otherSolutions.length, stats.unitsIn.get());

            }

            // Do the merge join and verify the expected solutions.
            doMergeJoinTest(constraints, expected, optional, first, other);

        } finally {

            if (first != null) {
                first.release();
            }

            if (other != null) {
                other.release();
            }
            
        }
        
    }

    /**
     * Test for optional merge join. This is based on the same data as the
     * non-optional merge join test above.
     */
    public void test_mergeJoin02() {
        final JoinSetup setup = new JoinSetup(getName());

        final boolean optional = true;

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        
        // The join variables. This must be the same for each source.
        final IVariable<?>[] joinVars = new IVariable[]{a};

        // The variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;
        
        // The join constraints.
        final IConstraint[] constraints = null;

        /**
         * Setup the solutions for [first].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] firstSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.john) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
        };
        assertEquals(7,firstSolutions.length);

        /**
         * Setup the source solutions for [other].
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] otherSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.mary),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.brad),
                                          new Constant<IV>(setup.fred) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.leon),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.paul),
                                          new Constant<IV>(setup.brad) }//
                ),//
        };
        assertEquals(8,otherSolutions.length);
        
        /**
         * The expected solutions to the join.
         * 
         * The source solutions are:
         * <pre>
         * (?a,     ?x)   (?a,     ?y)
         * (john, mary)   (john, brad)   // many-to-many join
         * (john, leon)   (john, fred) 
         *                (john, leon) 
         * (mary, john)   (mary, brad)   // 1-1 join.
         * (fred, brad)                  // OPTIONAL JOIN.
         *                (brad, fred)   // does not join.
         * (leon, john)   (leon, brad)   // many-1 join.
         * (leon, mary)                  // 
         * (paul, leon)   (paul, leon)   // 1-many join.
         *                (paul, brad)
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
            // many-to-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.fred)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            // 1-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // OPTIONAL JOIN
            new ListBindingSet(//
                    new IVariable[] { a, x },//
                    new IConstant[] { new Constant<IV>(setup.fred),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // many-to-1 join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.john),
                                      new Constant<IV>(setup.brad)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.mary),
                                      new Constant<IV>(setup.brad)}//
            ),//
            // 1-many join
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.leon)}//
            ),//
            new ListBindingSet(//
                    new IVariable[] { a, x, y },//
                    new IConstant[] { new Constant<IV>(setup.paul),
                                      new Constant<IV>(setup.leon),
                                      new Constant<IV>(setup.brad)}//
            ),//
        };

        IHashJoinUtility first = null;
        IHashJoinUtility other = null;
        try {

            // Setup a mock PipelineOp for the test.
            final PipelineOp firstOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp otherOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars)//
//                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            first = newHashJoinUtility(firstOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            other = newHashJoinUtility(otherOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            // Load into [first].
            {

                final BOpStats stats = new BOpStats();

                first.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(firstSolutions).iterator()), stats);

                assertEquals(firstSolutions.length, first.getRightSolutionCount());

                assertEquals(firstSolutions.length, stats.unitsIn.get());

            }
            
            // Load into [other].
            {

                final BOpStats stats = new BOpStats();

                other.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(otherSolutions).iterator()), stats);

                assertEquals(otherSolutions.length, other.getRightSolutionCount());

                assertEquals(otherSolutions.length, stats.unitsIn.get());

            }

            // Do the merge join and verify the expected solutions.
            doMergeJoinTest(constraints, expected, optional, first, other);

        } finally {

            if (first != null) {
                first.release();
            }

            if (other != null) {
                other.release();
            }
            
        }
        
    }    
    
    /**
     * Test with more than 2 join sets.
     */
    public void test_mergeJoin03_nonOpt() {
    	mergeJoin03(false, false);
    }
    
    public void test_mergeJoin03_opt() {
    	mergeJoin03(true, false);
    }
    
    public void test_mergeJoin03_nonOptConstrain() {
    	mergeJoin03(false, true);
    }
    
    public void test_mergeJoin03_optConstrain() {
    	mergeJoin03(true, true);
    }

    /**
     * 
     * <pre>
     * (?a     ?x) (?a     ?y) (?a     ?z)
     * (john mary) (john brad) (john mary)
     * (john leon) (john fred)
     * (fred leon)
     * </pre>
     * 
     * <pre>
     * (?a     ?x   ?y   ?z)
     * (john mary brad mary) // required join 
     * (john mary fred mary) // required join unless constraint
     * (john leon brad mary) // required join 
     * (john leon fred mary) // required join unless constraint
     * (fred leon ---- ----) // iff optional (regardless of constraint)
     * </pre>
     * 
     * @param optional
     * @param constrain
     *            When <code>true</code>, we constrain <code>?y = brad</code>
     */
    @SuppressWarnings("rawtypes")
    public void mergeJoin03(final boolean optional, final boolean constrain) {
        final JoinSetup setup = new JoinSetup(getName());

        final IVariable<?> a = Var.var("a");
        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");
        
        // The join variables. This must be the same for each source.
        final IVariable<?>[] joinVars = new IVariable[]{a};

        // The variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;
        
        
        // The join constraints.
        final IConstraint[] constraints = constrain ? new IConstraint[] { Constraint
                .wrap(new EQConstant(y, new Constant<IV>(setup.brad))),//
        } : null;

        /**
         * Setup the solutions for [first].
         */
        final IBindingSet[] firstSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.leon) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.leon) }//
                )//
        };
        assertEquals(3,firstSolutions.length);

        /**
         * Setup the source solutions for [other].
         */
        final IBindingSet[] otherSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.brad) }//
                ),//
                new ListBindingSet(//
                        new IVariable[] { a, y },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.fred) }//
                )
        };
        assertEquals(2,otherSolutions.length);
        
        /**
         * Setup the source solutions for [other].
         */
        final IBindingSet[] moreSolutions = new IBindingSet[] {
                new ListBindingSet(//
                        new IVariable[] { a, z },//
                        new IConstant[] { new Constant<IV>(setup.john),
                                          new Constant<IV>(setup.mary) }//
                ),//
        };
        assertEquals(1,moreSolutions.length);
        
        /**
         * The expected solutions to the join.
         * 
         * The source solutions are:
         * <pre>
         * (?a,     ?x)   (?a,     ?y)   (?a,     ?z)
         * (john, mary)   (john, brad)   (john, mary)// many-to-many join
         * (john, leon)   (john, fred) 
         * (fred, lyon)
         * </pre>
         */
        final IBindingSet[] expected;
        if (optional) {
        	if (constraints == null) {
	        	expected = new IBindingSet[] {//
	            // many-to-many join
	            new ListBindingSet(//
	                    new IVariable[] { a, x, y, z },//
	                    new IConstant[] { new Constant<IV>(setup.john),
	                                      new Constant<IV>(setup.mary),
	                                      new Constant<IV>(setup.brad),
	                                      new Constant<IV>(setup.mary)}//
	            ),//
	            new ListBindingSet(//
	                    new IVariable[] { a, x, y, z },//
	                    new IConstant[] { new Constant<IV>(setup.john),
	                                      new Constant<IV>(setup.mary),
	                                      new Constant<IV>(setup.fred),
	                                      new Constant<IV>(setup.mary)}//
	            ),//
	            new ListBindingSet(//
	                    new IVariable[] { a, x, y, z },//
	                    new IConstant[] { new Constant<IV>(setup.john),
	                                      new Constant<IV>(setup.leon),
	                                      new Constant<IV>(setup.brad),
	                                      new Constant<IV>(setup.mary)}//
	            ),//
	            new ListBindingSet(//
	                    new IVariable[] { a, x, y, z },//
	                    new IConstant[] { new Constant<IV>(setup.john),
	                                      new Constant<IV>(setup.leon),
	                                      new Constant<IV>(setup.fred),
	                                      new Constant<IV>(setup.mary)}//
	            ),//
                new ListBindingSet(//
                        new IVariable[] { a, x },//
                        new IConstant[] { new Constant<IV>(setup.fred),
                                          new Constant<IV>(setup.leon)})
	        };
        	} else {
	        	expected = new IBindingSet[] {//
	    	            // many-to-many join
	    	            new ListBindingSet(//
	    	                    new IVariable[] { a, x, y, z },//
	    	                    new IConstant[] { new Constant<IV>(setup.john),
	    	                                      new Constant<IV>(setup.mary),
	    	                                      new Constant<IV>(setup.brad),
	    	                                      new Constant<IV>(setup.mary)}//
	    	            ),//
	    	            new ListBindingSet(//
	    	                    new IVariable[] { a, x, y, z },//
	    	                    new IConstant[] { new Constant<IV>(setup.john),
	    	                                      new Constant<IV>(setup.leon),
	    	                                      new Constant<IV>(setup.brad),
	    	                                      new Constant<IV>(setup.mary)}//
	    	            ),//
	                    new ListBindingSet(//
	                            new IVariable[] { a, x },//
	                            new IConstant[] { new Constant<IV>(setup.fred),
	                                              new Constant<IV>(setup.leon)})
	        	};
        	}
        } else {
        	if (constraints == null) {
        	expected = new IBindingSet[] {//
                    // many-to-many join
                    new ListBindingSet(//
                            new IVariable[] { a, x, y, z },//
                            new IConstant[] { new Constant<IV>(setup.john),
                                              new Constant<IV>(setup.mary),
                                              new Constant<IV>(setup.brad),
                                              new Constant<IV>(setup.mary)}//
                    ),//
                    new ListBindingSet(//
                            new IVariable[] { a, x, y, z },//
                            new IConstant[] { new Constant<IV>(setup.john),
                                              new Constant<IV>(setup.mary),
                                              new Constant<IV>(setup.fred),
                                              new Constant<IV>(setup.mary)}//
                    ),//
                    new ListBindingSet(//
                            new IVariable[] { a, x, y, z },//
                            new IConstant[] { new Constant<IV>(setup.john),
                                              new Constant<IV>(setup.leon),
                                              new Constant<IV>(setup.brad),
                                              new Constant<IV>(setup.mary)}//
                    ),//
                    new ListBindingSet(//
                            new IVariable[] { a, x, y, z },//
                            new IConstant[] { new Constant<IV>(setup.john),
                                              new Constant<IV>(setup.leon),
                                              new Constant<IV>(setup.fred),
                                              new Constant<IV>(setup.mary)}//
                    )
                };
        	} else {
            	expected = new IBindingSet[] {//
                        // many-to-many join
                        new ListBindingSet(//
                                new IVariable[] { a, x, y, z },//
                                new IConstant[] { new Constant<IV>(setup.john),
                                                  new Constant<IV>(setup.mary),
                                                  new Constant<IV>(setup.brad),
                                                  new Constant<IV>(setup.mary)}//
                        ),//
                        new ListBindingSet(//
                                new IVariable[] { a, x, y, z },//
                                new IConstant[] { new Constant<IV>(setup.john),
                                                  new Constant<IV>(setup.leon),
                                                  new Constant<IV>(setup.brad),
                                                  new Constant<IV>(setup.mary)}//
                        )
                    };
        	}
        }

        IHashJoinUtility first = null;
        IHashJoinUtility other = null;
        IHashJoinUtility more = null;
        try {

            // Setup a mock PipelineOp for the test.
            final PipelineOp firstOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars),//
                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp otherOp = new MockPipelineOp(BOp.NOARGS, 
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars),//
                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            // Setup a mock PipelineOp for the test.
            final PipelineOp moreOp = new MockPipelineOp(BOp.NOARGS,
                    new NV(HTreeHashJoinAnnotations.RELATION_NAME,
                            new String[] { getName() }),//
                    new NV(HashJoinAnnotations.JOIN_VARS, joinVars),//
                    new NV(JoinAnnotations.SELECT, selectVars),//
                    new NV(JoinAnnotations.CONSTRAINTS, constraints)//
                    );

            first = newHashJoinUtility(firstOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            other = newHashJoinUtility(otherOp,
                    optional ? JoinTypeEnum.Optional : JoinTypeEnum.Normal);

            more = newHashJoinUtility(moreOp, optional ? JoinTypeEnum.Optional
                    : JoinTypeEnum.Normal);

            // Load into [first].
            {

                final BOpStats stats = new BOpStats();

                first.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(firstSolutions).iterator()), stats);

                assertEquals(firstSolutions.length, first.getRightSolutionCount());

                assertEquals(firstSolutions.length, stats.unitsIn.get());

            }
            
            // Load into [other].
            {

                final BOpStats stats = new BOpStats();

                other.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(otherSolutions).iterator()), stats);

                assertEquals(otherSolutions.length, other.getRightSolutionCount());

                assertEquals(otherSolutions.length, stats.unitsIn.get());

            }

            // Load into [more].
            {

                final BOpStats stats = new BOpStats();

                more.acceptSolutions(new Chunkerator<IBindingSet>(Arrays
                        .asList(moreSolutions).iterator()), stats);

                assertEquals(moreSolutions.length, more.getRightSolutionCount());

                assertEquals(moreSolutions.length, stats.unitsIn.get());

            }

            // Do the merge join and verify the expected solutions.
            doMergeJoinTest(constraints, expected, optional, first, other, more);

        } finally {

            if (first != null) {
                first.release();
            }

            if (other != null) {
                other.release();
            }
            
            if (more != null) {
                more.release();
            }
            
        }
        
    }
    
    /**
     * @param first
     *            The instances from which the required joins will be reported
     *            if the join is OPTIONAL.
     * @param others
     *            The other instances. There must be at least two instances to
     *            be joined when [first] and [others] are considered together.
     * @param constraints
     *            The join constraints.
     * @param expected
     *            The expected solutions.
     */
    protected void doMergeJoinTest(
            final IConstraint[] constraints,//
            final IBindingSet[] expected,//
            final boolean optional,//
            final IHashJoinUtility first,//
            final IHashJoinUtility... others
            ) {

        // Buffer used to collect the solutions.
        final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();

        // Do the merge join,
        first.mergeJoin(others, outputBuffer, constraints, optional);
        
        // Verify the expected solutions.
        assertEquals(expected.length, outputBuffer.size());
        assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());
        
    }

    /**
     * Unit test for EXISTS based on Sesame <code>sparql1-exists-01</code>.
     * 
     * <pre>
     * PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> 
     * 
     * SELECT ?person
     * WHERE 
     * {
     *     ?person rdf:type  foaf:Person .
     *     FILTER EXISTS { ?person foaf:name ?name }
     * }
     * </pre>
     * 
     * <pre>
     * @prefix  :       <http://example/> .
     * @prefix  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
     * @prefix  foaf:   <http://xmlns.com/foaf/0.1/> .
     * 
     * :alice  rdf:type   foaf:Person .
     * :alice  foaf:name  "Alice" .
     * :bob    rdf:type   foaf:Person .
     * </pre>
     * 
     * <pre>
     *     <result>
     *       <binding name="person">
     *         <uri>http://example/alice</uri>
     *       </binding>
     *     </result>
     * </pre>
     */
    public void test_exists_01() {

        final ExistsSetup setup = new ExistsSetup(getName());
        
        final IVariable<?> person = Var.var("person");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{person};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;//new IVariable[] { person };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getRight1();

        // The expected solutions to the join.
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { person},//
                        new IConstant[] { new Constant<IV>(setup.alice) }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.Exists, joinVars, selectVars, constraints,
                left, right, expected);

    }
    
    /**
     * Unit tests for NOT EXISTS based on Sesame <code>sparql11-exists-05</code>
     * .
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?n .
     *     }
     * }
     *  
     * :a :p 1 .
     * :b :p 3.0 .
     * 
     * :a :q 1 .
     * :a :q 2 .
     * :b :q 4.0 .
     * :b :q 5.0 .
     * 
     *     <result>
     *       <binding name="a">
     *         <uri>http://example/b</uri>
     *       </binding>
     *       <binding name="n">
     *         <literal datatype="http://www.w3.org/2001/XMLSchema#decimal">3.0</literal>
     *       </binding>
     *     </result>
     * 
     * </pre>
     */
    public void test_not_exists_01() {

        final NotExistsSetup setup = new NotExistsSetup(getName());
        
        final IVariable<?> avar = Var.var("a");
        final IVariable<?> nvar = Var.var("n");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[]{avar};

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;//new IVariable[] { avar, nvar };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getRight1(nvar);

        // The expected solutions to the join.
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                        new IVariable[] { avar, nvar },//
                        new IConstant[] { new Constant<IV>(setup.b),
                                          new Constant<IV>(setup.three)
                                          }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.NotExists, joinVars, selectVars,
                constraints, left, right, expected);

    }
    
    /*
     * Note: This test has been removed. The inner FILTER needs to be applied to
     * the join which reads from the inner access path. Thus, if we were to
     * model it in this test suite, the filter is effectively inoperative since
     * [?n] is not bound in that scope. The test is actually attaching the
     * FILTER to the MINUS join, which is NOT how the SPARQL query was written.
     * So, this test reduces to exactly the same thing as the one above unless
     * we are testing filter placement and evaluation rather than testing the
     * MINUS join operator.
     */
    
//    /**
//     * Unit tests for NOT EXISTS based on Sesame <code>sparql11-exists-06</code>
//     * . This uses the same data as the previous test (and has the same
//     * solutions), but the query is slightly different and includes a FILTER
//     * inside of the EXISTS graph pattern.
//     * <p>
//     * NOTE: Due to the scope of the variables, <code>?n</code> IS NOT BOUND
//     * inside of the FILTER!
//     * 
//     * <pre>
//     * PREFIX : <http://example/>
//     * SELECT * WHERE {
//     *     ?a :p ?n
//     *     FILTER NOT EXISTS {
//     *         ?a :q ?m .
//     *         FILTER(?n = ?m)
//     *     }
//     * }
//     * 
//     *     <result>
//     *       <binding name="a">
//     *         <uri>http://example/b</uri>
//     *       </binding>
//     *       <binding name="n">
//     *         <literal datatype="http://www.w3.org/2001/XMLSchema#decimal">3.0</literal>
//     *       </binding>
//     *     </result>
//     * 
//     * </pre>
//     * 
//     * @see http://www.w3.org/TR/sparql11-query/#negation, section on inner
//     *      filters.
//     */
//    public void test_not_exists_02() {
//
//        final NotExistsSetup setup = new NotExistsSetup(getName());
//        
//        final IVariable<?> avar = Var.var("a");
//        final IVariable<?> nvar = Var.var("n");
//        final IVariable<?> mvar = Var.var("m");
//        
//        // the join variables.
//        final IVariable<?>[] joinVars = new IVariable[]{avar};
//
//        // the variables projected by the join (iff non-null).
//        final IVariable<?>[] selectVars = null;//new IVariable[] { avar, nvar };
//
//        // the join constraints.
//        final IConstraint[] constraints = new IConstraint[] { Constraint
//                .wrap(new EQ(nvar, mvar)) };
//
//        // The left solutions (the pipeline).
//        final List<IBindingSet> right = setup.getLeft1();
//
//        // The right solutions (the hash index).
//        final List<IBindingSet> left = setup.getRight1(mvar);
//
//        // The expected solutions to the join.
//        @SuppressWarnings("rawtypes")
//        final IBindingSet[] expected = new IBindingSet[] {//
//                new ListBindingSet(//
//                        new IVariable[] { avar, nvar },//
//                        new IConstant[] { new Constant<IV>(setup.b),
//                                          new Constant<IV>(setup.three)
//                                          }//
//                ),//
//        };
//
//        doHashJoinTest(JoinTypeEnum.NotExists, joinVars, selectVars,
//                constraints, left, right, expected);
//
//    }

    /**
     * Setup a problem based on the following query, which is
     * <code>service02</code> from the openrdf SPARQL 1.1 Federated Query test
     * suite. 
     * 
     * <pre>
     * SELECT ?s ?o1 ?o2
     * {
     *   SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> {
     *   ?s ?p ?o1 . }
     *   OPTIONAL {
     *     SERVICE <http://localhost:18080/openrdf/repositories/endpoint2> {
     *     ?s ?p2 ?o2 }
     *   }
     * }
     * </pre>
     * 
     * Solutions for endpoint1:
     * 
     * <pre>
     * { s=http://example.org/a, p=http://xmlns.com/foaf/0.1/name, o1="Alan" }
     * { s=http://example.org/b, p=http://xmlns.com/foaf/0.1/name, o1="Bob" }
     * </pre>
     * 
     * Solutions for endpoint2:
     * 
     * <pre>
     * { s=http://example.org/a, p2=http://xmlns.com/foaf/0.1/interest, o2="SPARQL 1.1 Basic Federated Query" }
     * </pre>
     */
    static public class JoinSetup_service02 {

        public final String namespace;

        public final IV<?, ?> a, b, foafName, alan, bob, foafInterest, label;

        public JoinSetup_service02(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();
            
            this.namespace = namespace;

            a = makeIV(new URIImpl("http://example.org/a"));
            
            b = makeIV(new URIImpl("http://example.org/b"));

            foafName = makeIV(new URIImpl("http://xmlns.com/foaf/0.1/name"));

            alan = makeIV(new LiteralImpl("Alan"));

            bob = makeIV(new LiteralImpl("Bob"));

            foafInterest = makeIV(new URIImpl("http://xmlns.com/foaf/0.1/interest"));
            
            label = makeIV(new LiteralImpl("SPARQL 1.1 Basic Federated Query"));
            
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        /**
         * Solutions for end point 1.
         * 
         * <pre>
         * { s=http://example.org/a, p=http://xmlns.com/foaf/0.1/name, o1="Alan" }
         * { s=http://example.org/b, p=http://xmlns.com/foaf/0.1/name, o1="Bob" }
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getLeft1() {

            final IVariable<?> s = Var.var("s");
            final IVariable<?> p = Var.var("p");
            final IVariable<?> o1 = Var.var("o1");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alan));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(b));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(bob));
            left.add(tmp);

            return left;
        }

        /**
         * Solutions for end point 2.
         * 
         * <pre>
         * { s=http://example.org/a, p2=http://xmlns.com/foaf/0.1/interest, o2="SPARQL 1.1 Basic Federated Query" }
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getRight1() {

            final IVariable<?> s = Var.var("s");
            final IVariable<?> p2 = Var.var("p2");
            final IVariable<?> o2 = Var.var("o2");

            // The right solutions (the hash index).
            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p2, new Constant<IV>(foafInterest));
            tmp.set(o2, new Constant<IV>(label));
            right.add(tmp);

            return right;

        }
        
        /**
         * Solutions for the ServiceCallJoin with end point 2. It combines the
         * first left solution with the first (and only) right solution.
         * 
         * <pre>
         *     <result>
         *       <binding name="s"><uri>http://example.org/a</uri></binding>
         *       p=foaf:name
         *       <binding name="o1"><literal>Alan</literal></binding>
         *       p2=foaf:interest
         *       <binding name="o2"><literal>SPARQL 1.1 Basic Federated Query</literal></binding>
         *     </result>
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getServiceCall2JoinSolutions() {
            
            final IVariable<?> s = Var.var("s");
            final IVariable<?> p = Var.var("p");
            final IVariable<?> o1 = Var.var("o1");
            final IVariable<?> p2 = Var.var("p2");
            final IVariable<?> o2 = Var.var("o2");

            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alan));
            tmp.set(p2, new Constant<IV>(foafInterest));
            tmp.set(o2, new Constant<IV>(label));
            right.add(tmp);

            return right;

        }

    }

    /**
     * Unit test for the 2nd SERVICE hash join for {@link JoinSetup_service02}. 
     */
    public void test_service02() {
        
        final JoinSetup_service02 setup = new JoinSetup_service02(getName());
        
        final IVariable<?> s = Var.var("s");
//        final IVariable<?> p = Var.var("p");
//        final IVariable<?> o1 = Var.var("o1");
//        final IVariable<?> p2 = Var.var("p2");
//        final IVariable<?> o2 = Var.var("o2");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[] { s };

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;//new IVariable[] { s, o1, o2 };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getRight1();

        // The expected solutions to the join.
        final IBindingSet[] expected = setup.getServiceCall2JoinSolutions()
                .toArray(new IBindingSet[] {});

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }
    
    /**
     * Unit test for the OPTIONAL GROUP hash join for {@link JoinSetup_service02}. This
     * is the optional JOIN performed between the solutions from the 1st SERVICE
     * and the solutions from the 2nd SERVICE. The solutions to this join are
     * the solutions to the query, except that some variables are dropped by the
     * projection. We model the projection here in order to compare directly to
     * the expected solutions for the query.
     */
    public void test_service02b() {
        
        final JoinSetup_service02 setup = new JoinSetup_service02(getName());
        
        final IVariable<?> s = Var.var("s");
//        final IVariable<?> p = Var.var("p");
        final IVariable<?> o1 = Var.var("o1");
//        final IVariable<?> p2 = Var.var("p2");
        final IVariable<?> o2 = Var.var("o2");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[] { s };

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = new IVariable[] { s, o1, o2 };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getLeft1();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getServiceCall2JoinSolutions();

        /**
         * This is the OPTIONAL solution in the original query. It should not be
         * produced for the problem which we are setting up here since we are
         * modeling the ServiceCallJoin.
         * 
         * <pre>
         *     <result>
         *       <binding name="s"><uri>http://example.org/b</uri></binding>
         *       <binding name="o1"><literal>Bob</literal></binding>
         *     </result>
         * </pre>
         * 
         * This is the non-OPTIONAL ServiceCallJoin solution. It combines the
         * first left solution with the first (and only) right solution.
         * 
         * <pre>
         *     <result>
         *       <binding name="s"><uri>http://example.org/a</uri></binding>
         *       <binding name="o1"><literal>Alan</literal></binding>
         *       <binding name="o2"><literal>SPARQL 1.1 Basic Federated Query</literal></binding>
         *     </result>
         * </pre>
         */
        // The expected solutions to the join.
        @SuppressWarnings("rawtypes")
        final IBindingSet[] expected = new IBindingSet[] {//
                new ListBindingSet(//
                new IVariable[] { s, o1 },//
                new IConstant[] { new Constant<IV>(setup.b),
                        new Constant<IV>(setup.bob) }//
                ),//
                new ListBindingSet(//
                new IVariable[] { s, o1, o2},//
                new IConstant[] { new Constant<IV>(setup.a),
                        new Constant<IV>(setup.alan),
                        new Constant<IV>(setup.label), }//
                ),//
        };

        doHashJoinTest(JoinTypeEnum.Optional, joinVars, selectVars, constraints,
                left, right, expected);

    }
 
    /**
     * Setup a problem based on the following query, which is
     * <code>service04</code> from the openrdf SPARQL 1.1 Federated Query test
     * suite.
     * 
     * <pre>
     * PREFIX : <http://example.org/> 
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
     * SELECT ?s ?o1 ?o2
     * {
     *   ?s ?p1 ?o1 
     *   OPTIONAL { SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> {?s foaf:knows ?o2 }}
     * } BINDINGS ?o2 {
     *  (:b)
     * }
     * </pre>
     * 
     * Solutions for the local access path (?s ?p1 ?o1):
     * <pre>
     * :a foaf:name "Alan" .
     * :a foaf:mbox "alan@example.org" .
     * :b foaf:name "Bob" .
     * :b foaf:mbox "bob@example.org" .
     * :c foaf:name "Alice" .
     * :c foaf:mbox "alice@example.org" .
     * </pre>
     * 
     * Solutions for endpoint1:
     * 
     * <pre>
     * { o2=http://example.org/b, s=http://example.org/a }
     * { o2=http://example.org/b, s=http://example.org/a }
     * </pre>
     */
    static public class JoinSetup_service04 {

        public final String namespace;

        public final IV<?, ?> a, b, c, foafName, foafMbox, alan, bob, alice,
                alanEmail, bobEmail, aliceEmail;

        public JoinSetup_service04(final String namespace) {

            if (namespace == null)
                throw new IllegalArgumentException();
            
            this.namespace = namespace;

            a = makeIV(new URIImpl("http://example.org/a"));
            
            b = makeIV(new URIImpl("http://example.org/b"));
            
            c = makeIV(new URIImpl("http://example.org/c"));

            foafName = makeIV(new URIImpl("http://xmlns.com/foaf/0.1/name"));

            foafMbox = makeIV(new URIImpl("http://xmlns.com/foaf/0.1/mbox"));
            
            alan = makeIV(new LiteralImpl("Alan"));

            bob = makeIV(new LiteralImpl("Bob"));

            alice = makeIV(new LiteralImpl("Alice"));

            alanEmail = makeIV(new LiteralImpl("alan@example.org"));
            
            bobEmail = makeIV(new LiteralImpl("bob@example.org"));
            
            aliceEmail = makeIV(new LiteralImpl("alice@example.org"));
            
        }

        /**
         * Return a (Mock) IV for a Value.
         * 
         * @param v
         *            The value.
         * 
         * @return The Mock IV.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IV makeIV(final Value v) {
            final BigdataValueFactory valueFactory = BigdataValueFactoryImpl
                    .getInstance(namespace);
            final BigdataValue bv = valueFactory.asValue(v);
            final IV iv = new TermId(VTE.valueOf(v), nextId++);
            iv.setValue(bv);
            return iv;
        }

        private long nextId = 1L; // Note: First id MUST NOT be 0L !!!

        /**
         * Solutions for the local access path (?s ?p1 ?o1)
         * 
         * <pre>
         * :a foaf:name "Alan" .
         * :a foaf:mbox "alan@example.org" .
         * { s=TermId(2U)[http://example.org/b], p1=TermId(4U)[http://xmlns.com/foaf/0.1/name], o1=TermId(7L)[Bob] },
         * { s=TermId(2U)[http://example.org/b], p1=TermId(5U)[http://xmlns.com/foaf/0.1/mbox], o1=TermId(10L)[bob@example.org] },
         * { s=TermId(3U)[http://example.org/c], p1=TermId(4U)[http://xmlns.com/foaf/0.1/name], o1=TermId(8L)[Alice] },
         * { s=TermId(3U)[http://example.org/c], p1=TermId(5U)[http://xmlns.com/foaf/0.1/mbox], o1=TermId(11L)[alice@example.org] }]
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getSolutionsLocalAP() {

            final IVariable<?> s = Var.var("s");
            final IVariable<?> p = Var.var("p1");
            final IVariable<?> o1 = Var.var("o1");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            // alan
            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alan));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(alanEmail));
            left.add(tmp);

            // bob
            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(b));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(bob));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(b));
            tmp.set(p, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(bobEmail));
            left.add(tmp);

            // alice
            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(c));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alice));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(c));
            tmp.set(p, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(aliceEmail));
            left.add(tmp);

            return left;
        }

        /**
         * Solutions for end point 1.
         * 
         * <pre>
         * { o2=http://example.org/b, s=http://example.org/a }
         * { o2=http://example.org/b, s=http://example.org/a }
         * </pre>
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getSolutionsEndpoint1() {

            final IVariable<?> s = Var.var("s");
            final IVariable<?> o2 = Var.var("o2");

            // The left solutions (the pipeline).
            final List<IBindingSet> left = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(o2, new Constant<IV>(b));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(a));
            tmp.set(o2, new Constant<IV>(b));
            left.add(tmp);

            return left;
        }
        
        /**
         * Solutions for the ServiceCallJoin with end point 2. It combines the
         * solutions from the local access path (left) with the solutions from
         * endpoint1 (right).
         * 
         * <pre>
         * { o2=TermId(2U)[http://example.org/b], s=TermId(1U)[http://example.org/a], p1=TermId(5U), o1=TermId(8L) }
         * { o2=TermId(2U)[http://example.org/b], s=TermId(1U)[http://example.org/a], p1=TermId(6U), o1=TermId(7L) }
         * { o2=TermId(2U)[http://example.org/b], s=TermId(1U)[http://example.org/a], p1=TermId(5U), o1=TermId(8L) }
         * { o2=TermId(2U)[http://example.org/b], s=TermId(1U)[http://example.org/a], p1=TermId(6U), o1=TermId(7L) }
         * </pre>
         */
        //  5=foaf:mbox
        //  6=foaf:name
        //  7=Alan
        //  9=Alice
        // 11=Bob
        //  8=alan@example.org
        // 10=alice@example.org
        // 12=bob@example.org
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getServiceCallJoinSolutions() {

            final IVariable<?> o2 = Var.var("o2");
            final IVariable<?> s = Var.var("s");
            final IVariable<?> p1 = Var.var("p1");
            final IVariable<?> o1 = Var.var("o1");

            final List<IBindingSet> right = new LinkedList<IBindingSet>();

            IBindingSet tmp;

            tmp = new ListBindingSet();
            tmp.set(o2, new Constant<IV>(b));
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p1, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(alanEmail));
            right.add(tmp);
            right.add(tmp.clone()); // twice.

            tmp = new ListBindingSet();
            tmp.set(o2, new Constant<IV>(b));
            tmp.set(s, new Constant<IV>(a));
            tmp.set(p1, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alan));
            right.add(tmp);
            right.add(tmp.clone()); // twice

            return right;

        }

        /**
         * The OPTIONAL group join will return the UNION the solutions from the
         * SERVICE which did join, which is
         * {@link #getServiceCallJoinSolutions()}, with the solutions from the
         * local access path which did not join.
         */
        @SuppressWarnings("rawtypes")
        List<IBindingSet> getOptionalGroupSolutions() {

            final IVariable<?> s = Var.var("s");
            final IVariable<?> p = Var.var("p1");
            final IVariable<?> o1 = Var.var("o1");

            final List<IBindingSet> left = new LinkedList<IBindingSet>();
            
            left.addAll(getServiceCallJoinSolutions());

            IBindingSet tmp;
            
            // bob
            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(b));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(bob));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(b));
            tmp.set(p, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(bobEmail));
            left.add(tmp);

            // alice
            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(c));
            tmp.set(p, new Constant<IV>(foafName));
            tmp.set(o1, new Constant<IV>(alice));
            left.add(tmp);

            tmp = new ListBindingSet();
            tmp.set(s, new Constant<IV>(c));
            tmp.set(p, new Constant<IV>(foafMbox));
            tmp.set(o1, new Constant<IV>(aliceEmail));
            left.add(tmp);

            return left;
            
        }
        
    }

    /**
     * Unit test for the SERVICE hash join for {@link JoinSetup_service04}. 
     */
    public void test_service04a() {

        final JoinSetup_service04 setup = new JoinSetup_service04(getName());
        
        final IVariable<?> s = Var.var("s");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[] { s };

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;//new IVariable[] { s, o1, o2 };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getSolutionsLocalAP();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getSolutionsEndpoint1();

        // The expected solutions to the join.
        final IBindingSet[] expected = setup.getServiceCallJoinSolutions()
                .toArray(new IBindingSet[] {});

        doHashJoinTest(JoinTypeEnum.Normal, joinVars, selectVars, constraints,
                left, right, expected);

    }
    
    /**
     * Unit test for the OPTIONAL hash join for {@link JoinSetup_service04}. The
     * solutions for the query are just the PROJECTION of (?s ?o1 ?o2) for this
     * join.
     */
    public void test_service04b() {

        final JoinSetup_service04 setup = new JoinSetup_service04(getName());
        
        final IVariable<?> s = Var.var("s");
        
        // the join variables.
        final IVariable<?>[] joinVars = new IVariable[] { s };

        // the variables projected by the join (iff non-null).
        final IVariable<?>[] selectVars = null;//new IVariable[] { s, o1, o2 };

        // the join constraints.
        final IConstraint[] constraints = null;

        // The left solutions (the pipeline).
        final List<IBindingSet> right = setup.getSolutionsLocalAP();

        // The right solutions (the hash index).
        final List<IBindingSet> left = setup.getServiceCallJoinSolutions();

        // The expected solutions to the join.
        final IBindingSet[] expected = setup.getOptionalGroupSolutions()
                .toArray(new IBindingSet[] {});

        doHashJoinTest(JoinTypeEnum.Optional, joinVars, selectVars, constraints,
                left, right, expected);

    }
    
}
