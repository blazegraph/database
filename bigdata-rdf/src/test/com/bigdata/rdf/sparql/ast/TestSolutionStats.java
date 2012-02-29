/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 29, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;

/**
 * Test suite for {@link SolutionSetStats}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestSolutionStats extends TestCase2 {

    /**
     * 
     */
    public TestSolutionStats() {
    }

    /**
     * @param name
     */
    public TestSolutionStats(String name) {
        super(name);
    }

    /**
     * Typed empty set.
     */
    private static final Set<IVariable> emptySet = Collections.emptySet();
    
    /**
     * Turn an array of variables into a {@link Set} of variables.
     * 
     * @param vars
     *            The array.
     * @return The set.
     */
    private Set<IVariable> asSet(final IVariable... vars) {

        final Set<IVariable> set = new HashSet<IVariable>();

        for (IVariable v : vars) {
        
            if (!set.add(v))
                fail("Duplicate: " + v);
            
        }

        return set;
    }
    
    /**
     * Correct rejection test for the constructor.
     */
    public void test_001() {
        
        try {
            new SolutionSetStats(null/* bindingSets */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Unit test with an empty solution set.
     */
    public void test_002() {
        
        final IBindingSet[] bsets = new IBindingSet[] {};

        final SolutionSetStats stats = new SolutionSetStats(Arrays
                .asList(bsets).iterator());

        assertEquals("solutionSetSize", 0, stats.getSolutionSetSize());

        assertEquals("usedVars", Collections.emptySet(),
                stats.getUsedVars());

        assertEquals("alwaysBound", Collections.emptySet(),
                stats.getAlwaysBound());
        
        assertEquals("notAlwaysBound", Collections.emptySet(),
                stats.getNotAlwaysBound());

    }

    /**
     * Unit test with a single solution having a single bound variable.
     */
    public void test_003() {
        
        final IVariable x = Var.var("x");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                1,// nsolutions
                asSet(x),// usedVars
                emptySet,// notAlwaysBound
                asSet(x) // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }
    
    /**
     * Unit test with a single solution having a two bound variables.
     */
    public void test_004() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bset.set(y, new Constant<String>("2"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                1,// nsolutions
                asSet(x, y),// usedVars
                emptySet,// notAlwaysBound
                asSet(x, y) // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Unit test with two solutions having two variables which are bound in both
     * solutions.
     */
    public void test_005() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bset.set(y, new Constant<String>("2"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("3"));
            bset.set(y, new Constant<String>("4"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y),// usedVars
                emptySet,// notAlwaysBound
                asSet(x, y) // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Unit test with two solutions having two variables which are bound in
     * every solution plus one variable which is bound in just one of the
     * solutions.
     */
    public void test_006() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        final IVariable z = Var.var("z");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bset.set(y, new Constant<String>("2"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("3"));
            bset.set(y, new Constant<String>("4"));
            bset.set(z, new Constant<String>("5"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(z),// notAlwaysBound
                asSet(x, y) // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Unit test with two solutions having two variables which are bound in
     * every solution plus one variable which is bound in just one of the
     * solutions (this is the same as the previous test except that it presents
     * the solutions in a different order to test the logic which detects
     * variables which are not always bound).
     */
    public void test_007() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        final IVariable z = Var.var("z");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("3"));
            bset.set(y, new Constant<String>("4"));
            bset.set(z, new Constant<String>("5"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bset.set(y, new Constant<String>("2"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(z),// notAlwaysBound
                asSet(x, y) // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Unit test with three solutions having one solution in which nothing is
     * bound.
     */
    public void test_008() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        final IVariable z = Var.var("z");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("3"));
            bset.set(y, new Constant<String>("4"));
            bset.set(z, new Constant<String>("5"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, new Constant<String>("1"));
            bset.set(y, new Constant<String>("2"));
            bsets.add(bset);
        }

        final ISolutionStats expected = new MySolutionStats(//
                3,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(x, y, z),// notAlwaysBound
                asSet() // alwaysBound
        );

        final SolutionSetStats actual = new SolutionSetStats(bsets.iterator());
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Compare two {@link ISolutionStats}.
     * 
     * @param expected
     * @param actual
     */
    private static void assertSameStats(final ISolutionStats expected,
            final ISolutionStats actual) {

        assertEquals("solutionSetSize", expected.getSolutionSetSize(),
                actual.getSolutionSetSize());

        assertEquals("usedVars", expected.getUsedVars(), actual.getUsedVars());

        assertEquals("alwaysBound", expected.getAlwaysBound(),
                actual.getAlwaysBound());

        assertEquals("notAlwaysBound", expected.getNotAlwaysBound(),
                actual.getNotAlwaysBound());

    }
    
    /**
     * Helper class for tests.
     */
    private static class MySolutionStats implements ISolutionStats {

        /**
         * The #of solutions.
         */
        private final int nsolutions;
        
        /**
         * The set of variables observed across all solutions.
         */
        private final Set<IVariable> usedVars;

        /**
         * The set of variables which are NOT bound in at least one solution (e.g.,
         * MAYBE bound semantics).
         */
        private final Set<IVariable> notAlwaysBound;

        /**
         * The set of variables which are bound in ALL solutions.
         */
        private final Set<IVariable> alwaysBound;

        public MySolutionStats(final int nsolutions,
                final Set<IVariable> usedVars,
                final Set<IVariable> notAlwaysBound,
                final Set<IVariable> alwaysBound) {

            this.nsolutions = nsolutions;
            this.usedVars = usedVars;
            this.notAlwaysBound = notAlwaysBound;
            this.alwaysBound = alwaysBound;
            
        }
        
        
        @Override
        public int getSolutionSetSize() {
            return nsolutions;
        }

        @Override
        public Set<IVariable<?>> getUsedVars() {
            return (Set)usedVars;
        }

        @Override
        public Set<IVariable<?>> getAlwaysBound() {
            return (Set)alwaysBound;
        }

        @Override
        public Set<IVariable<?>> getNotAlwaysBound() {
            return (Set) notAlwaysBound;
        }
        
    }
    
}
