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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;

/**
 * Test suite for {@link SolutionSetStats}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestSolutionSetStats.java 6099 2012-03-09 16:35:55Z thompsonbry $
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
abstract public class AbstractSolutionSetStatsTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractSolutionSetStatsTestCase() {
    }

    /**
     * @param name
     */
    public AbstractSolutionSetStatsTestCase(String name) {
        super(name);
    }

    /**
	 * Implement this method to test a specific {@link ISolutionSetStats} class.
	 * 
	 * @param bindingSets
	 *            The solutions to be analyzer.
	 *            
	 * @return The compiled statistics for those solutions.
	 */
	abstract protected ISolutionSetStats newFixture(
			final IBindingSet[] bindingSets);

    /**
     * Typed empty set.
     */
    protected static final Set<IVariable> emptySet = Collections.emptySet();
    
    /**
     * Typed empty map.
     */
    protected static final Map<IVariable,IConstant> emptyMap = Collections.emptyMap();
    
    protected <T> IConstant<T> asConst(final T val) {

        return new Constant<T>(val);
        
    }
    
    /**
     * Turn an array of variables into a {@link Set} of variables.
     * 
     * @param vars
     *            The array.
     * @return The set.
     */
    protected Set<IVariable> asSet(final IVariable... vars) {

        final Set<IVariable> set = new HashSet<IVariable>();

        for (IVariable v : vars) {
        
            if (!set.add(v))
                fail("Duplicate: " + v);
            
        }

        return set;
    }
    
    protected <T> T[] asArray(T... vals) {
        return vals;
    }
    
    protected Map<IVariable, IConstant> asMap(final IVariable[] vars,
			final IConstant[] vals) {

		assert vars.length == vals.length;

		final Map<IVariable, IConstant> map = new LinkedHashMap<IVariable, IConstant>();

		for (int i = 0; i < vars.length; i++) {

			map.put(vars[i], vals[i]);

		}

		return map;

	}
    
    /**
     * Unit test with an empty solution set.
     */
    public void test_002() {
        
        final IBindingSet[] bsets = new IBindingSet[] {};

        final ISolutionSetStats expected = new MySolutionStats(//
        		0, // nsolutions
        		emptySet, // usedVars
        		emptySet,// notAlwaysBound
        		emptySet,// alwaysBound
        		asMap(new IVariable[0],new IConstant[0])// constants
        );

        final ISolutionSetStats actual = newFixture(bsets);

        assertSameStats(expected, actual);

    }

    /**
     * Unit test with a single solution having a single bound variable.
     */
    public void test_003() {
        
        final IVariable x = Var.var("x");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                1,// nsolutions
                asSet(x),// usedVars
                emptySet,// notAlwaysBound
                asSet(x), // alwaysBound
                asMap(asArray(x),asArray(asConst("1")))// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));

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
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                1,// nsolutions
                asSet(x, y),// usedVars
                emptySet,// notAlwaysBound
                asSet(x, y), // alwaysBound
                asMap(new IVariable[]{x,y},new IConstant[]{asConst("1"),asConst("2")})
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
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
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("3"));
            bset.set(y, asConst("4"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y),// usedVars
                emptySet,// notAlwaysBound
                asSet(x, y), // alwaysBound
                emptyMap// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
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
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("3"));
            bset.set(y, asConst("4"));
            bset.set(z, asConst("5"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(z),// notAlwaysBound
                asSet(x, y), // alwaysBound
                emptyMap// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
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
            bset.set(x, asConst("3"));
            bset.set(y, asConst("4"));
            bset.set(z, asConst("5"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                2,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(z),// notAlwaysBound
                asSet(x, y), // alwaysBound
                emptyMap// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
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
            bset.set(x, asConst("3"));
            bset.set(y, asConst("4"));
            bset.set(z, asConst("5"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                3,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(x, y, z),// notAlwaysBound
                asSet(), // alwaysBound,
                emptyMap// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Unit test with three solutions having two variables which are bound in
     * every solution to the same value plus one variable which is bound to
     * a different value in every solution.
     */
    public void test_009() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");
        final IVariable z = Var.var("z");
        
        final List<IBindingSet> bsets = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bset.set(z, asConst("5"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bset.set(z, asConst("6"));
            bsets.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(x, asConst("1"));
            bset.set(y, asConst("2"));
            bset.set(z, asConst("7"));
            bsets.add(bset);
        }

        final ISolutionSetStats expected = new MySolutionStats(//
                3,// nsolutions
                asSet(x, y, z),// usedVars
                asSet(),// notAlwaysBound
                asSet(x, y, z), // alwaysBound
                asMap(asArray(x, y),
                        asArray(asConst("1"), asConst("2")))// constants
        );

        final ISolutionSetStats actual = newFixture(
                bsets.toArray(new IBindingSet[] {}));
        
        assertSameStats(expected, actual);
        
    }

    /**
     * Compare two {@link ISolutionSetStats}.
     * 
     * @param expected
     * @param actual
     */
    protected static void assertSameStats(final ISolutionSetStats expected,
            final ISolutionSetStats actual) {

        assertEquals("solutionSetSize", expected.getSolutionSetSize(),
                actual.getSolutionSetSize());

        assertEquals("usedVars", expected.getUsedVars(), actual.getUsedVars());

        assertEquals("alwaysBound", expected.getAlwaysBound(),
                actual.getAlwaysBound());

        assertEquals("notAlwaysBound", expected.getNotAlwaysBound(),
                actual.getNotAlwaysBound());

		final Map<IVariable<?>, IConstant<?>> actualConstants = actual
				.getConstants();

		if (actualConstants != null) {
			
			/*
			 * Since getConstants() is an optional method, we only check
			 * the results when it is non-null.
			 */

			assertEquals("constants", expected.getConstants(),
					actual.getConstants());
		
		}

    }
    
    /**
     * Helper class for tests.
     */
    protected static class MySolutionStats implements ISolutionSetStats {

        /**
         * The #of solutions.
         */
    		protected final long nsolutions;
        
        /**
         * The set of variables observed across all solutions.
         */
        protected final Set<IVariable> usedVars;

        /**
         * The set of variables which are NOT bound in at least one solution (e.g.,
         * MAYBE bound semantics).
         */
        protected final Set<IVariable> notAlwaysBound;

        /**
         * The set of variables which are bound in ALL solutions.
         */
        protected final Set<IVariable> alwaysBound;

        /**
         * The set of variables which are effective constants (they are bound in
         * every solution and always to the same value) together with their constant
         * bindings.
         */
        protected final Map<IVariable,IConstant> constants;

        public MySolutionStats(final long nsolutions,
                final Set<IVariable> usedVars,
                final Set<IVariable> notAlwaysBound,
                final Set<IVariable> alwaysBound,
                final Map<IVariable, IConstant> constants
                ) {

            this.nsolutions = nsolutions;
            this.usedVars = usedVars;
            this.notAlwaysBound = notAlwaysBound;
            this.alwaysBound = alwaysBound;
            this.constants = constants;
            
        }
        
        @Override
        public long getSolutionSetSize() {
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

        @Override
        public Map<IVariable<?>, IConstant<?>> getConstants() {
            return (Map) constants;
        }
        
    }
    
}
