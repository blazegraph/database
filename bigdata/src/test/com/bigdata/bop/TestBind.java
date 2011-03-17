/**
 * 
 */
package com.bigdata.bop;

import com.bigdata.bop.bindingSet.ListBindingSet;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link Bind}.
 * 
 * @author thompsonbry
 * 
 * @todo Write a test where the {@link IValueExpression} given to bind is more
 *       complex than an {@link IVariable} or an {@link IConstant}.
 */
public class TestBind extends TestCase2 {

	/**
	 * 
	 */
	public TestBind() {
	}

	/**
	 * @param name
	 */
	public TestBind(String name) {
		super(name);
	}

	/**
	 * Unit test of bind(var,constant).
	 */
	public void test_bind_constant() {

		final IBindingSet bset = new ListBindingSet();
		
		final IVariable<?> y = Var.var("y");
		
		// verify bind() returns the value of the constant.
		assertEquals(Integer.valueOf(12), new Bind(y, new Constant<Integer>(
				Integer.valueOf(12))).get(bset));

		// verify side-effect on the binding set.
		assertEquals(new Constant<Integer>(Integer.valueOf(12)), bset.get(y));

	}

	/**
	 * Unit test of bind(var,otherVar).
	 */
	public void test_bind_var() {

		final IBindingSet bset = new ListBindingSet();

		final IVariable<?> x = Var.var("x");

		final IVariable<?> y = Var.var("y");

		bset.set(x, new Constant<Integer>(12));

		// verify bind() returns the value of the other variable.
		assertEquals(Integer.valueOf(12), new Bind(y, x).get(bset));

		// verify side-effect on the binding set.
		assertEquals(new Constant<Integer>(Integer.valueOf(12)), bset.get(y));

	}

}
