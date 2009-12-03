/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.algebra;

import java.util.Collections;
import java.util.Set;

/**
 * A tuple expression that contains exactly one solution with zero bindings.
 */
public class SingletonSet extends QueryModelNodeBase implements TupleExpr {

	public Set<String> getBindingNames() {
		return Collections.emptySet();
	}

	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
		throws X
	{
		visitor.meet(this);
	}

	@Override
	public SingletonSet clone() {
		return (SingletonSet)super.clone();
	}
}
