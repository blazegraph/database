/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import org.openrdf.query.algebra.ValueConstant;


/**
 *
 * @author Jeen
 */
public class PropertySetElem {

	private boolean inverse;

	private ValueConstant predicate;
	
	/**
	 * @param inverse The inverse to set.
	 */
	public void setInverse(boolean inverse) {
		this.inverse = inverse;
	}

	/**
	 * @return Returns the inverse.
	 */
	public boolean isInverse() {
		return inverse;
	}

	/**
	 * @param predicate The predicate to set.
	 */
	public void setPredicate(ValueConstant predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return Returns the predicate.
	 */
	public ValueConstant getPredicate() {
		return predicate;
	}
}
