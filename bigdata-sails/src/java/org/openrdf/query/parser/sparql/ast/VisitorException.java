/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql.ast;

public class VisitorException extends Exception {

	private static final long serialVersionUID = 6682567015759392643L;

	public VisitorException() {
		super();
	}

	public VisitorException(String msg) {
		super(msg);
	}

	public VisitorException(String msg, Throwable t) {
		super(msg, t);
	}

	public VisitorException(Throwable t) {
		super(t);
	}
}
