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
 * Created on Jul 9, 2010
 */

package com.bigdata.rdf.internal;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * This is used by certain {@link IVValueExpression}s to create an alternate
 * "view" of {@link IV} for a {@link Literal} or a {@link URI}, such as one
 * where the {@link BigdataValue} is transformed into a simple literal (no
 * datatype, no language tag) using the URI's toString() or the Literal's label.
 * <p>
 * WrappedIVs don't get shipped around in scale-out, they never become part of
 * the binding sets. They are intermediate objects passed between value
 * expressions inside a constraint, which are all executed at the same time in
 * one place. For example:
 * <pre>
 * FILTER(str(?o) = "foo")
 * </pre>
 * There is a CompareBOp wrapping a StrBOp - the StrBOp evaluates to a WrappedIV
 * with an altView of the BigdataValue (the plain literal form) and the
 * CompareBOp uses that WrappedIV as its left operand and returns true or false.
 */
public class WrappedIV implements IV {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5020144206004241997L;
	
	private final IV iv;
	
	private volatile transient BigdataValue altVal;
	
    public WrappedIV(final IV iv, final BigdataValue altVal) {
        this.iv = iv;
        this.altVal = altVal;
    }

    public String toString() {
        return iv.toString();
    }
    
    public BigdataValue asValue(final LexiconRelation lex) {
        return altVal;
    }

	public BigdataValue setValue(final BigdataValue altVal) {
		return (this.altVal = altVal);
	}

	public BigdataValue getValue() {
		return altVal;
	}

	public boolean hasValue() {
		return altVal != null;
	}

	public void dropValue() {
		altVal = null;
	}

	// delegate everything else
	
	public int compareTo(Object o) {
		return iv.compareTo(o);
	}

	public byte flags() {
		return iv.flags();
	}

	public int byteLength() {
		return iv.byteLength();
	}

	public IKeyBuilder encode(final IKeyBuilder keyBuilder) {
		return iv.encode(keyBuilder);
	}

	public VTE getVTE() {
		return iv.getVTE();
	}

	public boolean isLiteral() {
		return iv.isLiteral();
	}

	public boolean isBNode() {
		return iv.isBNode();
	}

	public boolean isURI() {
		return iv.isURI();
	}

	public boolean isStatement() {
		return iv.isStatement();
	}

	public boolean isResource() {
		return iv.isResource();
	}

	public DTE getDTE() {
		return iv.getDTE();
	}

	public boolean isNullIV() {
		return iv.isNullIV();
	}

	public boolean isInline() {
		return iv.isInline();
	}

	public boolean isExtension() {
		return iv.isExtension();
	}

	public Object getInlineValue() throws UnsupportedOperationException {
		return iv.getInlineValue();
	}

	public boolean isNumeric() {
		return iv.isNumeric();
	}

	public boolean isSignedNumeric() {
		return iv.isSignedNumeric();
	}

	public boolean isUnsignedNumeric() {
		return iv.isUnsignedNumeric();
	}

	public boolean isFixedNumeric() {
		return iv.isFixedNumeric();
	}

	public boolean isBigNumeric() {
		return iv.isBigNumeric();
	}

	public boolean isFloatingPointNumeric() {
		return iv.isFloatingPointNumeric();
	}
	
}
