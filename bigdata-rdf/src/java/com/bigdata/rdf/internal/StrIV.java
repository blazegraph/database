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

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * A StrIV is used by the StrBOp to create an alternate "view" of a Literal or
 * URI IV, one where the {@link BigdataValue} is transormed into a simple
 * literal (no datatype, no language tag) using the URI's toString() or the
 * Literal's label.
 * 
 * TODO Mike, I'd advise handling this as a TermId standing in for a Literal
 * whose datatype is xsd:string and whose termId is ZERO. That will basically
 * look like a Literal which has not been inserted into (or looked up against)
 * the database. At a minimum, I would extend {@link AbstractIV}. Bryan
 */
public class StrIV implements IV {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5020144206004241997L;
	
	private final IV iv;
	
	private final BigdataValue strVal;
	
    public StrIV(final IV iv, final BigdataValue strVal) {
        this.iv = iv;
        this.strVal = strVal;
    }

    public String toString() {
        return iv.toString();
    }
    
    public BigdataValue asValue(final LexiconRelation lex) {
        return strVal;
    }

	public int compareTo(Object o) {
		return iv.compareTo(o);
	}

	public byte flags() {
		return iv.flags();
	}

	public int byteLength() {
		return iv.byteLength();
	}

	public IKeyBuilder encode(IKeyBuilder keyBuilder) {
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

	public DTE getDTE() {
		return iv.getDTE();
	}

	public boolean isTermId() {
		return iv.isTermId();
	}

	public long getTermId() throws UnsupportedOperationException {
		return iv.getTermId();
	}

	public boolean isInline() {
		return iv.isInline();
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

	public void dropValue() {
		// TODO Auto-generated method stub
		
	}

	public BigdataValue getValue() throws NotMaterializedException {
		// TODO Auto-generated method stub
		return null;
	}

}
