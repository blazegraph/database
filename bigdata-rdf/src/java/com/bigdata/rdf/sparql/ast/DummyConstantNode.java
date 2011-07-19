package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.Constant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataValue;

/**
 * The dummy constant node is used to represent constants in the AST that do not
 * actually exist in the database. For example, the pattern and flags arguments
 * to regex, or possibly the right operand to a compare operation whose left
 * operand is a datatype, label, or str function. Also useful for magic
 * predicates such as those used by free text search.
 * 
 * @author mikepersonick
 */
public class DummyConstantNode extends TermNode {

	private static final IV dummyIV(final BigdataValue val) {
		
		final IV dummy = TermId.mockIV(VTE.valueOf(val));
		
		dummy.setValue(val);
		
		val.setIV(dummy);
		
		return dummy;
		
	}
	
	public DummyConstantNode(final BigdataValue val) {
		
		super(new Constant<IV>(dummyIV(val)));
		
	}
	
}
