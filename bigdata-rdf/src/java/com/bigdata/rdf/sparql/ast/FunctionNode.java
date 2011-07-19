package com.bigdata.rdf.sparql.ast;

import org.openrdf.model.URI;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.store.AbstractTripleStore;

public class FunctionNode extends ValueExpressionNode {

	/**
	 * Construct a function node in the AST.
	 * 
	 * @param lex
	 * 			the fully qualified namespace of the lexicon relation containing 
	 * 			the data on which this function will operate. Use the methods
	 * 		 	{@link AbstractTripleStore#getLexiconRelation()} and 
	 * 			{@link LexiconRelation#getNamespace()} to get this.
	 * @param functionURI
	 * 			the function URI. see {@link FunctionRegistry}
	 * @param args
	 * 			the arguments to the function.
	 */
	public FunctionNode(final String lex, final URI functionURI, 
			final ValueExpressionNode... args) {
		
		super(FunctionRegistry.toVE(lex, functionURI, args));
		
	}
	
}
