package com.bigdata.rdf.rules;

import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.Rule;

/**
 * The closure program must include the new custom inference rules by
 * overriding the parent method {@link BaseClosure#getCustomRules(String)}.
 */
public class SampleClosure extends FastClosure {

	public SampleClosure(final AbstractTripleStore db) {
		super(db);
	}

	/**
	 * Called once by super class during construction of inference program.
	 */
	public List<Rule> getCustomRules(final String relationName) {
		
		final List<Rule> rules = new LinkedList<Rule>();
		
		rules.add(new SampleRule(relationName, vocab));
		
		return rules;
		
	}
	
}
