package com.bigdata.rdf.rules;

/**
 * We have two rules that can throw this exception -
 * {@link RuleOwlFunctionalProperty} and
 * {@link RuleOwlInverseFunctionalProperty}. Those rules define conditions where
 * instead of producing an entailment, a constraint is violated and the closure
 * operation must fail.
 * 
 * @author mikepersonick
 */
public class ConstraintViolationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5203955209965836817L;

	public ConstraintViolationException(final String s) {
		super(s);
	}
	
}
