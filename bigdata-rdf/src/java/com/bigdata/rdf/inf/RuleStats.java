package com.bigdata.rdf.inf;


/**
 * Statistics about what the Rule did during {@link Rule#apply()}.
 * 
 * @author mikep
 */
public class RuleStats {

    /**
     * #of matches for the triple pattern for the first antecedent of the rule.
     */
    public int stmts1;

    /**
     * #of matches for the triple pattern for the second antecedent of the
     * rule (if there are two).
     */
    public int stmts2;
    
    /**
     * #of matches for the 3rd triple pattern (if there are three).
     */
    public int stmts3;
    
    /**
     * If the rule performs a JOIN, this is the #of distinct queries that
     * are made for the 2nd triple pattern.  For some rules, we can reorder
     * the results from the first triple pattern in order to reduce the #of
     * subqueries.
     */
    public int numSubqueries1;

    /**
     * If the rule performs a 2nd JOIN, this is the #of distinct queries that
     * are made for the inner triple pattern. For some rules, we can reorder the
     * results from the outer triple pattern in order to reduce the #of
     * subqueries.
     */
    public int numSubqueries2;
    
    /**
     * #of entailments computed by the rule (does not consider whether or not
     * the entailments were pre-existing in the database nor whether or not the
     * entailments filtered out such that they did not enter the database).
     */
    public int numComputed;
    
    /**
     * Time to compute the entailments (ms).
     */
    public long elapsed;

    public String toString() {

        return "#stmts1=" + stmts1 //
                + ", #stmts2=" + stmts2//
                + ", #stmts3=" + stmts3//
                + ", #subqueries1=" + numSubqueries1 //
                + ", #subqueries2=" + numSubqueries2 //
                + ", #computed=" + numComputed//
                + ", elapsed=" + elapsed//
        ;

    }

}
