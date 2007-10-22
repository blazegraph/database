package com.bigdata.rdf.inf;

import com.bigdata.rdf.store.TempTripleStore;

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
     * #of entailments computed.
     */
    public int numComputed;
    
    /**
     * Time to compute the entailments and store them within the
     * {@link TempTripleStore} in milliseconds.
     */
    long computeTime;
    
    public String toString() {

        return "#stmts1=" + stmts1 //
                + ", #stmts2=" + stmts2//
                + ", #stmts3=" + stmts3//
                + ", #subqueries1=" + numSubqueries1 //
                + ", #subqueries2=" + numSubqueries2 //
                + ", #computed=" + numComputed//
                + ", computeTime=" + computeTime//
        ;

    }

}
