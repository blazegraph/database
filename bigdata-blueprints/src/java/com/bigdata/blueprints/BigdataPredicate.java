package com.bigdata.blueprints;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Predicate;

public enum BigdataPredicate implements Predicate {

    EQ,
    
    NE,
    
    GT,
    
    GTE,
    
    LT,
    
    LTE,
    
    IN,
    
    NIN;

    @Override
    public boolean evaluate(Object arg0, Object arg1) {
        throw new RuntimeException();
    }
    
    @SuppressWarnings("deprecation")
    public static BigdataPredicate toBigdataPredicate(final Predicate p) {
        
        if (p instanceof BigdataPredicate) {
            return (BigdataPredicate) p;
        }
        
        if (p instanceof Compare) {
            final Compare c = (Compare) p;
            switch(c) {
            case EQUAL: 
                return BigdataPredicate.EQ;
            case NOT_EQUAL: 
                return BigdataPredicate.NE;
            case GREATER_THAN: 
                return BigdataPredicate.GT;
            case GREATER_THAN_EQUAL: 
                return BigdataPredicate.GTE;
            case LESS_THAN: 
                return BigdataPredicate.LT;
            case LESS_THAN_EQUAL: 
                return BigdataPredicate.LTE;
            }
        } else if (p instanceof Contains) {
            final Contains c = (Contains) p;
            switch(c) {
            case IN: 
                return BigdataPredicate.IN;
            case NOT_IN: 
                return BigdataPredicate.NIN;
            }
        } else if (p instanceof com.tinkerpop.blueprints.Query.Compare) {
            final com.tinkerpop.blueprints.Query.Compare c = 
                    (com.tinkerpop.blueprints.Query.Compare) p;
            switch(c) {
            case EQUAL: 
                return BigdataPredicate.EQ;
            case NOT_EQUAL: 
                return BigdataPredicate.NE;
            case GREATER_THAN: 
                return BigdataPredicate.GT;
            case GREATER_THAN_EQUAL: 
                return BigdataPredicate.GTE;
            case LESS_THAN: 
                return BigdataPredicate.LT;
            case LESS_THAN_EQUAL: 
                return BigdataPredicate.LTE;
            }
        }
        
        throw new IllegalArgumentException();
        
    }
    
}
