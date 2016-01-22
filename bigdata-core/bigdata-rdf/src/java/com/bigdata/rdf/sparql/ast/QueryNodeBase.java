package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.Properties;

import com.bigdata.bop.BOp;

/**
 * AST node base class.
 */
public abstract class QueryNodeBase extends ASTBase implements
        IQueryNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends ASTBase.Annotations {
        
    }
    
    public QueryNodeBase() {

        super(BOp.NOARGS, null/* anns */);

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public QueryNodeBase(final QueryNodeBase op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public QueryNodeBase(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Return the query hints for this AST node.
     * 
     * @return The query hints -or- <code>null</code> if none have been
     *         declared.
     */
    public Properties getQueryHints() {

        return (Properties) getProperty(Annotations.QUERY_HINTS);

    }

    @Override
    public final String toString() {

//        return super.toString(); 
        return toString(0/* indent */);
        
    }

}
