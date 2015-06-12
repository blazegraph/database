package com.bigdata.rdf.sparql.ast;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;

/**
 * AST node for anything which is neither a constant nor a variable, including
 * math operators, string functions, etc. The {@link FunctionNode} arguments are
 * the ordered list {@link ValueExpressionNode}s that are the arguments to the
 * SPARQL function.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class FunctionNode extends ValueExpressionNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends ValueExpressionNode.Annotations {
  
        /**
         * The function URI from the {@link FunctionRegistry}.
         */
        String FUNCTION_URI = FunctionNode.class.getName() + ".functionURI";

        /**
         * The scalar values needed to construct the {@link IValueExpression}.
         * This is a {@link Serializable} {@link Map} with {@link String} keys
         * and {@link Object} values.
         */
        String SCALAR_VALS = FunctionNode.class.getName() + ".scalarVals";

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param functionURI
     *            the function URI. see {@link FunctionRegistry}
     * @param scalarValues
     *            One or more scalar values that are passed to the function
     * @param args
     *            the arguments to the function.
     *            
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/672">
     *      Occasional error on BSBM Explore query</a>
     */
	public FunctionNode(// 
			final URI functionURI, 
	        final Map<String,Object> scalarValues,
			final ValueExpressionNode... args) {

	    super(args, NV.asMap(new NV[] {
                new NV(Annotations.SCALAR_VALS, scalarValues),
                new NV(Annotations.FUNCTION_URI, functionURI), }));

	    if (functionURI == null)
            throw new IllegalArgumentException();
        
	    // scalarValues MAY be null.
	    
//		super(args, null/*anns*/);
//		
//		super.setProperty(Annotations.SCALAR_VALS, scalarValues);
//		
//		super.setProperty(Annotations.FUNCTION_URI, functionURI);
		
	}
	
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public FunctionNode(final FunctionNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public FunctionNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    public URI getFunctionURI() {
		
		return (URI) getRequiredProperty(Annotations.FUNCTION_URI);
		
	}

    /**
     * Returns an unmodifiable map because if the scalar values are modified, we
     * need to clear the value expression cache. This is handled correctly by
     * {@link #setScalarValues(Map)}.
     */
    public Map<String, Object> getScalarValues() {

        @SuppressWarnings("unchecked")
        final Map<String, Object> scalarValues = (Map<String, Object>) getProperty(Annotations.SCALAR_VALS);

        if (scalarValues == null) {

            return Collections.emptyMap();

        }

        return Collections.unmodifiableMap(scalarValues);

	}
	
	/**
	 * Overridden to clear the cached value expression.
	 */
	@Override
	public void invalidate() {
	    
	    super.clearProperty(Annotations.VALUE_EXPR);
	    
        super.invalidate();
        
    }

    /**
     * Return <code>true</code> iff the {@link FunctionNode} makes use of a
     * {@link FunctionRegistry#BOUND} operator.
     * 
     * @return <code>true</code>iff it uses <code>BOUND()</code>
     * 
     * TODO Unit test.
     */
    public boolean isBound() {

        if (FunctionRegistry.BOUND.equals(getFunctionURI()))
            return true;

        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = get(i);

            if (child instanceof FunctionNode) {

                if (!((FunctionNode) child).isBound()) {

                    return true;

                }

            }

        }

        return false;
        
    }

    /*
     * Factory methods.
     */

    /**
     * Return <code>t1 AND t2</code>.
     */
    static public FunctionNode AND(final ValueExpressionNode t1,
            final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.AND, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 OR t2</code>.
     */
    static public FunctionNode OR(final ValueExpressionNode t1,
            final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.OR, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }
    
    /**
     * Return <code>t1 OR t2</code>.
     */
    static public FunctionNode NOT(final ValueExpressionNode inner) {

        return new FunctionNode(FunctionRegistry.NOT, null/* scalarValues */,
                new ValueExpressionNode[] { inner });

    }

    /**
     * Return <code>t1 + t2</code> (aka ADD).
     */
    static public FunctionNode add(final ValueExpressionNode t1, 
             final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.ADD, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 - t2</code> (aka SUBTRACT).
     */
    static public FunctionNode subtract(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.SUBTRACT, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>sameTerm(t1,t2)</code> (aka EQ).
     */
    static public FunctionNode sameTerm(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.SAME_TERM, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 = t2</code> (aka EQ aka RDFTERM-EQUALS).
     */
    static public FunctionNode EQ(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.EQ, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 != t2</code>
     */
    static public FunctionNode NE(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.NE, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 < t2</code>
     */
    static public FunctionNode LT(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.LT, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 > t2</code>
     */
    static public FunctionNode GT(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.GT, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }
    
    /**
     * Return <code>t1 <= t2</code>
     */
    static public FunctionNode LE(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.LE, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>t1 >= t2</code>
     */
    static public FunctionNode GE(final ValueExpressionNode t1, 
          final ValueExpressionNode t2) {

        return new FunctionNode(FunctionRegistry.GE, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

    /**
     * Return <code>min(v1,v2)</code>
     */
    static public FunctionNode MIN(
    		final ValueExpressionNode v1, final ValueExpressionNode v2) {

        return new FunctionNode(FunctionRegistry.MIN, null/* scalarValues */,
                new ValueExpressionNode[] { v1, v2 });

    }
    
    /**
     * Return <code>max(v1,v2)</code>
     */
    static public FunctionNode MAX(
    		final ValueExpressionNode v1, final ValueExpressionNode v2) {

        return new FunctionNode(FunctionRegistry.MAX, null/* scalarValues */,
                new ValueExpressionNode[] { v1, v2 });

    }
    
    /**
     * Return a binary function <code>op(t1,t2)</code>
     */
    static public FunctionNode binary(final URI uri, final TermNode t1,
            final TermNode t2) {

        return new FunctionNode(uri, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }


    /**
     * Provides a pretty print representation with recursive descent.
     */
	@Override
	public String toString(int i) {

        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        final Integer bopId = (Integer) getProperty(Annotations.BOP_ID);
        if (bopId != null) {
            sb.append("[" + bopId + "]");
        }
        sb.append("(");
        int nwritten = 0;
        final Iterator<BOp> itr = argIterator();
        while(itr.hasNext()) {
            final BOp t = itr.next();
            if (nwritten > 0)
                sb.append(',');
            if (t == null) {
                sb.append("<null>");
            } else {
                sb.append(((IValueExpressionNode)t).toString(i+1));
            }
            nwritten++;
        }
        sb.append(")");
        annotationsToString(sb, i);
        return sb.toString();
	}
	
}
