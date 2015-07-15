package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;

/**
 * Used to represent a variable in the AST.
 * <p>
 * Note: Unlike {@link Var}, this class does NOT support reference testing for
 * equality. Multiple {@link VarNode} instances MAY exist for the same
 * {@link IVariable}. You must either compare the value returned by
 * {@link #getVar()} (which is canonical) or use {@link #equals(Object)}.
 * 
 * @author mikepersonick
 */
public class VarNode extends TermNode {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2368082533964951789L;

    public interface Annotations extends TermNode.Annotations {
        
        /**
         * Annotation marks anonymous variables (a variable created by the
         * interpretation of the parse tree, AST, etc rather than one given
         * directly in the original query).
         */
        String ANONYMOUS = "anon";

        boolean DEFAULT_ANONYMOUS = false;

		/*
		 * Note: Annotating variables which are "SIDS" does not work out because
		 * we lack sufficient context in the parser to make this determination
		 * when the variable appears outside of the BIND( <<...>> as ?sid )
		 * context. In order for the parser to do this it would have to reason
		 * about the visibility of variables as well, which is just too much
		 * effort.
		 */
//        /**
//		 * Annotation marks a variable which is a statement identifier.
//		 * 
//		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
//		 *      Reification Done Right</a>
//		 */
//        String SID = "sid";
//
//        boolean DEFAULT_SID = false;

        /**
         * Annotation marks a variable which is actually the name of a solution
         * set.
         */
        String SOLUTION_SET = "solutionSet";

        boolean DEFAULT_SOLUTION_SET = false;

    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public VarNode(VarNode op) {
        super(op);
    }
    
    /**
     * Required shallow copy constructor.
     */
    public VarNode(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
    }

    @SuppressWarnings("unchecked")
    public VarNode(final String var) {
    	
    	this(Var.var(var));
    	
    }
    
	@SuppressWarnings("rawtypes")
    public VarNode(final IVariable<IV> var) {
		
		super(new BOp[]{var}, null);
		
//		setValueExpression(var);
		
	}
	
    /**
     * Return <code>true</code> if this is an anonymous variable (anonymous
     * variables are introduced during a rewrite of the query in which blank
     * nodes are replaced by anonymous variables). This marker is useful mainly
     * when reading the AST tree as an aid to understanding where a given
     * variable came from in the original query.
     */
    public boolean isAnonymous() {

        return getProperty(Annotations.ANONYMOUS, Annotations.DEFAULT_ANONYMOUS);

	}

    /**
     * Mark this as an anonymous variable (one introduced during a query rewrite
     * in place of a blank node).
     * 
     * @param anonymous
     */
	public void setAnonymous(final boolean anonymous) {

	    setProperty(Annotations.ANONYMOUS, anonymous);
	    
	    ((Var) getValueExpression()).setAnonymous(anonymous);
	    
	}

//	/**
//	 * Return <code>true</code> if this is an SID variable (a variable which
//	 * binds statement identifiers composed from triples). This marker is useful
//	 * mainly when reading the AST tree as an aid to understanding where a given
//	 * variable came from in the original query.
//	 */
//    public boolean isSID() {
//
//        return getProperty(Annotations.SID, Annotations.DEFAULT_SID);
//
//	}
//
//	/**
//	 * Mark this as an SID variable (one which represents a triple reference
//	 * pattern).
//	 * 
//	 * @param val
//	 */
//	public void setSID(final boolean val) {
//
//	    setProperty(Annotations.SID, val);
//	    
//	}

	/**
     * Return <code>true</code> if the variable represents a solution set name.
     */
    public boolean isSolutionSet() {

        return getProperty(Annotations.SOLUTION_SET,
                Annotations.DEFAULT_SOLUTION_SET);

    }

    /**
     * Mark this as a variable which actually conveys the name of a solution
     * set.
     * 
     * @param <code>true</code> if the variable represents a solution set name.
     */
    public void setSolutionSet(final boolean solutionSet) {

        setProperty(Annotations.SOLUTION_SET, solutionSet);

    }

	/**
	 * Return <code>true</code> iff the variable is <code>*</code>.
	 */
	public boolean isWildcard() {

		return getValueExpression().isWildcard();
	    
	}
	
	/**
	 * Strengthen return type.
	 */
	@SuppressWarnings("rawtypes")
    @Override
	public IVariable<IV> getValueExpression() {
		
		return (IVariable<IV>) super.getValueExpression();
		
	}
	
    /**
     * Hot-swap the IVariable underneath this VarNode.
     */
    @Override
    public void setValueExpression(IValueExpression<? extends IV> ve) {

        setArg(0, ve);
        
    }

    /**
	 * Overridden to mark metadata about variables.
	 */
    @Override
    public String toString() {

        return "VarNode(" + getValueExpression() + ")"
                + (isAnonymous() ? "[anonymous]" : "")
//                + (isSID() ? "[sid]" : "")
                + (isSolutionSet() ? "[solutionSet]" : "")
                ;

    }
    
    /**
     * @return a varnode containing a fresh (uniquely distinct) variable
     */
    public static VarNode freshVarNode() {
       return new VarNode((Var<IV>)Var.var());
    }
	
}
