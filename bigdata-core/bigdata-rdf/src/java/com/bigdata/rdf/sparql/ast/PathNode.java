package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.NV;

/**
 * AST Node used to represent a property path.
 * 
 * See http://www.w3.org/TR/sparql11-query/#rTriplesSameSubjectPath for details.
 * 
 * This class corresponds to "VerbPath".
 * 
 * A VerbPath (PathNode) has one Path.
 * VerbPath ::= Path
 * 
 * A Path has one PathAlternative.
 * Path ::= PathAlt
 * 
 * A PathAlternative has one or more PathSequences.
 * PathAlternative ::= PathSequence ( '|' PathSequence )*
 * 
 * A PathSequence has one or more PathEltOrInverses.
 * PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
 * 
 * A PathEltOrInverse has one PathElt and a boolean flag for inverse ('^').
 * PathEltOrInverse ::= PathElt | '^' PathElt
 * 
 * A PathElt has a PathPrimary and an optional PathMod.
 * PathElt ::= PathPrimary PathMod?
 * 
 * A PathPrimary has either an iri, a PathNegatedPropertySet, or a nested Path.
 * PathPrimary ::= iri | '!' PathNegatedPropertySet | '(' Path ')'
 * 
 * A PathMod is one from the enumeration '?', '*', or '+'. '?' means zero or
 * one (simple optional), '+' means one or more (fixed point), and '*' means
 * zero or more (optional fixed point).
 * PathMod ::= '?' | '*' | '+'
 * 
 * A PathNegatedPropertySet is zero or more PathOneInPropertySets.
 * PathNegatedPropertySet ::= PathOneInPropertySet |
 *     '(' (PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
 *     
 * A PathOneInPropertySet is an iri and a boolean flag for inverse ('^').
 * PathOneInPropertySet ::= iri | '^' iri
 * 
 * This model is actually flattened a bit by Sesame, so I followed Sesame's
 * model instead of the grammar.  In Sesame's model, the top level is 
 * PathAlternative, which contains one or more PathSequences.  Each 
 * PathSequence contains one or more PathElt.  Each PathElt has two modifiers -
 * the PathMod (for arbitrary length and zero length paths) and the inverse
 * modifier.  It also has the actual element - one of either a TermNode,
 * a nested path (a PathAlternative), a NegatedPropertySet, or a zero
 * length path.
 * 
 * @author mikepersonick
 */
public class PathNode extends ASTBase {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4396141823074067307L;

	/**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public PathNode(PathNode op) {
        super(op);
    }
    
    /**
     * Required shallow copy constructor.
     */
	public PathNode(final BOp[] args, final Map<String, Object> anns) {
		super(args, anns);
	}
	
	public PathNode(final PathAlternative arg) {
		this(new BOp[] { arg }, BOp.NOANNS);
	}
	
	/**
	 * The root of the property path is always a PathAlternative.
	 */
	public PathAlternative getPathAlternative() {
		return (PathAlternative) get(0);
	}

	/**
	 * Used to signify an OR (UNION) of multiple possible subpaths.
	 */
	public static class PathAlternative extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathAlternative(PathAlternative op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathAlternative(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathAlternative(final PathSequence... args) {
			this(args, BOp.NOANNS);
			
			if (args == null || args.length == 0)
				throw new IllegalArgumentException("one or more args required");
		}
		
	}

	/**
	 * A sequence of paths (JOINS).
	 */
	public static class PathSequence extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathSequence(PathSequence op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathSequence(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathSequence(final PathElt... args) {
			this(args, BOp.NOANNS);
			
			if (args == null || args.length == 0)
				throw new IllegalArgumentException("one or more args required");
		}
		
	}
	
	/**
	 * A specific path element.  Can be a nested path (a PathAlternative).
	 */
	public static class PathElt extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
	    interface Annotations extends ASTBase.Annotations {
	    	  
	    	/**
	    	 * The inverse modifier '^'.
	    	 */
	        String INVERSE = Annotations.class.getName() + ".inverse";

	    	/**
	    	 * The cardinality modifiers '?', '*', and '+'.
	    	 */
	        String MOD = Annotations.class.getName() + ".mod";

	    }
	    
		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathElt(PathElt op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathElt(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		/**
		 * @see {@link #PathNode(BOp, boolean, PathMod)}.
		 */
		public PathElt(final BOp arg) {
			this(arg, false);
		}
		
		/**
		 * @see {@link #PathNode(BOp, boolean, PathMod)}.
		 */
		public PathElt(final BOp arg, final boolean inverse) {
			this(arg, inverse, null);
		}
		
		/**
		 * @see {@link #PathNode(BOp, boolean, PathMod)}.
		 */
		public PathElt(final BOp arg, final PathMod mod) {
			this(arg, false, mod);
		}
		
		/**
		 * @param arg Must be one of the following types: 
		 *   <ul>
		 *   <li>{@link ConstantNode}</li>
		 *   <li>{@link PathAlternative}</li>
		 *   <li>{@link PathNegatedPropertySet}</li>
		 *   <li>{@link ZeroLengthPathNode}</li>
		 *   <ul>
		 */
		public PathElt(final BOp arg, final boolean inverse, final PathMod mod) {
			this(new BOp[] { arg }, NV.asMap(
					new NV(Annotations.INVERSE, inverse),
					new NV(Annotations.MOD, mod)));
			
			if (!(arg instanceof ConstantNode ||
					 arg instanceof PathAlternative ||
						arg instanceof PathNegatedPropertySet ||
						   arg instanceof ZeroLengthPathNode)) {
				throw new IllegalArgumentException();
			}
		}
		
		public boolean inverse() {
			return (Boolean) super.getRequiredProperty(Annotations.INVERSE);
		}
		
		public void setInverse(final boolean inverse) {
			super.setProperty(Annotations.INVERSE, inverse);
		}
		
		public PathMod getMod() {
			return (PathMod) super.getProperty(Annotations.MOD);
		}
		
		public void setMod(final PathMod mod) {
			super.setProperty(Annotations.MOD, mod);
		}
		
		public boolean isIRI() {
			return get(0) instanceof ConstantNode;
		}
		
		public boolean isNestedPath() {
			return get(0) instanceof PathAlternative;
		}
		
		public boolean isNegatedPropertySet() {
			return get(0) instanceof PathNegatedPropertySet;
		}
		
		public boolean isZeroLengthPath() {
			return get(0) instanceof ZeroLengthPathNode;
		}
		
	}
	
	public static enum PathMod {
		
		ZERO_OR_ONE("?"),
		
		ZERO_OR_MORE("*"),
		
		ONE_OR_MORE("+");

		final String mod;
		PathMod(final String mod) {
			this.mod = mod;
		}
		
		public String toString() {
			return mod;
		}
		
	}
	
	public static class PathNegatedPropertySet extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathNegatedPropertySet(PathNegatedPropertySet op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathNegatedPropertySet(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathNegatedPropertySet(final PathOneInPropertySet... args) {
			this(args, BOp.NOANNS);
		}
		
	}
	
	public static class PathOneInPropertySet extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	    interface Annotations extends ASTBase.Annotations {
	    	  
	        String INVERSE = Annotations.class.getName() + ".inverse";

	    }

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathOneInPropertySet(PathOneInPropertySet op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathOneInPropertySet(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathOneInPropertySet(final ConstantNode arg) {
			this(arg, false);
		}
		
		public PathOneInPropertySet(final ConstantNode arg, final boolean inverse) {
			this(new BOp[] { arg }, NV.asMap(new NV(Annotations.INVERSE, inverse)));
		}
		
		public boolean inverse() {
			return (Boolean) super.getRequiredProperty(Annotations.INVERSE);
		}
		
	}
	
}
