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
 * @author mikepersonick
 */
public class OldBackupPathNode extends ASTBase {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4396141823074067307L;

	/**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public OldBackupPathNode(OldBackupPathNode op) {
        super(op);
    }
    
    /**
     * Required shallow copy constructor.
     */
	public OldBackupPathNode(final BOp[] args, final Map<String, Object> anns) {
		super(args, anns);
	}
	
	public OldBackupPathNode(final Path arg) {
		this(new BOp[] { arg }, BOp.NOANNS);
	}
	
	public Path getPath() {
		return (Path) get(0);
	}
	
	public static class Path extends ASTBase {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public Path(OldBackupPathNode op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public Path(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public Path(final PathAlternative arg) {
			this(new BOp[] { arg }, BOp.NOANNS);
		}
		
		public PathAlternative getPathAlternative() {
			return (PathAlternative) get(0);
		}
		
	}

	public static class PathAlternative extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathAlternative(OldBackupPathNode op) {
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
	
	public static class PathSequence extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathSequence(OldBackupPathNode op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathSequence(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathSequence(final PathEltOrInverse... args) {
			this(args, BOp.NOANNS);
			
			if (args == null || args.length == 0)
				throw new IllegalArgumentException("one or more args required");
		}
		
	}
	
	public static class PathEltOrInverse extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
	    interface Annotations extends ASTBase.Annotations {
	    	  
	        String INVERSE = "inverse";

	    }

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathEltOrInverse(OldBackupPathNode op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathEltOrInverse(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathEltOrInverse(final PathElt arg) {
			this(arg, false);
		}
		
		public PathEltOrInverse(final PathElt arg, final boolean inverse) {
			this(new BOp[] { arg }, NV.asMap(new NV(Annotations.INVERSE, inverse)));
		}
		
		public PathElt getPathElt() {
			return (PathElt) get(0);
		}
		
		public boolean inverse() {
			return (Boolean) super.getRequiredProperty(Annotations.INVERSE);
		}
		
	}
	
	public static class PathElt extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	    interface Annotations extends ASTBase.Annotations {
	    	  
	        String MOD = "mod";

	    }
	    
		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathElt(OldBackupPathNode op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathElt(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathElt(final PathPrimary arg) {
			this(new BOp[] { arg }, BOp.NOANNS);
		}
		
		public PathElt(final PathPrimary arg, final PathMod mod) {
			this(new BOp[] { arg }, NV.asMap(new NV(Annotations.MOD, mod)));
		}
		
		public PathPrimary getPrimary() {
			return (PathPrimary) get(0);
		}
		
		public PathMod getMod() {
			return (PathMod) super.getProperty(Annotations.MOD);
		}
		
	}
	
	public static class PathPrimary extends ASTBase {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathPrimary(OldBackupPathNode op) {
	        super(op);
	    }
	    
	    /**
	     * Required shallow copy constructor.
	     */
		public PathPrimary(final BOp[] args, final Map<String, Object> anns) {
			super(args, anns);
		}
		
		public PathPrimary(final ConstantNode arg) {
			this(new BOp[] { arg }, BOp.NOANNS);
		}
		
		public PathPrimary(final Path arg) {
			this(new BOp[] { arg }, BOp.NOANNS);
		}
		
		public PathPrimary(final PathNegatedPropertySet arg) {
			this(new BOp[] { arg }, BOp.NOANNS);
		}
		
		public Object get() {
			return get(0);
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
	    public PathNegatedPropertySet(OldBackupPathNode op) {
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
	    	  
	        String INVERSE = "inverse";

	    }

		/**
	     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	     */
	    public PathOneInPropertySet(OldBackupPathNode op) {
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
		
		public ConstantNode getArg() {
			return (ConstantNode) get(0);
		}
		
		public boolean inverse() {
			return (Boolean) super.getRequiredProperty(Annotations.INVERSE);
		}
		
	}
	
}
