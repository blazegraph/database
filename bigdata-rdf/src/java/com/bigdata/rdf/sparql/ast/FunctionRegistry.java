package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AndBOp;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.DatatypeBOp;
import com.bigdata.rdf.internal.constraints.EBVBOp;
import com.bigdata.rdf.internal.constraints.FalseBOp;
import com.bigdata.rdf.internal.constraints.FuncBOp;
import com.bigdata.rdf.internal.constraints.IsBNodeBOp;
import com.bigdata.rdf.internal.constraints.IsBoundBOp;
import com.bigdata.rdf.internal.constraints.IsLiteralBOp;
import com.bigdata.rdf.internal.constraints.IsURIBOp;
import com.bigdata.rdf.internal.constraints.LangBOp;
import com.bigdata.rdf.internal.constraints.LangMatchesBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.NotBOp;
import com.bigdata.rdf.internal.constraints.OrBOp;
import com.bigdata.rdf.internal.constraints.RegexBOp;
import com.bigdata.rdf.internal.constraints.SameTermBOp;
import com.bigdata.rdf.internal.constraints.SparqlTypeErrorBOp;
import com.bigdata.rdf.internal.constraints.StrBOp;
import com.bigdata.rdf.internal.constraints.TrueBOp;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;

public class FunctionRegistry {

	private static Map<URI, Factory> factories = new LinkedHashMap<URI, Factory>();
	
	private static final String SPARQL_FUNCTIONS = "http://www.w3.org/2006/sparql-functions#";
	private static final String XPATH_FUNCTIONS = "http://www.w3.org/2005/xpath-functions";
	
    public static final URI BOUND = new URIImpl(SPARQL_FUNCTIONS+"bound");       
    public static final URI IS_LITERAL = new URIImpl(SPARQL_FUNCTIONS+"isLiteral");
    public static final URI IS_BLANK = new URIImpl(SPARQL_FUNCTIONS+"isBlank");
    public static final URI IS_URI = new URIImpl(SPARQL_FUNCTIONS+"isURI");
    public static final URI STR = new URIImpl(SPARQL_FUNCTIONS+"str");
    public static final URI LANG = new URIImpl(SPARQL_FUNCTIONS+"lang");
    public static final URI DATATYPE = new URIImpl(SPARQL_FUNCTIONS+"datatype");
    public static final URI LANG_MATCHES = new URIImpl(SPARQL_FUNCTIONS+"langMatches");
    public static final URI REGEX = new URIImpl(XPATH_FUNCTIONS+"matches");
    public static final URI OR = new URIImpl(SPARQL_FUNCTIONS+"logical-or");
    public static final URI AND = new URIImpl(SPARQL_FUNCTIONS+"logical-and");
    public static final URI NOT = new URIImpl(XPATH_FUNCTIONS+"not");
    public static final URI SAME_TERM = new URIImpl(SPARQL_FUNCTIONS+"sameTerm");
    
    public static final URI EQ = new URIImpl(XPATH_FUNCTIONS+"equal");
    public static final URI NE = new URIImpl(XPATH_FUNCTIONS+"not-equal");
    public static final URI GT = new URIImpl(XPATH_FUNCTIONS+"greater-than");
    public static final URI GE = new URIImpl(XPATH_FUNCTIONS+"greater-than-or-equal");
    public static final URI LT = new URIImpl(XPATH_FUNCTIONS+"less-than");
    public static final URI LE = new URIImpl(XPATH_FUNCTIONS+"less-than-or-equal");

    public static final URI ADD = new URIImpl(XPATH_FUNCTIONS+"numeric-add");
    public static final URI SUBTRACT = new URIImpl(XPATH_FUNCTIONS+"numeric-subtract");
    public static final URI MULTIPLY = new URIImpl(XPATH_FUNCTIONS+"numeric-multiply");
    public static final URI DIVIDE = new URIImpl(XPATH_FUNCTIONS+"numeric-divide");

    public static final URI XSD_BOOL = XMLSchema.BOOLEAN;
    public static final URI XSD_DT = XMLSchema.DATETIME;
    public static final URI XSD_DEC = XMLSchema.DECIMAL;
    public static final URI XSD_DBL = XMLSchema.DOUBLE;
    public static final URI XSD_FLT = XMLSchema.FLOAT;
    public static final URI XSD_INT = XMLSchema.INTEGER;
    public static final URI XSD_STR = XMLSchema.STRING;
    
    static {
		
		// add the bigdata built-ins
    	
		add(BOUND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, VarNode.class);
				
				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new IsBoundBOp(var);
				
			}
		});
		
		add(IS_LITERAL, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, VarNode.class);
				
				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new IsLiteralBOp(var);
				
			}
		});
		
		add(IS_BLANK, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, VarNode.class);
				
				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new IsBNodeBOp(var);
				
			}
		});
		
		add(IS_URI, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, VarNode.class);
				
				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new IsURIBOp(var);
				
			}
		});
		
		add(STR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, VarNode.class);
				
				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new StrBOp(var, lex);
				
			}
		});
		
		add(LANG, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, ValueExpressionNode.class);
				
				final IValueExpression<? extends IV> ve = 
					args[0].getValueExpression();
				return new LangBOp(ve, lex);
				
			}
		});
		
		add(DATATYPE, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, ValueExpressionNode.class);
				
				final IValueExpression<? extends IV> ve = 
					args[0].getValueExpression();
				return new DatatypeBOp(ve, lex);
				
			}
		});
		
		add(LANG_MATCHES, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, 
						ValueExpressionNode.class, ValueExpressionNode.class);
				
				final IValueExpression<? extends IV> left = 
					args[0].getValueExpression();
				final IValueExpression<? extends IV> right = 
					args[1].getValueExpression();
				return new LangMatchesBOp(left, right);
				
			}
		});
		
		add(REGEX, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, 
						ValueExpressionNode.class, ValueExpressionNode.class);
				
				final IValueExpression<? extends IV> var = 
					args[0].getValueExpression();
				final IValueExpression<? extends IV> pattern = 
					args[1].getValueExpression();
				
				if (args.length == 2) {
					
					return new RegexBOp(var, pattern);
					
				} else {

					final IValueExpression<? extends IV> flags =
						args[2].getValueExpression();
					
					return new RegexBOp(var, pattern, flags);
					
				}
				
			}
		});
		
		add(AND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, 
						ValueExpressionNode.class, ValueExpressionNode.class);
				
				IValueExpression<? extends IV> left = 
					args[0].getValueExpression();
				if (!(left instanceof XSDBooleanIVValueExpression)) {
					left = new EBVBOp(left);
				}
				IValueExpression<? extends IV> right = 
					args[1].getValueExpression();
				if (!(right instanceof XSDBooleanIVValueExpression)) {
					right = new EBVBOp(right);
				}
				
				return new AndBOp(left, right);
				
			}
		});
		
		add(OR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, 
						ValueExpressionNode.class, ValueExpressionNode.class);
				
				IValueExpression<? extends IV> left = 
					args[0].getValueExpression();
				if (!(left instanceof XSDBooleanIVValueExpression)) {
					left = new EBVBOp(left);
				}
				IValueExpression<? extends IV> right = 
					args[1].getValueExpression();
				if (!(right instanceof XSDBooleanIVValueExpression)) {
					right = new EBVBOp(right);
				}
				
				return new OrBOp(left, right);
				
			}
		});
		
		add(NOT, new Factory() {
			public IValueExpression<? extends IV> create(final String lex, 
					final ValueExpressionNode... args) {
				
				checkArgs(args, ValueExpressionNode.class);
				
				IValueExpression<? extends IV> arg = 
					args[0].getValueExpression();
				if (!(arg instanceof XSDBooleanIVValueExpression)) {
					arg = new EBVBOp(arg);
				}
				
				return new NotBOp(arg);
				
			}
		});
		
		add(SAME_TERM, new SameTermFactory());
		
		add(EQ, new CompareFactory(CompareOp.EQ));
		
		add(NE, new CompareFactory(CompareOp.NE));
		
		add(GT, new CompareFactory(CompareOp.GT));
		
		add(GE, new CompareFactory(CompareOp.GE));
		
		add(LT, new CompareFactory(CompareOp.LT));
		
		add(LE, new CompareFactory(CompareOp.LE));
		
		add(ADD, new MathFactory(MathOp.PLUS));
		
		add(SUBTRACT, new MathFactory(MathOp.MINUS));
		
		add(MULTIPLY, new MathFactory(MathOp.MULTIPLY));
		
		add(DIVIDE, new MathFactory(MathOp.DIVIDE));
		
	    add(XSD_BOOL, new CastFactory(XMLSchema.BOOLEAN.toString()));

	    add(XSD_DT, new CastFactory(XMLSchema.DATETIME.toString()));

	    add(XSD_DEC, new CastFactory(XMLSchema.DECIMAL.toString()));

	    add(XSD_DBL, new CastFactory(XMLSchema.DOUBLE.toString()));

	    add(XSD_FLT, new CastFactory(XMLSchema.FLOAT.toString()));

	    add(XSD_INT, new CastFactory(XMLSchema.INTEGER.toString()));

	    add(XSD_STR, new CastFactory(XMLSchema.STRING.toString()));

    }
    
    public static final void checkArgs(final ValueExpressionNode[] args, 
    		final Class... types) {
    	
    	if (args.length < types.length) {
    		throw new IllegalArgumentException("wrong # of args");
    	}
    	
    	for (int i = 0; i < args.length; i++) {
    		if (types[i].isAssignableFrom(args[i].getClass())) {
    			throw new IllegalArgumentException(
    					"wrong type for arg# " + i + ": " + args[i].getClass());
    		}
    	}
    	
    }
	
	public static final IValueExpression<? extends IV> toVE(
			final String lex, final URI functionURI, 
			final ValueExpressionNode... args) {

		final Factory f = factories.get(functionURI);

		if (f == null) {
			throw new IllegalArgumentException("unknown function: " + functionURI);
		}
		
		return f.create(lex, args);
		
	}
	
	public static final void add(final URI functionURI, final Factory factory) {
		
		factories.put(functionURI, factory);
		
	}
	
	public static interface Factory {
		
		IValueExpression<? extends IV> create(
				final String lex, final ValueExpressionNode... args);
		
	}
	
	public static class CompareFactory implements Factory {
		
		private final CompareOp op;
		
		public CompareFactory(final CompareOp op) {
			this.op = op;
		}
		
		public IValueExpression<? extends IV> create(
				final String lex, final ValueExpressionNode... args) {
			
			checkArgs(args, 
					ValueExpressionNode.class, ValueExpressionNode.class);
			
			final IValueExpression<? extends IV> left = 
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right = 
				args[1].getValueExpression();
			
	    	if (left.equals(right)) {
	    		
	    		if (op == CompareOp.EQ) {
	    			
	    			return TrueBOp.INSTANCE;
	    			
	    		} else {
	    			
	    			return FalseBOp.INSTANCE;
	    			
	    		}
	    		
	    	}
	    	
	    	/*
	         * If we are dealing with a URI constant:
	         * 
	         * We can use SparqlTypeErrorBOp for any operator other than EQ, NE
	         * 
	         * If it's a real term:
	         * 
	         * We can use SameTermBOp
	         * 
	         * If it's not a real term:
	         * 
	         * The only time we actually need to evaluate this is when the other
	         * operand is a DatatypeBOp. All other times, we can return FalseBOp for 
	         * EQ and TrueBOp for NE.
	         * 
	    	 */

	    	if (left instanceof Constant) {
	    		
	    		final IV iv = ((Constant<? extends IV>) left).get();
	    		
	    		if (iv.isURI()) {
	    			
	    	    	if (!(op == CompareOp.EQ || op == CompareOp.NE)) {
	    	    		
//	    	    		return new SparqlTypeErrorBOp(new CompareBOp(left, right, op));
	    	    		return SparqlTypeErrorBOp.INSTANCE;
	    	    		
	    	    	}
	    	    	
	    	    	/* 
	    	    	 * DatatypeBOp is the only bop that can cast things to a URI,
	    	    	 * and that URI might not be in the database. Thus there are no
	    	    	 * optimizations we can do: we can't check term equality, and
	    	    	 * we can't assume automatic false or true.
	    	    	 */
		    		if (!(right instanceof DatatypeBOp)) {
		    			
		    			if (!iv.isNullIV()) {
	    	    		
			    			// if it's a real term we can use SameTermBOp
		    				return new SameTermBOp(left, right, op);
	    	    		
		    			} else {

		    				// if it's not a real term then we can substitute false
		    				// for EQ and true for NE
		    				if (op == CompareOp.EQ) {
		        			
//	    	        			return new FalseBOp(new CompareBOp(left, right, op));
		    					return FalseBOp.INSTANCE;
		        			
		    				} else {
		        			
//	    	        			return new TrueBOp(new CompareBOp(left, right, op));
		    					return TrueBOp.INSTANCE;
		        			
		    				}
	    	    			
		    			}
	                
		    		}
	    			
	    		}
	    		
	    	}
	    	
	    	if (right instanceof Constant) {
	    		
	    		final IV iv = ((Constant<? extends IV>) right).get();
	    		
	    		if (iv.isURI()) {
	    			
	    	    	if (!(op == CompareOp.EQ || op == CompareOp.NE)) {
	    	    		
//	    	    		return new SparqlTypeErrorBOp(new CompareBOp(left, right, op));
	    	    		return SparqlTypeErrorBOp.INSTANCE;
	    	    		
	    	    	}
	    	    	
	    	    	/* 
	    	    	 * DatatypeBOp is the only bop that can cast things to a URI,
	    	    	 * and that URI might not be in the database. Thus there are no
	    	    	 * optimizations we can do: we can't check term equality, and
	    	    	 * we can't assume automatic false or true.
	    	    	 */
		    		if (!(left instanceof DatatypeBOp)) {
		    			
		    			if (!iv.isNullIV()) {
	    	    		
			    			// if it's a real term we can use SameTermBOp
		    				return new SameTermBOp(left, right, op);
	    	    		
		    			} else {

		    				// if it's not a real term then we can substitute false
		    				// for EQ and true for NE
		    				if (op == CompareOp.EQ) {
		        			
//	    	        			return new FalseBOp(new CompareBOp(left, right, op));
		    					return FalseBOp.INSTANCE;
		        			
		    				} else {
		        			
//	    	        			return new TrueBOp(new CompareBOp(left, right, op));
		    					return TrueBOp.INSTANCE;
		        			
		    				}
	    	    			
		    			}
	                
		    		}
	    			
	    		}
	    		
	    	}
	    	
	        return new CompareBOp(left, right, op);
			
		}
		
	}
	
	public static class SameTermFactory implements Factory {
		
		public IValueExpression<? extends IV> create(
				final String lex, final ValueExpressionNode... args) {
			
			checkArgs(args, 
					ValueExpressionNode.class, ValueExpressionNode.class);
			
			final IValueExpression<? extends IV> left = 
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right = 
				args[1].getValueExpression();
			
			/*
			 * If a constant operand in the SameTerm op uses a value not found in
			 * the database, we end up in one of two possible situations:
			 * 
			 * 1. If the constant operand is a URI, there is no possible way for
			 * SameTerm to evaluate to true, unless the other operand is a
			 * DatatypeBOp. (This is because DatatypeBOp will stamp phony TermId IVs
			 * for the datatypes for inline numerics and math operations.) So if the
			 * other operand is not a DatatypeBOp, we can just return a FalseBOp
			 * that wraps the SameTermBOp that would have happened (this wrapping is
			 * purely informational).
			 * 
			 * 2. If the constant operand is not a URI, we need to defer to the
			 * CompareBOp, which knows how to do value comparisons. SameTermBOp only
			 * works on IVs.
			 */
	    	if (left instanceof Constant) {
	    		
	    		final IV iv = ((Constant<? extends IV>) left).get();
	            if (!iv.isInline() && iv.isNullIV()) {
//	    		if (iv.isTermId() && iv.getTermId() == TermId.NULL) {
	    			
	    			if (iv.isURI() && !(right instanceof DatatypeBOp)) {
	    				
//	    				return new FalseBOp(new SameTermBOp(left, right));
	    				return FalseBOp.INSTANCE;
	    				
	    			} else {
	    				
	    				return new CompareBOp(left, right, CompareOp.EQ);
	    				
	    			}
	    			
	    		}
	    		
	    	}
	    	
	    	if (right instanceof Constant) {
	    		
	    		final IV iv = ((Constant<? extends IV>) right).get();
	            if (!iv.isInline() && iv.isNullIV()) {
//	    		if (iv.isTermId() && iv.getTermId() == TermId.NULL) {
	    			
	    			if (iv.isURI() && !(left instanceof DatatypeBOp)) {
	    				
//	    				return new FalseBOp(new SameTermBOp(left, right));
	    				return FalseBOp.INSTANCE;
	    				
	    			} else {
	    				
	    				return new CompareBOp(left, right, CompareOp.EQ);
	    				
	    			}
	    			
	    		}
	    		
	    	}
	    	
	        return new SameTermBOp(left, right);
			
		}
		
	}
	
	public static class MathFactory implements Factory {
		
		private final MathOp op;
		
		public MathFactory(final MathOp op) {
			this.op = op;
		}
		
		public IValueExpression<? extends IV> create(
				final String lex, final ValueExpressionNode... args) {
			
			checkArgs(args, 
					ValueExpressionNode.class, ValueExpressionNode.class);
			
			final IValueExpression<? extends IV> left = 
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right = 
				args[1].getValueExpression();
			
			return new MathBOp(left, right, op);
			
		}
		
	}
	
	public static class CastFactory implements Factory {
		
		private final String uri;
		
		public CastFactory(final String uri) {
			this.uri = uri;
		}
		
		public IValueExpression<? extends IV> create(
				final String lex, final ValueExpressionNode... args) {

			final IValueExpression<? extends IV>[] bops = 
				new IValueExpression[args.length]; 
			
			for (int i = 0; i < args.length; i++) {
				bops[i] = args[i].getValueExpression();
			}
			
			return new FuncBOp(bops, uri, lex);
			
		}
		
	}

}
