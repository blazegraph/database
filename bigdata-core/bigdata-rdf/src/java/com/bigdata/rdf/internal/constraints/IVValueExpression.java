/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.bop.AbstractAccessPathOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * A specialized IValueExpression that evaluates to an IV.  The inputs are
 * usually, but not strictly limited to, IVs as well.  This class also contains
 * many useful helper methods for evaluation, including providing access to
 * the BigdataValueFactory and LexiconConfiguration.
 */
public abstract class IVValueExpression<T extends IV> extends BOpBase 
		implements IValueExpression<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7068219781217676085L;

    public interface Annotations extends BOpBase.Annotations {

        /**
         * The namespace of the lexicon.
         */
        public String NAMESPACE = IVValueExpression.class.getName()
                + ".namespace";
        
        /**
         * The timestamp of the query.
         */
        public String TIMESTAMP = IVValueExpression.class.getName()
                + ".timestamp";
        
    }

    /**
     * 
     * Note: The double-checked locking pattern <em>requires</em> the keyword
     * <code>volatile</code>.
     */
    private transient volatile BigdataValueFactory vf;

    /**
     * Note: The double-checked locking pattern <em>requires</em> the keyword
     * <code>volatile</code>.
     */
    private transient volatile ILexiconConfiguration<BigdataValue> lc;

    /**
     * Used by subclasses to create the annotations object from the global
     * annotations and the custom annotations for the particular VE.
     * 
     * @param globals
     *            The global annotations, including the lexicon namespace.
     * @param anns
     * 			  Any additional custom annotations.            
     */
    protected static Map<String, Object> anns(
    		final GlobalAnnotations globals, final NV... anns) {
    	
    	final int size = 2 + (anns != null ? anns.length : 0);
    	
    	final NV[] nv = new NV[size];
    	nv[0] = new NV(Annotations.NAMESPACE, globals.lex);
    	nv[1] = new NV(Annotations.TIMESTAMP, globals.timestamp);
    	
    	if (anns != null) {
    		for (int i = 0; i < anns.length; i++) {
    			nv[i+2] = anns[i];
    		}
    	}
    	
    	return NV.asMap(nv);
    	
    }
    
    /**
     * Zero arg convenience constructor.
     * 
     * @param globals
     *            The global annotations, including the lexicon namespace.
     * @param anns
     * 			  Any additional custom annotations.            
     */
    public IVValueExpression(final GlobalAnnotations globals, final NV... anns) {
        
        this(BOpBase.NOARGS, anns(globals, anns));
        
    }
    
    /**
     * One arg convenience constructor.
     * 
     * @param globals
     *            The global annotations, including the lexicon namespace.
     * @param anns
     * 			  Any additional custom annotations.            
     */
    public IVValueExpression(final IValueExpression<? extends IV> x, 
    		final GlobalAnnotations globals, final NV... anns) {
        
        this(new BOp[] { x }, anns(globals, anns));
        
    }
    
	/**
     * Required shallow copy constructor.
     */
    public IVValueExpression(final BOp[] args, final Map<String, Object> anns) {
        super(args, anns);
        
        if (areGlobalsRequired()) {
        	
	        if (getProperty(Annotations.NAMESPACE) == null) {
	        	throw new IllegalArgumentException("must set the lexicon namespace");
	        }
	        
	        if (getProperty(Annotations.TIMESTAMP) == null) {
	        	throw new IllegalArgumentException("must set the query timestamp");
	        }
	        
        }
        
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public IVValueExpression(final IVValueExpression<T> op) {
        super(op);
        
        if (areGlobalsRequired()) {
        	
	        if (getProperty(Annotations.NAMESPACE) == null) {
	        	throw new IllegalArgumentException("must set the lexicon namespace");
	        }
	        
	        if (getProperty(Annotations.TIMESTAMP) == null) {
	        	throw new IllegalArgumentException("must set the query timestamp");
	        }
	        
        }
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public IValueExpression<? extends IV> get(final int i) {

        try {

            return (IValueExpression<? extends IV>) super.get(i);

        } catch (ClassCastException ex) {

            throw new SparqlTypeErrorException();

        }

    }
    
    /**
     * Returns <code>true</code> unless overridden, meaning the 
     * {@link GlobalAnnotations} are required for this value expression
     * (certain boolean value expressions do not require them).  Global
     * annotations allow the method getValueFactory and getLexiconConfiguration
     * to work.
     */
    protected boolean areGlobalsRequired() {
        
        return true;
        
    }
    
    /**
     * Return the {@link BigdataValueFactory} for the {@link LexiconRelation}.
     * <p>
     * Note: This is lazily resolved and then cached.
     */
    protected BigdataValueFactory getValueFactory() {

        if (vf == null) {
        
            synchronized (this) {
            
                if (vf == null) {
                    
                    final String namespace = getNamespace();
                    
                    vf = BigdataValueFactoryImpl.getInstance(namespace);
                    
                }

            }
        
        }
        
        return vf;
        
    }

    /**
     * Return the namespace of the {@link LexiconRelation}.
     */
    protected String getNamespace() {
        
        return (String) getRequiredProperty(Annotations.NAMESPACE);
        
    }

    /**
     * Return the timestamp for the query.
     */
    protected long getTimestamp() {
        
        return (Long) getRequiredProperty(Annotations.TIMESTAMP);
        
    }

    /**
     * Return the {@link ILexiconConfiguration}. The result is cached. The cache
     * it will not be serialized when crossing a node boundary.
     * <p>
     * Note: It is more expensive to obtain the {@link ILexiconConfiguration}
     * than the {@link BigdataValueFactory} because we have to resolve the
     * {@link LexiconRelation} view. However, this happens once per function bop
     * in a query per node, so the cost is amortized.
     * 
     * @param bset
     *            A binding set flowing through this operator.
     * 
     * @throws ContextNotAvailableException
     *             if the context was not accessible on the solution.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/513">
     *      Expose the LexiconConfiguration to function BOPs </a>
     * 
     *      TODO This locates the last committed view of the
     *      {@link LexiconRelation}. Unlike {@link AbstractAccessPathOp}, the
     *      {@link LiteralBooleanBOp} does not declares the TIMESTAMP of the
     *      view. We really need that annotation to recover the right view of
     *      the {@link LexiconRelation}. However, the
     *      {@link ILexiconConfiguration} metadata is immutable so it is Ok to
     *      use the last committed time for that view. This is NOT true of if we
     *      were going to read data from the {@link LexiconRelation}.
     */
    protected ILexiconConfiguration<BigdataValue> getLexiconConfiguration(
            final IBindingSet bset) {

        if (lc == null) {

            synchronized (this) {

                if (lc == null) {

                    if (!(bset instanceof ContextBindingSet)) {

                        /*
                         * This generally indicates a failure to propagate the
                         * context wrapper for the binding set to a new binding
                         * set during a copy (projection), bind (join), etc. It
                         * could also indicate a failure to wrap binding sets
                         * when they are vectored into an operator after being
                         * received at a node on a cluster.
                         */

                        throw new ContextNotAvailableException(this.toString());

                    }

                    final BOpContextBase context = ((ContextBindingSet) bset)
                            .getBOpContext();

                    final String namespace = getNamespace();
                    
//                    final long timestamp = ITx.READ_COMMITTED;
                    final long timestamp = getTimestamp();

                    final LexiconRelation lex = (LexiconRelation) context
                		.getResource(namespace, timestamp);

                    lc = lex.getLexiconConfiguration();

                    if (vf != null) {

                        // Available as an attribute here.
                        vf = lc.getValueFactory();

                    }

                }
                
            }
            
        }
        
        return lc;
        
    }

    /**
     * Return the {@link Literal} for the {@link IV}.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return The {@link Literal}.
     * 
     * @throws SparqlTypeErrorException
     *             if the argument is <code>null</code>.
     * @throws SparqlTypeErrorException
     *             if the argument does not represent a {@link Literal}.
     * @throws NotMaterializedException
     *             if the {@link IVCache} is not set and the {@link IV} can not
     *             be turned into a {@link Literal} without an index read.
     */
    @SuppressWarnings("rawtypes")
    final static public Literal asLiteral(final IV iv) {

        if (iv == null)
            throw new SparqlTypeErrorException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();
        
        if (iv.isInline() && !iv.needsMaterialization()) {

    		return (Literal) iv;
        		
        } else if (iv.hasValue()) {

            return (BigdataLiteral) iv.getValue();

        } else {

            throw new NotMaterializedException();

        }

    }

    /**
     * Return the {@link Value} for the {@link IV}.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return The {@link Value}.
     * 
     * @throws SparqlTypeErrorException
     *             if the argument is <code>null</code>.
     * @throws NotMaterializedException
     *             if the {@link IVCache} is not set and the {@link IV} can not
     *             be turned into a {@link Literal} without an index read.
     */
    @SuppressWarnings("rawtypes")
    final static public Value asValue(final IV iv) {

        if (iv == null)
            throw new SparqlTypeErrorException();

        if (iv.isInline() && !iv.needsMaterialization()) {

    		return (Value) iv;
        		
        } else if (iv.hasValue()) {

            return (BigdataValue) iv.getValue();

        } else {

            throw new NotMaterializedException();

        }

    }

    /**
     * Return the {@link String} label for the {@link IV}.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return {@link Literal#getLabel()} for that {@link IV}.
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>.
     *             
     * @throws NotMaterializedException
     *             if the {@link IVCache} is not set and the {@link IV} must be
     *             materialized before it can be converted into an RDF
     *             {@link Value}.
     */
    @SuppressWarnings("rawtypes")
    final protected String literalLabel(final IV iv)
            throws NotMaterializedException {

    	return asLiteral(iv).getLabel();

    }
    
    /**
     * Get the function argument (a value expression) and evaluate it against
     * the source solution. The evaluation of value expressions is recursive.
     * 
     * @param i
     *            The index of the function argument ([0...n-1]).
     * @param bs
     *            The source solution.
     * 
     * @return The result of evaluating that argument of this function.
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not the index of an operator for this
     *             operator.
     * 
     * @throws SparqlTypeErrorException
     *             if the value expression at that index can not be evaluated.
     * 
     * @throws NotMaterializedException
     *             if evaluation encountered an {@link IV} whose {@link IVCache}
     *             was not set when the value expression required a materialized
     *             RDF {@link Value}.
     */
    protected IV getAndCheckLiteral(final int i, final IBindingSet bs)
            throws SparqlTypeErrorException, NotMaterializedException {
        
        final IV<?, ?> iv = getAndCheckBound(i, bs);
        
        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();
        
        if (iv.needsMaterialization() && !iv.hasValue())
            throw new NotMaterializedException();

        return iv;

    }
    
    /**
     * Get the function argument (a value expression) and evaluate it against
     * the source solution. The evaluation of value expressions is recursive.
     * 
     * @param i
     *            The index of the function argument ([0...n-1]).
     * @param bs
     *            The source solution.
     * 
     * @return The result of evaluating that argument of this function.
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not the index of an operator for this
     *             operator.
     * 
     * @throws SparqlTypeErrorException
     *             if the value expression at that index can not be evaluated.
     */
    protected IV getAndCheckBound(final int i, final IBindingSet bs)
            throws SparqlTypeErrorException, NotMaterializedException {

        final IV<?, ?> iv = get(i).get(bs);
        
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();

        return iv;

    }
    
    /**
     * Combination of {@link #getAndCheckLiteral(int, IBindingSet)} and
     * {@link #asLiteral(IV)}.
     */
    protected Literal getAndCheckLiteralValue(final int i, final IBindingSet bs) {
    	
    	return asLiteral(getAndCheckLiteral(i, bs));
    	
    }
    
    /**
     * Return an {@link IV} for the {@link Value}.
     * 
     * @param value
     *            The {@link Value}.
     * @param bs
     *            The bindings on the solution are ignored, but the reference is
     *            used to obtain the {@link ILexiconConfiguration}.
     *            
     * @return An {@link IV} for that {@link Value}.
     */
    final protected IV asIV(final Value value, final IBindingSet bs) {

        /*
         * Convert to a BigdataValue if not already one.
         * 
         * If it is a BigdataValue, then make sure that it is associated with
         * the namespace for the lexicon relation.
         */
        
    	if (value instanceof IV) {
    		
    		return (IV) value;
    		
    	}
    	
        final BigdataValue v = getValueFactory().asValue(value);
        
        return asIV(v, bs);

    }
    
    /**
     * Return an {@link IV} for the {@link Value}.
     * <p>
     * If the supplied BigdataValue has an IV, cache the BigdataValue on the
     * IV and return it.  If there is no IV, first check the LexiconConfiguration
     * to see if an inline IV can be created.  As a last resort, create a
     * "dummy IV" (a TermIV with a "0" reference) for the value.
     * 
     * @param value
     * 			The {@link BigdataValue}
     * @param bs
     *            The bindings on the solution are ignored, but the reference is
     *            used to obtain the {@link ILexiconConfiguration}.
     *            
     * @return An {@link IV} for that {@link BigdataValue}.
     */
    protected IV asIV(final BigdataValue value, final IBindingSet bs) {
    	
    	// first check to see if there is already an IV
    	IV iv = value.getIV();
    	if (iv != null) {
			// cache the value
    		iv.setValue(value);
    		return iv;
    	}
    	
		@SuppressWarnings("rawtypes")
		ILexiconConfiguration lc = null;
		try {
			lc = getLexiconConfiguration(bs);
		} catch (ContextNotAvailableException ex) {
			// can't access the LC (e.g. some test cases)
			// log.warn?
		}
		
		// see if we happen to have the value in the vocab or can otherwise
    	// create an inline IV for it
		if (lc != null) {
			iv = lc.createInlineIV(value);
			if (iv != null) {
				// cache the value only if it's something that would require materialization
				if (iv.needsMaterialization()) {
					iv.setValue(value);
				}
				return iv;
			}
		}
		
		// toDummyIV will also necessarily cache the value since the IV
		// doesn't mean anything
		return DummyConstantNode.toDummyIV(value);

    }

}
