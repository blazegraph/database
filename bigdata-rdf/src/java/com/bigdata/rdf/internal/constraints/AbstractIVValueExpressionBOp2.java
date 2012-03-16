/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Mar 14, 2012
 */

package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.bop.AbstractAccessPathOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

/**
 * Class introduced when adding the ability to resolve the
 * {@link ILexiconConfiguration}. This base class does not implement the
 * {@link INeedsMaterialization} interface and is extended by classes which do (
 * {@link AbstractLiteralBOp}) and by clases which do not (
 * {@link XSDBooleanIVValueExpression}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractIVValueExpressionBOp2<V extends IV> extends
        IVValueExpression<V> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends IVValueExpression.Annotations {

        /**
         * The namespace of the lexicon.
         */
        public String NAMESPACE = AbstractIVValueExpressionBOp2.class.getName()
                + ".namespace";
        
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
     * @param args
     * @param anns
     */
    public AbstractIVValueExpressionBOp2(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

    /**
     * @param op
     */
    public AbstractIVValueExpressionBOp2(final AbstractIVValueExpressionBOp2<V> op) {
        super(op);
    }

    /**
     * Returns <code>true</code> unless overridden.
     */
    protected boolean isLexiconNamespaceRequired() {
        
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

                        throw new UnsupportedOperationException(
                                "Context is not available.");

                    }

                    final BOpContext<?> context = ((ContextBindingSet) bset)
                            .getBOpContext();

                    final String namespace = getNamespace();

                    final LexiconRelation lex = (LexiconRelation) context
                            .getResource(namespace, ITx.READ_COMMITTED);

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
     * Return the {@link BigdataLiteral} for the {@link IV}.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return The {@link BigdataLiteral}.
     * 
     * @throws NotMaterializedException
     *             if the {@link IVCache} is not set and the {@link IV} can not
     *             be turned into a {@link Literal} without an index read.
     */
    @SuppressWarnings("rawtypes")
    final protected BigdataLiteral literalValue(final IV iv) {

        final BigdataValueFactory vf = getValueFactory();

        if (iv.isInline() && !iv.isExtension()) {

            final BigdataURI datatype = vf
                    .asValue(iv.getDTE().getDatatypeURI());

            return vf.createLiteral(((Value) iv).stringValue(), datatype);

        } else if (iv.hasValue()) {

            return ((BigdataLiteral) iv.getValue());

        } else {

            throw new NotMaterializedException();

        }

    }

    /**
     * Return an {@link IV} for the {@link Value}.
     * 
     * @param value
     *            The {@link Value}.
     * @param bsetIsIgnored
     *            The bindings on the solution are ignored, but the reference is
     *            used to obtain the {@link ILexiconConfiguration}.
     *            
     * @return An {@link IV} for that {@link Value}.
     */
    final protected IV asValue(final Value value,
            final IBindingSet bsetIsIgnored) {

        /*
         * Convert to a BigdataValue if not already one.
         * 
         * If it is a BigdataValue, then make sure that it is associated with
         * the namespace for the lexicon relation.
         */
        
        final BigdataValue v = getValueFactory().asValue(value);

        @SuppressWarnings("rawtypes")
        IV iv = null;

        // See if the IV is already set. 
        iv = v.getIV();

        if (iv == null) {

            // Resolve the lexicon configuration.
            final ILexiconConfiguration<BigdataValue> lexConf = getLexiconConfiguration(bsetIsIgnored);

            // Obtain an Inline IV iff possible.
            iv = lexConf.createInlineIV(v);

        }
        
        if (iv == null) {

            /*
             * Since we can not represent this using an Inline IV, we will stamp
             * a mock IV for the value.
             */

            iv = DummyConstantNode.toDummyIV(v);

        }

        return iv;

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
    final protected static String literalLabel(final IV iv)
            throws NotMaterializedException {

        if (iv.isInline() && !iv.isInline()) {

            return ((Value) iv).stringValue();

        } else if (iv.hasValue()) {

            return ((BigdataLiteral) iv.getValue()).getLabel();

        } else {

            throw new NotMaterializedException();

        }

    }

}
