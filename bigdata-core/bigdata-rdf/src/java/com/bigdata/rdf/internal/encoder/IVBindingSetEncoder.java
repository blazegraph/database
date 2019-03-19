/**

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
/*
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.Iterator;
import java.util.LinkedHashSet;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.MockedValueIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * A utility class for generating and processing compact representations of
 * {@link IBindingSet}s whose {@link IConstant}s are bound to {@link IV}s.
 * Individual {@link IV}s may be associated with a cached RDF {@link Value}.
 * 
 * As a contract, the class provides guarantees that the representation of a
 * given binding set is always the same. This makes it possible to use an
 * HTree based DistinctFilter over encoded binding sets.
 * <p>
 * Note: This implementation does NOT maintain the {@link IVCache} associations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IVBindingSetEncoder.java 6032 2012-02-16 12:48:04Z thompsonbry
 *          $
 */
public class IVBindingSetEncoder implements IBindingSetEncoder,
        IBindingSetDecoder {

    /**
     * Value factory
     */
    protected final BigdataValueFactory vf;
    
    /**
     * <code>true</code> iff this is in support of a DISTINCT filter.
     * <p>
     * Note: we do not maintain the {@link #ivCacheSchema} for a DISTINCT filter
     * since the original solutions flow through the filter.
     */
    protected final boolean filter;
    
    /**
     * The schema provides the order in which the {@link IV}[] for solutions
     * stored in the hash index are encoded in the {@link HTree}. {@link IV}
     * s which are not bound are modeled by a {@link TermId#NullIV}.
     * <p>
     * Note: In order to be able to encode/decode the schema based on the
     * lazy identification of the variables which appear in solutions the
     * {@link HTree} must store variable length {@link IV}[]s since new
     * variables may be discovered at any point.
     */
    private final LinkedHashSet<IVariable<?>> schema;
    

    
    /**
     * Used to encode the {@link IV}s.
     */
    private final IKeyBuilder keyBuilder;
    
    /**
     * @param store the backing store
     * @param filter
     *            <code>true</code> iff this is in support of a DISTINCT filter.
     *            <p>
     *            Note: we do not maintain the {@link #ivCacheSchema} for a
     *            DISTINCT filter since the original solutions flow through the
     *            filter.
     */
    public IVBindingSetEncoder(final BigdataValueFactory vf, final boolean filter) {

        this.vf = vf;
        
        this.filter = filter;
        
        this.schema = new LinkedHashSet<IVariable<?>>();

        this.keyBuilder = new ASCIIKeyBuilderFactory(128).getKeyBuilder();

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation does not maintain the {@link IVCache} associations.
     */
    @Override
    public boolean isValueCache() {
        
        return false;
        
    }

    /**
     * Build up the schema based on variables that are actually bound in the
     * observed bindings.
     * 
     * @param bset
     *            An observed binding set.
     */
    private void updateSchema(final IBindingSet bset) {

        @SuppressWarnings("rawtypes")
        final Iterator<IVariable> vitr = bset.vars();

        while (vitr.hasNext()) {

            schema.add(vitr.next());

        }

    }

    @Override
    public byte[] encodeSolution(final IBindingSet bset) {

        return encodeSolution(bset, true/* updateCache */);

    }
    
    @Override
    public byte[] encodeSolution(final IBindingSet bset,
            final boolean updateCache) {

        if(bset == null)
            throw new IllegalArgumentException();
        
        /*
         * Before we can encode the binding set, we need to update the schema
         * such that it captures any variables used in the binding set (plus
         * any variables which have been observed in previous binding sets).
         */
        updateSchema(bset);

        /*
         * Encode the binding set. Bindings will appear in the same order that
         * they were added to the schema. Unbound variables are represented by a
         * NullIV.
         */
        keyBuilder.reset();
        
        
        /**
         * https://jira.blazegraph.com/browse/BLZG-4476:
         * DISTINCT does not eliminate duplicate rows
         * 
         * Given that the schema is extended over time, we always encode
         * up to the position of the last variable in the schema. This is required
         * because we need *exactly the same syntactical representation* when
         * encoding the same mapping at a later point (which is required because we
         * need to do equality lookups on the values when storing encoded binding
         * sets in the HTree). As an example, assume we encounter (and encode) the
         * following sequence of mapping sets:
         * 
         * 1.) { ?a -> 1 }
         * 2.) { ?a -> 2, ?b -> 1 }
         * 3.) { ?b -> 1 }
         * 4.) { ?a -> 1 }
         * 5.) { ?c -> 1 }
         * 6.) { ?a-> 2, ?c -> 1 }
         * 
         * Here's how it works:
         * 
         * 1.) When processing the first binding set, we do not yet know the full extend
         * of the schema, all that is known at this point is that ?a belongs to the
         * schema. So our encoding will be enc(1) and the first schema variable is
         * recorded to be ?a:
         * => schema:=[?a] / encoded value:=enc(1)
         * 
         * 2.) We extend the schema to schema:=[?a,?b] and encode value:=enc(2).enc(1).
         * Note that both positions are present.
         * 
         * 3.) The schema remains schema:=[?a,?b]. In order to account for the fact that
         * we now encode ?b, we must pad an "empty" encoded binding, i.e. we encode
         * enc(null).enc(1).
         * 
         * 4.) The interesting part comes now (and this is what was failing in the bug),
         * when we see our initial mapping again. The schema remains unchanged, but we 
         * must not encode two values in that case, i.e. the result *must* be the same
         * as in step 1.). We can do that by encoding *up to the latest position for which
         * there is a variable in the binding set. By following this rule, we will always
         * obtain the same result that we computed at the time when we encoded the mapping
         * for the first time, just because that's what we did last time. In our case,
         * the only variable in the binding set is ?a, so we only iterate up to the first
         * position and get encoded value:=enc(1), just like in step 1.).
         * 
         * 5.) ?c is a new variable, so we set schema:=[?a,?b,?c] and encode the value
         * as enc(null).enc(null).enc(1).
         * 
         * 6.) Following our rules, we keep the schema and get enc(2).enc(null).enc(1).
         * 
         * The key observation here is that this strategy gives us a bidirectional and
         * unique mapping scheme.
         */
        int numBoundInBSet = bset.size();
        
        final Iterator<IVariable<?>> vitr = schema.iterator();
        while (vitr.hasNext() && numBoundInBSet>0) {
            final IVariable<?> v = vitr.next();
            @SuppressWarnings("unchecked")
            final IConstant<IV<?, ?>> c = bset.get(v);
            if (c == null) {
                IVUtility.encode(keyBuilder, TermId.NullIV);
            } else {
                
                final IV<?, ?> iv = c.get();
                
                if (iv.isNullIV()) {

                    /**
                     * BLZG-611 (https://jira.blazegraph.com/browse/BLZG-611):
                     * we need to properly encode (and later on, decode)
                     * mocked IVs, which have either been constructed at runtime or 
                     * represent values that are not present in the database. We do
                     * this by wrapping fully inlined IV types (for URIs, literals,
                     * or blank nodes) into MockedValueIV, which will be properly
                     * decoded as a mocked IV later on.
                     */
                    final Object val = iv.getValue();
                    
                    final IV<?,?> ivToEncode;
                    if (val instanceof BigdataURIImpl) {

                        // create fully inlined URI IV
                        ivToEncode = new FullyInlineURIIV<>((BigdataURIImpl)val);
                        
                    } else if (val instanceof BigdataLiteralImpl) {
                        
                        // create fully inlined literal IV
                        final BigdataLiteralImpl valAsLiteral = (BigdataLiteralImpl)val;
                        ivToEncode = 
                            new FullyInlineTypedLiteralIV<>(
                                valAsLiteral.getLabel(), 
                                ((BigdataLiteralImpl) val).getLanguage(),
                                ((BigdataLiteralImpl) val).getDatatype());
                        
                    } else if (val instanceof BigdataBNodeImpl) {

                        // create fully inlined blank node IV
                        final BigdataBNodeImpl valAsBNode = (BigdataBNodeImpl)val;
                        ivToEncode = new FullyInlineUnicodeBNodeIV<>(valAsBNode.getID());
                        
                    } else {
                        
                        // unreachable code, just in case...
                        throw new IllegalArgumentException("Uncovered iv.getValue() type in encode.");
                    }

                    IVUtility.encode(keyBuilder, new MockedValueIV(ivToEncode));
                    
                } else {
                    
                    IVUtility.encode(keyBuilder, iv);
                    
                    cacheSchemaAndValue(v, iv, updateCache); // caching hook
                    
                }

                // the following will become zero iff we processed the latest 
                // position in which a variable was bound
                numBoundInBSet--;

            }
        }
        
        return keyBuilder.getKey();
        
    }

    /**
     * Hook method to trigger caching of variable and the value. May be
     * re-implemented in subclasses to batch values, see {@link IVBindingSetEncoderWithIVCache}.
     * 
     * @param iv
     * @param v
     */
    void cacheSchemaAndValue(final IVariable<?> v, final IV<?,?> iv, final boolean updateCache) {
        // NOP
    }
    
    @Override
    public void flush() {
        // NOP
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public IBindingSet decodeSolution(final byte[] val, final int off,
            final int len, final boolean resolveCachedValues) {

        final IBindingSet bset = new ListBindingSet();

        final IV<?, ?>[] ivs = IVUtility.decodeAll(val, off, len);

        int i = 0;

        for (IVariable<?> v : schema) {

            if (i == ivs.length) {
                /*
                 * This solution does not include all variables which were
                 * eventually discovered to be part of the schema.
                 */
                break;
            }

            final IV<?, ?> iv = ivs[i++];
            
            if (iv == null) {
            
                // Not bound.
                continue;
                
            }
            
            /**
             * BLZG-611 (https://jira.blazegraph.com/browse/BLZG-611):
             * decoding of MockeedValueIV, see encodeSolution() for more information.
             */
            if (iv instanceof MockedValueIV) {

                final MockedValueIV mvIv = (MockedValueIV)iv;
                final IV<?,?> innerIv = mvIv.getIV();
                
                final BigdataValue value; // set inside subsequent if block
                final TermId mockIv; // populated subsequently
                if (innerIv instanceof FullyInlineURIIV) {
                    
                    final FullyInlineURIIV innerIvAsUri = (FullyInlineURIIV)innerIv;
                    
                    final URI inlineUri = innerIvAsUri.getInlineValue();
                    
                    value = vf.createURI(inlineUri.stringValue());
                    mockIv = TermId.mockIV(VTE.URI);
                    
                } else if (innerIv instanceof FullyInlineTypedLiteralIV) {

                    final FullyInlineTypedLiteralIV innerIvAsLiteral = 
                        (FullyInlineTypedLiteralIV)innerIv;

                    value = vf.createLiteral(
                            innerIvAsLiteral.getLabel(), 
                            innerIvAsLiteral.getDatatype(), 
                            innerIvAsLiteral.getLanguage());
                    mockIv = TermId.mockIV(VTE.LITERAL);

                } else if (innerIv instanceof FullyInlineUnicodeBNodeIV) {
                    
                    final FullyInlineUnicodeBNodeIV innerIvAsBNode = 
                            (FullyInlineUnicodeBNodeIV)innerIv;

                    value = vf.createBNode(innerIvAsBNode.getID());
                    mockIv = TermId.mockIV(VTE.BNODE);

                } else {
                    
                    // unreachable code, just in case...
                    throw new IllegalArgumentException("Uncovered inner IV type in decode");
                }

                // re-construct original mock IV
                mockIv.setValue(value);
                value.setIV(mockIv);

                bset.set(v, new Constant<IV<?, ?>>(mockIv));

            } else {
                
                bset.set(v, new Constant<IV<?, ?>>(iv));
                
            }
        }
        
        if(resolveCachedValues)
            resolveCachedValues(bset);

        return bset;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This implementation is a NOP as the {@link IVCache} association
     * is NOT maintained by this class.
     */
    @Override
    public void resolveCachedValues(final IBindingSet bset) {

        // NOP
        
    }
    
    @Override
    public void release() {
        
        schema.clear();

    }

}
