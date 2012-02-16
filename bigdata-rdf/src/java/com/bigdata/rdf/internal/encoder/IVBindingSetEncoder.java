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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

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
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;

/**
 * A utility class for generating and processing compact representations of
 * {@link IBindingSet}s whose {@link IConstant}s are bound to {@link IV}s.
 * Individual {@link IV}s may be associated with a cached RDF {@link Value}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IVBindingSetEncoder {

    /**
     * <code>true</code> iff this is in support of a DISTINCT filter.
     * <p>
     * Note: we do not maintain the {@link #ivCacheSchema} for a DISTINCT filter
     * since the original solutions flow through the filter.
     */
    private final boolean filter;
    
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
     * The set of variables for which materialized {@link IV}s have been
     * observed.
     */
    final protected LinkedHashSet<IVariable<?>> ivCacheSchema;
    
    /**
     * Used to encode the {@link IV}s.
     */
    private final IKeyBuilder keyBuilder;
    
    /**
     * 
     * @param filter
     *            <code>true</code> iff this is in support of a DISTINCT filter.
     *            <p>
     *            Note: we do not maintain the {@link #ivCacheSchema} for a
     *            DISTINCT filter since the original solutions flow through the
     *            filter.
     */
    public IVBindingSetEncoder(final boolean filter) {

        this.filter = filter;
        
        this.schema = new LinkedHashSet<IVariable<?>>();

        // The set of variables for which materialized values are observed.
        this.ivCacheSchema = filter ? null : new LinkedHashSet<IVariable<?>>();

        this.keyBuilder = new ASCIIKeyBuilderFactory(128).getKeyBuilder();

    }

//    /**
//     * Declare any variables which are known to be used, such as join variables.
//     * 
//     * @param vars
//     *            The variables.
//     */
//    public void updateSchema(final IVariable<?>[] vars) {
//
//        for (IVariable<?> v : vars) {
//
//            schema.add(v);
//
//        }
//
//    }

    /**
     * Build up the schema. This includes all observed variables, not just those
     * declared in {@link #joinVars}
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

    /**
     * Encode the solution as an {@link IV}[].
     * 
     * @param bset
     *            The solution to be encoded.
     * 
     * @return The encoded solution.
     */
    public byte[] encodeSolution(final IBindingSet bset) {

        return encodeSolution(null/* cache */, bset);
        
    }
    
    /**
     * Encode the solution as an {@link IV}[].
     * 
     * @param cache
     *            Any cached {@link BigdataValue}s on the {@link IV}s are
     *            inserted into this map (optional since the cache is not used
     *            when we are filtering DISTINCT solutions).
     * @param bset
     *            The solution to be encoded.
     * 
     * @return The encoded solution.
     */
    public byte[] encodeSolution(final Map<IV<?, ?>, BigdataValue> cache,
            final IBindingSet bset) {

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
        final Iterator<IVariable<?>> vitr = schema.iterator();
        while (vitr.hasNext()) {
            final IVariable<?> v = vitr.next();
            @SuppressWarnings("unchecked")
            final IConstant<IV<?, ?>> c = bset.get(v);
            if (c == null) {
                IVUtility.encode(keyBuilder, TermId.NullIV);
            } else {
                final IV<?, ?> iv = c.get();
                IVUtility.encode(keyBuilder, iv);
                if (iv.hasValue() && !filter) {
                    ivCacheSchema.add(v);
                    cache.put(iv, iv.getValue());
                }
            }
        }
        
        return keyBuilder.getKey();
        
    }

    /**
     * Decode a solution from an encoded {@link IV}[].
     * <p>
     * Note: The {@link IV#getValue() cached value} is NOT part of the encoded
     * data and will NOT be present in the returned {@link IBindingSet}. The
     * cached {@link BigdataValue} (if any) must be unified by consulting the
     * {@link #ivCache}.
     * 
     * @param val
     *            The encoded IV[].
     * @param off
     *            The starting offset.
     * @param len
     *            The #of bytes of data to be decoded.
     * 
     * @return The decoded {@link IBindingSet}.
     */
    public IBindingSet decodeSolution(final byte[] val, final int off,
            final int len) {

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

            bset.set(v, new Constant<IV<?, ?>>(iv));

        }

        return bset;

    }

    /**
     * Release the state associated with the {@link IVBindingSetEncoder}.
     */
    public void release() {
        
        schema.clear();

        if (ivCacheSchema != null) {

            ivCacheSchema.clear();
            
        }

    }
    
}
