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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.htree.HTree;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.util.BytesUtil;

/**
 * This class provides fast, efficient serialization for solution sets. Each
 * solution must be an {@link IBindingSet}s whose bound values are {@link IV}s
 * and their cached {@link BigdataValue}s. The {@link IV}s and the cached
 * {@link BigdataValue}s are efficiently and compactly represented in format
 * suitable for chunked messages or streaming. Decode is a fast online process.
 * Both encode and decode require the maintenance of a map from the {@link IV}
 * having cached {@link BigdataValue}s to those cached values.
 * 
 * <h2>Record Format</h2>
 * 
 * The format is as follows:
 * 
 * <pre>
 * nbound
 * nvars
 * ncached 
 * (namespace)
 * var[0]...var[nvars-1]
 * bitmap-for-bound-variables
 * bitmap-for-IV-with-cached-Values
 * IV[0] ... IV[nbound-1]
 * Value[0] ... Value[ncached-1]
 * </pre>
 * 
 * where <code>nbound</code> is the #of bindings in the binding set. When zero,
 * the rest of the record is omitted.
 * <p>
 * where <code>nvars</code> is the #of new variables in this binding set. The
 * "schema" used to encode the bindings is based on the ordered set of variables
 * for which bindings are observed. The encoder writes this information out
 * incrementally. The decoder builds up this information as it decodes
 * solutions.
 * <p>
 * where <code>ncached</code> is the #of bindings in the binding set for which
 * there is a cached {@link BigdataValue} which has not already been written
 * into a previous record. Even if the {@link IV} has a cached
 * {@link BigdataValue}, if the {@link IV} has been previously written into a
 * record then the {@link IV} is NOT record in this record with a cached Value.
 * Further, if the {@link IV} appears more than once in a given record, the
 * cached value is only marked in the bitmap for the first such occurrence and
 * the cached value is only written into the record once.
 * <p>
 * where <code>namespace</code> is the namespace of the lexicon relation. This
 * is written out for the first solution having an {@link IVCache} association.
 * It is assumed that all {@link Value}s are {@link BigdataValue} for the same
 * lexicon relation. If no solutions have an {@link IVCache} association, then
 * the namespace will never be written into the encoded output.
 * <p>
 * where <code>var</code> is the name of a variable for which a binding was
 * first observed for the current solution. The names of the variables are
 * written in the order in which they are first observed. This forms the
 * implicit "schema" required to decode the {@link IV}[].
 * <p>
 * where <code>bitmap-for-bound-variables</code> is zero or more bytes providing
 * a bit map indicating those variables which are bound in this solution out of
 * the total set of variables which have been observed in the solutions
 * presented to this encode.
 * <p>
 * where <code>bitmap-for-IVs-with-cached-Values</code> is zero or more bytes
 * providing a bit map indicating which IVs are associated with cached values
 * written into the record. Whether or not an IV has a cached value must be
 * decided by the caller after processing the record and consulting an
 * (IV,Value) cache which they maintain over the set of records processed to
 * date. Cached values are written out (and the bit set) only the first time a
 * given IV with a cached Value is observed.
 * <p>
 * where <code>IV[n]</code> is an {@link IV} as encoded by {@link IVUtility}.
 * <p>
 * where {@link BigdataValue} is an RDF Value serialized using the
 * {@link BigdataValueSerializer} for the namespace of the lexicon.
 * 
 * <h2>Decode</h2>
 * 
 * The namespace of the lexicon is required in to obtain the
 * {@link BigdataValueFactory} and {@link BigdataValueSerializer} used to decode
 * and materialize the cached {@link BigdataValue}s. This information can be
 * sent before the records if it is not known to the caller.
 * <p>
 * The decoder materializes the cached values into a map (either a HashMap or
 * HTree, as appropriate for the data scale) as the records are processed. Only
 * one solution needs to be decoded at a time, but the decoder must maintain the
 * (IV,Value) cache across all decoded records. There is no need to indicate the
 * #of records, but IChunkMessage#getSolutionCount() in fact reports exactly
 * that information.
 * <p>
 * Each solution can be turned into an {@link IBindingSet} at the time that it
 * is decoded. If we use a standard {@link ListBindingSet}, then we need to
 * resolve each {@link IV} against the {@link IV} cache, setting its RDF Value
 * as a side effect before returning the IBindingSet to the caller. If we do a
 * custom {@link IBindingSet} implementation, then the cached
 * {@link BigdataValue} could be lazily materialized by hooking
 * {@link IVCache#getValue()}. Either way, the life cycle of the materialized
 * objects will be very short unless they are propagated into new solutions.
 * Short life cycle objects entail very little heap burden.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IVSolutionSetEncoder.java 6032 2012-02-16 12:48:04Z thompsonbry
 *          $
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/475">Optimize
 *      serialization for query messages on cluster </a>
 * 
 *      TODO There chould be a completely different encoding when only a single
 *      variable is bound (column projection) especially if there are likely to
 *      be duplicate IVs. However, we still have to pass through the cached
 *      Value associations, which this does pretty efficiently.
 */
public class IVSolutionSetEncoder implements IBindingSetEncoder {

    private static final Logger log = Logger
            .getLogger(IVSolutionSetEncoder.class);
    
    /**
     * The schema provides the order in which the {@link IV}[] for solutions
     * stored in the hash index are encoded in the {@link HTree}. {@link IV} s
     * which are not bound are modeled by a {@link TermId#NullIV}.
     * <p>
     * Note: In order to be able to encode/decode the schema based on the lazy
     * identification of the variables which appear in solutions the
     * {@link HTree} must store variable length {@link IV}[]s since new
     * variables may be discovered at any point.
     */
    private final LinkedHashSet<IVariable<?>> schema;

    /**
     * Used to store the {@link IVCache} associations. This allows us to elide
     * {@link BigdataValue}s which have already been written by this encoder
     * instance.
     */
    private final Map<IV<?, ?>, BigdataValue> cache;

    /**
     * Used to encode the {@link IV}s.
     */
    private final IKeyBuilder keyBuilder;
    
    /**
     * Used to format the encoded records (reset each time).
     */
    private final DataOutputBuffer out;

    /**
     * Temporary buffer used by {@link BigdataValueSerializer}.
     */
    private final ByteArrayBuffer tmp;
    
    /**
     * The initial version.
     */
    static final int VERSION0 = 0x0;
    
    /**
     * The version used by this encoder.
     */
    private final int version = VERSION0;

    /**
     * The #of solutions encoded to date.
     */
    private int nsolutions = 0;

    /*
     * Set when the first solution having a bound value is processed.
     */
    
    /**
     * The namespace of the lexicon relation. This is discovered from the first
     * {@link IVCache} association and written out into the stream at that
     * point. If there are no {@link IVCache} associations then it is never set.
     */
    private String namespace;

    /**
     * Used to de-serialize the {@link BigdataValue}s for {@link IVCache}
     * associations.
     */
    private BigdataValueSerializer<BigdataValue> valueSer;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{namespace=" + namespace);
        sb.append(",schema=" + schema); // Not thread-safe.
        sb.append(",cacheSize=" + cache.size());// Not thread-safe
        sb.append(",nsolutions=" + nsolutions);
        sb.append("}");
        return sb.toString();
    }
    
    public IVSolutionSetEncoder() {

        // The ordered set of variables for which bindings have been observed.
        this.schema = new LinkedHashSet<IVariable<?>>();

        // The IV -> BigdataValue cache
        this.cache = new HashMap<IV<?, ?>, BigdataValue>();

        // Used to encode the IVs.
        this.keyBuilder = new ASCIIKeyBuilderFactory(128).getKeyBuilder();
        
        // Used to format the encoded records (reset each time).
        this.out = new DataOutputBuffer();
        
        // Temp buffer used by the BigdataValueSerializer
        this.tmp = new ByteArrayBuffer();
        
    }

    /**
     * Encode the solution on the stream.
     * 
     * @param out
     *            The stream.
     * @param bset
     *            The solution.
     */
    public void encodeSolution(final DataOutputBuffer out,
            final IBindingSet bset) {

        out.append(encodeSolution(bset));

    }
    
    @Override
    public byte[] encodeSolution(final IBindingSet bset) {

        return encodeSolution(bset, true/* updateCache */);

    }

    /**
     * {@inheritDoc}
     * 
     * TODO We typically use a {@link ListBindingSet}. If the
     * {@link IBindingSet} is large enough, then it would be more efficient to
     * create an {@link IVariable} to {@link IV} map within this method since we
     * have to lookup bindings by variables more than once.
     */
    @Override
    public byte[] encodeSolution(final IBindingSet bset,
            final boolean updateCacheIsIgnored) {

        if (bset == null)
            throw new IllegalArgumentException();

        final boolean trace = log.isTraceEnabled();
        
        // Reset internal buffers.
        keyBuilder.reset();
        out.reset();

        if (0 == nsolutions++) {

            // Write out the version number before the first solution.
            out.packLong(version);

        }
        
        /*
         * Before we can encode the binding set, we need to update the schema
         * such that it captures any variables used in the binding set (plus any
         * variables which have been observed in previous binding sets). We also
         * note which variables are present for the first time in this solution.
         * We will need to write those onto the wire.
         */
        final List<IVariable<?>> newVars = new LinkedList<IVariable<?>>();
        {
            /*
             * Note: Changed to use bset.iterator() based on a hot spot in the
             * profiler.
             */
            @SuppressWarnings("rawtypes")
//            final Iterator<IVariable> vitr = bset.vars();
//            while (vitr.hasNext()) {
//                final IVariable<?> v = vitr.next();
//                if (schema.add(v)) {
//                    newVars.add(v);
//                }
//            }
            final Iterator<Map.Entry<IVariable, IConstant>> itr = bset
                    .iterator();
            while (itr.hasNext()) {
                final IVariable<?> v = itr.next().getKey();
                if (schema.add(v)) {
                    newVars.add(v);
                }
            }
        }

        /*
         * Encode the binding set. Bindings will appear in the same order that
         * they were added to the schema. 
         */
        // Ordered set of new IV -> Value associations. List entry is [null] if
        // no association for IV at that ordinal position in the solution.
        final List<BigdataValue> values = new LinkedList<BigdataValue>();
        // #of bindings with non-null IVs.
        int numBindings = 0;
        // #of new IVCache associations.
        int newCached = 0;
        boolean discoveredNamespace = false;
        {
            // For each variable in the [schema] order.
            final Iterator<IVariable<?>> vitr = schema.iterator();
            while (vitr.hasNext()) {
                final IVariable<?> v = vitr.next();
                // Lookup binding for that variable in the solution.
                @SuppressWarnings("unchecked")
                final IConstant<IV<?, ?>> c = bset.get(v);
                if (c == null)
                    continue;
                // Variable is bound in this solution.
                final IV<?, ?> iv = c.get();
                // Encode binding into buffer.
                IVUtility.encode(keyBuilder, iv);
                if (iv.hasValue() && (iv.isNullIV() || !cache.containsKey(iv))) {
                    // New IV => Value association (all NullIVs are "new").
                    final BigdataValue value = iv.getValue();
                    if (namespace == null) {
                        // Note: The namespace is discovered here!!!
                        namespace = value.getValueFactory().getNamespace();
                        valueSer = BigdataValueFactoryImpl.getInstance(
                                namespace).getValueSerializer();
                        discoveredNamespace = true;
                    }
                    if (!iv.isNullIV()) {
                        /*
                         * We can not lookup Null IVs in the cache on the
                         * decoder side since ties are broken by comparing the
                         * IVCache association, which is what we are trying to
                         * resolve. Therefore we always inline the IVCache
                         * assocation if TermId.isNull() is true.
                         */
                        cache.put(iv, value);
                    }
                    values.add(value);
                    newCached++;
                } else {
                    values.add(null/* no-value */);
                }
                numBindings++;
            }
        }

//        * nbound
//        * nvars
//        * ncached 
//        * var[0]...var[nvars-1]
//        * bitmap-for-bound-variables
//        * bitmap-for-IV-with-cached-Values
//        * IV[0] ... IV[nbound-1]
//        * Value[0] ... Value[ncached-1]
        
        out.packLong(numBindings);
        if (numBindings == 0) {
            // Return formatted record.
            return out.toByteArray();
        }
        out.packLong(newVars.size());
        out.packLong(newCached);
        if (discoveredNamespace) {
            out.writeUTF2(namespace);
        }
        if (trace) {
            log.trace("schemaSize=" + schema.size() + ", cacheSize="
                    + cache.size() + ", namespace=" + namespace);
            log.trace("newVars=" + newVars.size() + ", numBindings="
                    + numBindings + ", newCached=" + newCached);
        }
        
        // write newly declared variable names.
        for (IVariable<?> var : newVars) {
            out.writeUTF2(var.getName());
        }
        
        /*
         * Write out a bit map for the variables which are bound in this
         * solution.
         * 
         * Note: This is more compact than using TermId.NULL to indicate an
         * unbound variable. TermId.NULL is 9 bytes. 9 bytes is enough for a bit
         * map for 9*8=72 variables, so this is a win if there is even one
         * unbound variable.
         */
        if (numBindings > 0) {
            // #of bytes required for the bit flags (one per declared var to date)
            final int nbytes = BytesUtil.bitFlagByteLength(schema.size());
            // current buffer position as bit index.
            int bitIndex = out.pos() << 3;
            if (trace)
                log.trace("varbitmap: beginBitOffset=" + bitIndex + ", nbytes="
                        + nbytes);
            // pre-extend the buffer, zeroing the bitmap.
            out.ensureFree(nbytes);
            for (int i = 0; i < nbytes; i++) {
                out.append((byte) 0);
            }
            for (IVariable<?> var : schema) {
                if (bset.isBound(var)) {
                    BytesUtil.setBit(out.array(), bitIndex, true);
                }
                bitIndex++;
            }
        }
        /*
         * Write out bit map for IVs for which we have newly observed a cached
         * Value.
         */
        if (newCached > 0) {
            // #of bytes required for the bit flags.
            final int nbytes = BytesUtil.bitFlagByteLength(numBindings);
            // current buffer position as bit index.
            int bitIndex = out.pos() << 3;
            // pre-extend the buffer, zeroing the bitmap.
            out.ensureFree(nbytes);
            for (int i = 0; i < nbytes; i++) {
                out.append((byte) 0);
            }
            if (trace)
                log.trace("cachebitmap: beginBitOffset=" + bitIndex
                        + ", nbytes=" + nbytes);
            for (BigdataValue value : values) {
                if (value != null) {
                    BytesUtil.setBit(out.array(), bitIndex, true);
                }
                bitIndex++;
            }
        }

        // write IV[].
        if (trace)
            log.trace("IV[]: off=" + out.pos() + ", numBindings=" + numBindings
                    + ", byteLength=" + keyBuilder.len());
        out.append(keyBuilder.array(), 0/* off */, keyBuilder.len());

        /*
         * Write Value[]. Each Value is written directly into [out].
         */
        if (newCached > 0) {
            if(trace)
                log.trace("cache[]: off=" + out.pos() + ", newCached="
                        + newCached);
            for (BigdataValue value : values) {

                if (value != null) {

                    valueSer.serialize2(value, out, tmp);

                }

            }
        }
        if(trace)
            log.trace("done: off=" + out.pos());

        // Return formatted record.
        return out.toByteArray();

    }

    @Override
    public void release() {

        cache.clear();

        schema.clear();

        out.clear();

        tmp.clear();
        
        nsolutions = 0;
        
    }

    @Override
    public void flush() {

        // NOP

    }

    /**
     * {@inheritDoc}
     * <p>
     * Always returns <code>true</code>.
     */
    @Override
    public boolean isValueCache() {

        return true;

    }

}
