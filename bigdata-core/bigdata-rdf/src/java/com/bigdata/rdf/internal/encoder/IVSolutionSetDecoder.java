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
 * Created on Feb 16, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.htree.HTree;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.util.BytesUtil;

/**
 * Decoder for {@link IVSolutionSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class IVSolutionSetDecoder implements IBindingSetDecoder {

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
     * An extensible random access list. The order of the items in the list is
     * the order in which they are entered into the {@link #schema}. This is
     * used to interpret the bitmap for the variables which are bound in a
     * solution.
     */
    private final ArrayList<IVariable<?>> schemaIndex;

    /**
     * The observed {@link IVCache} associations.
     */
    private final Map<IV<?, ?>, BigdataValue> cache;

    /**
     * Used to de-serialize the {@link BigdataValue}s.
     */
    private final StringBuilder tmp;
    
    /**
     * The #of solutions decoded to date.
     */
    private int nsolutions = 0;

    /**
     * The version number. The versions are declared by
     * {@link IVSolutionSetEncoder}. They are read from the first solution in a
     * stream.
     */
    private int version = -1;

    /*
     * Discovered dynamically.
     */
    
    /**
     * The namespace of the lexicon relation. The namespace is discovered
     * dynamically when we read the first record with an {@link IVCache}
     * association. It will be <code>null</code> until then.
     */
    private String namespace;
    
    /**
     * Used to de-serialize the {@link BigdataValue}s for {@link IVCache}
     * associations. This is initialized if and when we discover the
     * {@link #namespace}.
     */
    private BigdataValueSerializer<BigdataValue> valueSer;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{namespace=" + namespace);
        sb.append(",schema=" + schema); // Not thread-safe.
        sb.append(",cacheSize=" + cache.size());// Not thread-safe
        sb.append(",nsolutions="+nsolutions);
        sb.append("}");
        return sb.toString();
    }
    
    public IVSolutionSetDecoder() {

        // The ordered set of variables for which bindings have been observed.
        this.schema = new LinkedHashSet<IVariable<?>>();

        // The ordered set of variables for which bindings have been observed.
        this.schemaIndex = new ArrayList<IVariable<?>>();

        // The IV -> BigdataValue cache
        this.cache = new HashMap<IV<?, ?>, BigdataValue>();

        this.tmp = new StringBuilder();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Solutions MUST be decoded in the encode order because the schema
     * (the set of variables for which bindings have been observed) is assembled
     * incrementally from the decoded solutions and the encoding is sensitive to
     * the order in which the variables are first observed. Also, the presence
     * of the version field in the first solution makes it impossible to
     * re-process a stream of solutions with the same decoder.
     */
    @Override
    public IBindingSet decodeSolution(final byte[] data, int off, int len,
            final boolean resolveCachedValues) {

        // Note: close() is NOT required for DataInputBuffer.
        final DataInputBuffer in = new DataInputBuffer(data, off, len);
        
        return decodeSolution(in, resolveCachedValues);
        
    }

    /**
     * Stream oriented decode.
     * 
     * @param in
     *            The source data.
     * @param resolveCachedValues
     *            <code>true</code> if {@link IVCache} associations should be
     *            resolved as the solutions are decoded.
     *            
     * @return The next decoded solution.
     */
    public IBindingSet decodeSolution(final DataInputBuffer in,
            final boolean resolveCachedValues) {

        if (version == -1) {

            try {

                version = in.unpackInt();

                switch (version) {
                case IVSolutionSetEncoder.VERSION0:
                    break;
                default:
                    throw new RuntimeException("Unknown version: " + version);
                }

//                final int versionLength = (int) in.position();
//
//                off += versionLength;
//                len -= versionLength;

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

        final IBindingSet bset = _decodeSolution(in, //data, off, len,
                resolveCachedValues);

        nsolutions++;

        return bset;

    }

    private IBindingSet _decodeSolution(final DataInputBuffer in,
//            final byte[] data, final int off, final int lenX,
            final boolean resolveCachedValuesIsIgnored) {

        final byte[] data = in.getBuffer();

        final int off = in.getOrigin();
        
        try {

            final IBindingSet bset = new ListBindingSet();

            // * nbound
            // * nvars
            // * ncached
            // * var[0]...var[nvars-1]
            // * bitmap-for-bound-variables
            // * bitmap-for-IV-with-cached-Values
            // * IV[0] ... IV[nbound-1]
            // * Value[0] ... Value[ncached-1]

//            final DataInputBuffer in = new DataInputBuffer(data, off, len);

            // #of bindings in this record.
            final int numBindings = in.unpackInt();

            if (numBindings == 0) {

                // Empty solution.
                return bset;
                
            }
            
            // #of variables declared for the first time by this record.
            final int newVars = in.unpackInt();

            // #of new IVCache associations encoded in this record.
            final int newCached = in.unpackInt();

            if (newCached > 0 && numBindings == 0) {
                /*
                 * Illegal combination. New IV => BigdataValue cache
                 * associations can only appear with new bindings.
                 */
                throw new RuntimeException();
            }

            if (newCached > 0 && namespace == null) {
                /*
                 * This is where we discover the namespace for the serialized
                 * BigdataValue objects.
                 */
                namespace = in.readUTF2();
                valueSer = BigdataValueFactoryImpl.getInstance(namespace)
                        .getValueSerializer();
            }
            
            // read newly declared variable names and add them to the schema.
            for (int i = 0; i < newVars; i++) {

                final IVariable<?> var = Var.var(in.readUTF2());

                if (schema.add(var)) {

                    schemaIndex.add(var);

                } else {

                    // The variable was already declared.
                    throw new IllegalStateException("Already declared: "
                            + var.getName());

                }

            }

            // #of variables declared so far across decoded solutions.
            final int schemaSize = schema.size();
            
            // The bit index into the byte[] of the variable bit map.
            final int offsetVarBits = (off + ((int) in.position())) << 3;

            // #of bytes required for the bit flags (one per declared var to
            // date, but only if there are some bindings in this solution)
            final int nbytesVarBits = numBindings == 0 ? 0 : BytesUtil
                    .bitFlagByteLength(schemaSize);

            // Skip over the variable bit map.
            in.skipBytes(nbytesVarBits);

            // The bit index into the byte[] of the cached Value bit map.
            final int offsetCacheValueBits = (off + ((int) in.position())) << 3;

            // #of bytes required for the bit flags (one per declared var to
            // date, but only if there are some IVCache values in this record).
            final int nbytesCacheValueBits = newCached == 0 ? 0 : BytesUtil
                    .bitFlagByteLength(numBindings);

            // Skip over the cache bit map.
            in.skipBytes(nbytesCacheValueBits);

            /*
             * Decode the IV[].
             * 
             * The IV[] is dense. There are [numBindings] values in the IV[].
             * The variable for each binding is obtained by finding the next
             * non-zero bit in the variable bit map (which must sum to
             * numBindings).
             * 
             * The [ivs] List is maintained iff we need to index into the IVs by
             * their decode order. That is only necessary when there are new
             * IVCache associations in this record (newCached>0).
             */
            final List<IV<?,?>> ivs;
            if (newCached > 0)
                ivs = new ArrayList<IV<?, ?>>(numBindings);
            else
                ivs = Collections.emptyList();
            if (numBindings > 0) {
                int chksum = 0;
                long bitIndex = offsetVarBits;
                final long maxBitIndex = offsetCacheValueBits;//bitIndex + (nbytesVarBits << 3);
                int i = 0;
                int ivoff = (int) (off + in.position());
                while (i < schemaSize) {
                    if (bitIndex >= maxBitIndex)
                        break;
                    final boolean isSet = BytesUtil.getBit(data, bitIndex++);
                    if (isSet) {
                        /*
                         * Decode the IV for this variable.
                         * 
                         * Note: A "mock" IV will be decoded into a non-null
                         * TermId.
                         */
                        chksum++;
                        final IVariable<?> var = schemaIndex.get(i);
                        final IV<?, ?> iv = IVUtility.decodeFromOffset(data,
                                ivoff, false/* nullIsNullRef */);
                        bset.set(var, new Constant<IV<?, ?>>(iv));
                        if (newCached > 0) {
                            ivs.add(iv);
                        }
                        final int byteLength = iv.byteLength();
                        ivoff += byteLength;
                        in.skipBytes(byteLength);
                    }
                    i++;
                }
                if (chksum != numBindings) {
                    throw new RuntimeException("Bad bit sum: chksum=" + chksum
                            + ", expected=" + numBindings);
                }
            }

            /*
             * Decode any cached BigdataValue objects and associate them with
             * the correct IVs in the IVCache mapping. The bit map has
             * [numBindings] bits. Each bit indicates whether or not there is a
             * cached Value inline for the corresponding binding (decoded
             * above).
             */
            if (newCached > 0) {
                int chksum = 0;
                long bitIndex = offsetCacheValueBits;
                final long maxBitIndex = bitIndex + (nbytesCacheValueBits << 3);
                int i = 0;
                while (i < numBindings) {
                    if (bitIndex >= maxBitIndex)
                        break;
                    final boolean isSet = BytesUtil.getBit(data, bitIndex++);
                    if (isSet) {
                        /*
                         * Decode the cached Value for the IV.
                         */
                        chksum++;
                        final IV<?, ?> iv = ivs.get(i);
                        final BigdataValue value = valueSer
                                .deserialize(in, tmp);
                        if (!iv.isNullIV()) {
                            cache.put(iv, value);
                        } else {
                            /*
                             * We must attach the IVCache association for a
                             * mockIV since it is NOT possible to recover that
                             * association from the cache.
                             * 
                             * Note: This is because TermId.equals() considers
                             * TermIds to be equals() if they have termId=0L and
                             * ignores the cached Value unless BOTH TermIds have
                             * the value set. When we decode a mock IV the Value
                             * is not set so we can not resolve it against a
                             * hash map. Hence we MUST set it on the IV
                             * immediately while we can stil maintain the
                             * correlation between the IV and the cached Value.
                             */
                           ((IV) iv).setValue(value);
                        }
                    }
                    i++;
                }
                if (chksum != newCached) {
                    throw new RuntimeException("Bad bit sum: chksum=" + chksum
                            + ", expected=" + newCached);
                }
            }
            
            if (numBindings > 0)
                resolveCachedValues(bset);

            return bset;

        } catch (IOException ex) {

            throw new RuntimeException(ex);
            
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void resolveCachedValues(final IBindingSet bset) {

        final Iterator<Map.Entry<IVariable, IConstant>> itr = bset.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            final IConstant c = e.getValue();

            final IV iv = (IV) c.get();

            final BigdataValue val = cache.get(iv);

            if (val != null) {

                iv.setValue(val);

            }
            
        }

    }

    @Override
    public void release() {

        cache.clear();

        schema.clear();

        schemaIndex.clear();

        tmp.setLength(0);
        
        version = -1;
        
        nsolutions = 0;

        namespace = null;

        valueSer = null;

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
