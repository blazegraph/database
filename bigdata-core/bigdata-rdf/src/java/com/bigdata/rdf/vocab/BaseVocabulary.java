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
/*
 * Created on Aug 26, 2008
 */

package com.bigdata.rdf.vocab;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Base class for {@link Vocabulary} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BaseVocabulary implements Vocabulary, Externalizable {

    final static private transient Logger log = Logger
            .getLogger(BaseVocabulary.class);

    /**
     * The serialVersionUID as reported by the trunk on Oct 6, 2010.
     */
    private static final long serialVersionUID = 1560142397515291331L;

    /**
     * The {@link BigdataValueFactory} for the namespace associated with the KB
     * instance.
     */
    private transient BigdataValueFactory valueFactory;

    /**
     * An ordered set of the declared vocabulary classes in the order in which
     * they were declared.
     */
    private transient LinkedHashSet<VocabularyDecl> decls;

    /**
     * The {@link Value}s together with their assigned {@link IV}s.
     * <p>
     * Note: The {@link IV} is permanently attached to each {@link BigdataValue}.
     * <p>
     * Note: A {@link Map} is used for O(1) lookup of a {@link BigdataValue}
     * from a {@link Value}, but the keys and values for a given entry are
     * always the same reference.
     */
    private transient LinkedHashMap<Value, BigdataValue> val2iv;

    /**
     * Reverse lookup from {@link IV} to {@link Value}.
     */
    @SuppressWarnings("rawtypes")
    private transient Map<IV, BigdataValue> iv2val;
    
    /**
     * De-serialization ctor. 
     */
    protected BaseVocabulary() {
    
        /*
         * Note: [namespace] is set by readExternal().
         */
        
    }
    
    /**
     * Ctor used by {@link AbstractTripleStore#create()}.
     * 
     * @param database
     *            The database.
     */
    protected BaseVocabulary(final String namespace) {

        if (namespace == null)
            throw new IllegalArgumentException();

        this.valueFactory = BigdataValueFactoryImpl
                .getInstance(namespace);
        
    }

    /**
     * Invoked by {@link AbstractTripleStore#create()} to initialize the
     * {@link Vocabulary}.
     * 
     * @throws IllegalStateException
     *             if {@link #init()} has already been invoked.
     */
    synchronized final public void init() {

        /*
         * Note: This just passes in the default initial capacity for a hash map
         * since we do not have better information when invoked in this manner.
         */
        
        init(16/* ndecls */, 16/* nvalues */);
        
    }
    
    /**
     * Invoked by {@link AbstractTripleStore#create()} to initialize the
     * {@link Vocabulary}.
     * 
     * @throws IllegalStateException
     *             if {@link #init()} has already been invoked.
     */
    synchronized final private void init(final int declsInitialCapacity,
            final int valuesInitialCapacity) {

        if (valueFactory == null)
            throw new IllegalStateException();

        if (val2iv != null)
            throw new IllegalStateException();
        
        if (iv2val != null)
            throw new IllegalStateException();
        
        // Setup declarations set.
        this.decls = new LinkedHashSet<VocabularyDecl>(declsInitialCapacity);

        // Setup forward map.
        val2iv = new LinkedHashMap<Value, BigdataValue>(valuesInitialCapacity);

        // Hook for subclass to provide its vocabulary decls.
        addValues();

        // Setup reverse map now that we know the exact size.
        iv2val = new LinkedHashMap<IV, BigdataValue>(val2iv.size());

        addAllDecls();
        
        // Make stable assignment of IVs to each Value, populating maps.
        generateIVs();
        
    }
    
    /**
     * Hook for subclasses to provide their {@link VocabularyDecl}s using
     * {@link #addDecl(VocabularyDecl)}.
     */
    abstract protected void addValues();
    
    /**
     * Add a declared vocabulary.
     * 
     * @param decl
     *            The vocabulary declaration.
     */
    final protected void addDecl(final VocabularyDecl decl) {

        if (decl == null)
            throw new IllegalArgumentException();
        
        if(log.isInfoEnabled())
            log.info(decl.getClass().getName());
        
        decls.add(decl);
        
    }
    
//    /**
//     * Adds a {@link Value} into the internal collection.
//     * 
//     * @param value
//     *            The value.
//     * 
//     * @throws IllegalArgumentException
//     *             if the value is <code>null</code>.
//     */
//    final protected void add(final URI value) {
//
//        if (value == null)
//            throw new IllegalArgumentException();
//        
//        // convert to BigdataValues when adding to the collection.
//        val2iv.add(valueFactory.asValue(value));
//
//    }

    /**
     * Add all vocabulary items from all declaring classes.
     */
    private void addAllDecls() {

        for (VocabularyDecl decl : decls) {
        
            final Iterator<URI> itr = decl.values();

            while (itr.hasNext()) {

                // Convert to BigdataValues when adding to the collection.
                final BigdataValue value = valueFactory.asValue(itr.next());

                // Add to the collection.
                if (val2iv.put(value, value) != null) {

                    /*
                     * This has already been declared by some vocabulary. There
                     * is no harm in this, but the vocabularies should be
                     * distinct.
                     */
         
                    log.warn("Duplicate declaration: " + value);
                    
                } else {
                    
                    if (log.isDebugEnabled())
                        log.debug(decl.getClass().getName() + ":" + value);
                    
                }

            }
            
        }

    }
    
    /**
     * Make a stable assignment of {@link IV}s to declared {@link Value}s.
     * <p>
     * Note: The {@link Value}s are converted to {@link BigdataValue}s by
     * {@link #add(Value)} so that we can invoke
     * {@link AbstractTripleStore#addTerms(BigdataValue[])} directly and get
     * back the assigned {@link IV}s. We rely on the <code>namespace</code> of
     * the {@link AbstractTripleStore} to deserialize {@link BigdataValue}s
     * using the appropriate {@link BigdataValueFactory}.
     */
    private void generateIVs() {
        
        /*
         * Assign IVs to each vocabulary item.
         */
        final int n = size();
        
        if (n > MAX_ITEMS)
            throw new UnsupportedOperationException(
                    "Too many vocabulary items: n=" + n + ", but maximum is "
                            + MAX_ITEMS);

        // The #of generated IVs.
        int i = 0;

        // The Values in the order in which they were declared.
        for (Map.Entry<Value, BigdataValue> e : val2iv.entrySet()) {

            final BigdataValue value = e.getValue();

            @SuppressWarnings("rawtypes")
            final IV iv;

            if (i <= 255) {
            
                // Use a byte for the 1st 256 declared vocabulary items.
                iv = new VocabURIByteIV<BigdataURI>((byte) i);
                
            } else {
                
                // Use a short for the next 64k declared vocabulary items.
                iv = new VocabURIShortIV<BigdataURI>((short) i);
                
            }
            
            // Cache the IV on the Value.
            value.setIV(iv);

            // Note: Do not cache the Value on the IV.
             iv.setValue(value);

            iv2val.put(iv, value);

            i++;

        }

        assert iv2val.size() == val2iv.size();

    }

    /**
     * The maximum #of items is 256 {@link VocabURIByteIV}s plus 64k
     * {@link VocabURIShortIV}s.
     */
    static private final int MAX_ITEMS = Short.MAX_VALUE + 256;
    
    final public String getNamespace() {
        
        return valueFactory.getNamespace();
        
    }
    
    final public int size() {
        
        if (val2iv == null)
            throw new IllegalStateException();
        
        return val2iv.size();
        
    }

    final public Iterator<BigdataValue> values() {
        
        return Collections.unmodifiableMap(val2iv).values().iterator();
        
    }

    @SuppressWarnings("rawtypes")
    final public BigdataValue asValue(final IV iv) {
        
        if (val2iv == null)
            throw new IllegalStateException();
        
        if (iv == null)
            throw new IllegalArgumentException();

        return iv2val.get(iv);
        
    }

    @SuppressWarnings("rawtypes")
    final public IV get(final Value value) {

        if (val2iv == null)
            throw new IllegalStateException();

        if (value == null)
            throw new IllegalArgumentException();

        final BigdataValue tmp = val2iv.get(value);

        if (tmp == null)
            return null;

        return tmp.getIV();

    }

    @SuppressWarnings("rawtypes")
    final public IConstant<IV> getConstant(final Value value) {

        final IV iv = get(value);
        
        if (iv == null)
            throw new IllegalArgumentException("Not defined: " + value);

        return new Constant<IV>(iv);

    }

//    /**
//     * The initial version. This version is no longer supported. The manner in
//     * which the lexicon is encoded has fundamentally changed with the
//     * replacement of the TERM2ID and ID2TERM indices with a single TERMS index
//     * and additional inlining of values into the statement indices.
//     */
//    private static final transient short VERSION0 = 0;
//
//    /**
//     * This version modified the serialization to include the namespace of the
//     * KB instance and to pack the byte length values (this version was never
//     * deployed).
//     */
//    private static final transient short VERSION1 = 1;

    /**
     * This version modified the serialization to include the namespace of the
     * KB instance and a list of the {@link VocabularyDecl} classes to be
     * instantiated. The names of those classes are given in the order in which
     * they were declared. When the vocabulary is deserialized, the
     * {@link BigdataValue}s and {@link IV}s are simply reconstructed from those
     * classes.
     * <p>
     * Note: VERSION ZERO (0) was the initial version. That version is no longer
     * supported. The manner in which the lexicon is encoded has fundamentally
     * changed with the replacement of the TERM2ID and ID2TERM indices with a
     * single TERMS index and additional inlining of values into the statement
     * indices.
     */
    private static final transient short VERSION2 = 2;

    /**
     * The current version.
     */
    private static final transient short currentVersion = VERSION2;

    /**
     * Note: The de-serialized state contains {@link Value}s but not
     * {@link BigdataValue}s since the {@link AbstractTripleStore} reference is
     * not available and we can not obtain the appropriate
     * {@link BigdataValueFactory} instance without it. This should not matter
     * since the only access to the {@link Value}s is via {@link #get(Value)}
     * and {@link #getConstant(Value)}.
     */
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        if (val2iv != null)
            throw new IllegalStateException();
        if (iv2val != null)
            throw new IllegalStateException();
        
        final short version = in.readShort();

        switch (version) {
//        case VERSION0:
//            readVersion0(in);
//            break;
//        case VERSION1:
//            readVersion1(in);
//            break;
        case VERSION2:
            readVersion2(in);
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

    }

//    /**
//     * The old code for {@link #VERSION0}. This is here for historical purposes
//     * only.
//     *
//     * @param in
//     * @throws IOException
//     */
//    private void readVersion0(final ObjectInput in) throws IOException {
//
//        /*
//         * Note: VERSION0 was not able to provide the correct
//         * BigdataValueFactory since it did not have access to the KB namespace.
//         */
//        final ValueFactory valueFactory = new ValueFactoryImpl();
//        
//        final BigdataValueSerializer<Value> valueSer = new BigdataValueSerializer<Value>(
//                valueFactory);
//
//        // read in the #of values.
//        final int nvalues = in.readInt();
//        
//        if (nvalues < 0)
//            throw new IOException();
//        
//        // allocate the map with sufficient capacity.
//        val2iv = new LinkedHashMap<BigdataValue, IV>(nvalues);
//        iv2val = new LinkedHashMap<IV, BigdataValue>(nvalues);
//
//        for (int i = 0; i < nvalues; i++) {
//            
//            // #of bytes in the serialized value.
//            int nbytes = in.readInt();
//
//            // allocate array of that many bytes.
//            byte[] b = new byte[nbytes];
//            
//            // read the data for the serialized value.
//            in.readFully(b);
//
//            // de-serialize the value.
//            final Value value = valueSer.deserialize(b);
//
//            // #of bytes in the serialized IV.
//            nbytes = in.readInt(); 
//            
//            // allocate array for that many bytes.
//            b = new byte[nbytes];
//            
//            // read the data for the serialized IV.
//            in.readFully(b);
//            
//            // decode the IV.
//            final IV iv = IVUtility.decode(b);
//
//            // stuff in the map.
//            val2iv.put(value, iv);
//            iv2val.put(iv, value);
//            
//        }
//
//    }

//    private void readVersion1(final ObjectInput in) throws IOException {
//        
//        // read in the #of values.
//        final int nvalues = LongPacker.unpackInt(in);
//        
//        // The namespace of the KB instance.
//        final String namespace = in.readUTF();
//        
//        // Note: The value factory uses the namespace of the KB instance! 
//        valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
//
//        // ValueSerializer using the namespace of the KB instance!
//        final BigdataValueSerializer<BigdataValue> valueSer = new BigdataValueSerializer<BigdataValue>(
//                valueFactory);
//
//        // allocate the map with sufficient capacity.
//        val2iv = new LinkedHashMap<Value, BigdataValue>(nvalues);
//        iv2val = new LinkedHashMap<IV, BigdataValue>(nvalues);
//
//        // buffer reused for each Value/IV.
//        final ByteArrayBuffer buf = new ByteArrayBuffer();
//
//        // buffer reused for each Value.
//        final StringBuilder tmp = new StringBuilder();
//
//        for (int i = 0; i < nvalues; i++) {
//            
//            // #of bytes in the serialized value.
//            int nbytes = LongPacker.unpackInt(in);
//
//            buf.reset();
//            buf.ensureCapacity(nbytes);
//            
//            // read the data for the serialized value.
//            in.readFully(buf.array(), 0/* off */, nbytes/* len */);
//
//            // de-serialize the value.
//            final BigdataValue value = valueSer
//                    .deserialize(//
//                            new DataInputBuffer(buf.array(), 0/* off */, nbytes/* len */), //
//                            tmp//
//                    );
//
//            // #of bytes in the serialized IV.
//            nbytes = LongPacker.unpackInt(in);
//            
//            buf.reset();
//            buf.ensureCapacity(nbytes);
//
//            // read the data for the serialized IV.
//            in.readFully(buf.array(), 0/* off */, nbytes/* len */);
//
//            // decode the IV.
//            final IV iv = IVUtility.decode(buf.array());
//
//            // stuff in the map.
//            val2iv.put(value, value);
//            iv2val.put(iv, value);
//            value.setIV(iv); // cache the IV
////            iv.setValue(value); // but do not cache the Value.
//            
//        }
//        
//    }
    
    private void readVersion2(final ObjectInput in) throws IOException {

        // read in the #of declarations.
        final int ndecls = LongPacker.unpackInt(in);

        // read in the #of values.
        final int nvalues = LongPacker.unpackInt(in);

        // read in the checksum.
        final long checksumActual = in.readLong();
        
        // The namespace of the KB instance.
        final String namespace = in.readUTF();

        // Note: The value factory uses the namespace of the KB instance!
        valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
        
        // Initialize the vocabulary.
        init(ndecls, nvalues);
        
//        decls = new LinkedHashSet<VocabularyDecl>(ndecls);
//
//        // allocate the map with sufficient capacity.
//        val2iv = new LinkedHashMap<Value, BigdataValue>(nvalues);
//        iv2val = new LinkedHashMap<IV, BigdataValue>(nvalues);
//
//        for (int i = 0; i < ndecls; i++) {
//
//            final String className = in.readUTF();
//
//            try {
//
//                final Class<?> cls = Class.forName(className);
//
//                if (!VocabularyDecl.class.isAssignableFrom(cls))
//                    throw new IOException(className);
//
//                final VocabularyDecl decl = (VocabularyDecl) cls.newInstance();
//
//                decls.add(decl);
//
//            } catch (InstantiationException e) {
//
//                throw new IOException(e);
//
//            } catch (IllegalAccessException e) {
//
//                throw new IOException(e);
//
//            } catch (ClassNotFoundException e) {
//
//                throw new IOException(e);
//
//            }
//
//        }
//
//        addAllDecls();

        if (ndecls != decls.size()) {
            /*
             * This indicates a versioning problem with the vocabulary
             * declaration classes.
             */
            throw new IOException();
        }

        if (nvalues != val2iv.size()) {
            /*
             * This indicates a versioning problem with the vocabulary
             * declaration classes.
             */
            throw new VocabularyVersioningException();
        }

        // compute a checksum on the hash codes of the URIs.
        long checksum = 0;
        for(Value value : val2iv.keySet()) {
            checksum += value.hashCode();
        }
        
        if (checksum != checksumActual) {
            /*
             * This indicates a versioning problem with the vocabulary
             * declaration classes.
             */
            throw new VocabularyVersioningException();
        }

        generateIVs();
        
    }
    
    public void writeExternal(final ObjectOutput out) throws IOException {

        if (val2iv == null)
            throw new IllegalStateException();
        if (iv2val == null)
            throw new IllegalStateException();

        out.writeShort(currentVersion);

        switch (currentVersion) {
//        case VERSION0:
//            writeVersion0(out);
//            break;
//        case VERSION1:
//            writeVersion1(out);
//            break;
        case VERSION2:
            writeVersion2(out);
            break;
        default:
            throw new AssertionError();
        }
    
    }

//    /**
//     * The old code for {@link #VERSION0}. This is here for historical purposes
//     * only.
//     * 
//     * @param out
//     * @throws IOException
//     */
//    private void writeVersion0(final ObjectOutput out) throws IOException {
//        
//        final int nvalues = val2iv.size();
//        
//        // write on the #of values.
//        out.writeInt(nvalues);
//
//        // reused for each serialized term.
//        final DataOutputBuffer buf = new DataOutputBuffer();
//        final ByteArrayBuffer tbuf = new ByteArrayBuffer();
//
//        final BigdataValueSerializer<Value> valueSer = new BigdataValueSerializer<Value>(
//                new ValueFactoryImpl());
//
//        final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
//
//        final Iterator<Map.Entry<BigdataValue, IV>> itr = val2iv.entrySet()
//                .iterator();
//
//        while (itr.hasNext()) {
//
//            final Map.Entry<BigdataValue, IV> entry = itr.next();
//
//            final BigdataValue value = entry.getKey();
//
//            final IV iv = entry.getValue();
//
//            assert value != null;
//
//            assert iv != null;
//
//            // reset the buffer.
//            buf.reset();
//
//            // serialize the Value onto the buffer.
//            valueSer.serialize(value, buf, tbuf);
//
//            // #of bytes in the serialized value.
//            final int nbytes = buf.limit();
//
//            // write #of bytes on the output stream.
//            out.writeInt(nbytes);
//
//            // copy serialized value onto the output stream.
//            out.write(buf.array(), 0, buf.limit());
//
//            final byte[] b = iv.encode(keyBuilder.reset()).getKey();
//
//            out.writeInt(b.length);
//
//            out.write(b);
//
//        }
//
//    }

//    private void writeVersion1(final ObjectOutput out) throws IOException {
//        
//        final int nvalues = val2iv.size();
//        assert iv2val.size() == nvalues;
//        
//        // write on the #of values.
//        LongPacker.packLong(out, nvalues);
//
//        // The namespace of the KB instance.
//        out.writeUTF(valueFactory.getNamespace());
//        
//        // reused for each serialized term.
//        final DataOutputBuffer buf = new DataOutputBuffer();
//        final ByteArrayBuffer tbuf = new ByteArrayBuffer();
//        final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
//
//        final BigdataValueSerializer<Value> valueSer = new BigdataValueSerializer<Value>(
//                valueFactory);
//
//        final Iterator<Map.Entry<Value, BigdataValue>> itr = val2iv.entrySet()
//                .iterator();
//
//        while (itr.hasNext()) {
//
//            final Map.Entry<Value, BigdataValue> entry = itr.next();
//
//            final BigdataValue value = entry.getValue();
//
//            final IV iv = value.getIV();
//
//            assert value != null;
//
//            assert iv != null;
//
//            // reset the buffer.
//            buf.reset();
//
//            // serialize the Value onto the buffer.
//            valueSer.serialize(value, buf, tbuf);
//
//            // #of bytes in the serialized value.
//            final int nbytes = buf.limit();
//
//            // write #of bytes on the output stream.
//            LongPacker.packLong(out, nbytes);
//
//            // copy serialized value onto the output stream.
//            out.write(buf.array(), 0, buf.limit());
//
//            // encode the key.
//            iv.encode(keyBuilder.reset());
//
//            // write #of bytes in the IV on the output stream.
//            LongPacker.packLong(out, keyBuilder.len());
//
//            // write the IV on the output stream.
//            out.write(keyBuilder.array(), 0/* off */, keyBuilder.len());
//
//        }
//
//    }

    private void writeVersion2(final ObjectOutput out) throws IOException {

        assert iv2val.size() == val2iv.size();

        // compute a checksum on the hash codes of the URIs.
        long checksum = 0;
        for(Value value : val2iv.keySet()) {
            checksum += value.hashCode();
        }
        
        // write on the #of declarations.
        LongPacker.packLong(out, decls.size());

        // write on the #of values.
        LongPacker.packLong(out, val2iv.size());

        // write out the checksum.
        out.writeLong(checksum);
        
        // The namespace of the KB instance.
        out.writeUTF(valueFactory.getNamespace());

//        for (VocabularyDecl decl : decls) {
//
//            // The class name of the vocabulary declaration.
//            out.writeUTF(decl.getClass().getName());
//
//        }

    }

    /**
     * An instance of this class indicates a versioning problem with the
     * {@link VocabularyDecl declaration classes}. If a vocabulary declaration
     * class is modified after it has been used to instantiate a triple store
     * then the mapping of URIs onto IVs might not be stable with the result
     * that encode and decode of statements may be broken.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class VocabularyVersioningException extends IOException {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
    }
    
}
