/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 26, 2008
 */

package com.bigdata.rdf.vocab;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IConstant;

/**
 * Base class for {@link Vocabulary} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BaseVocabulary implements Vocabulary, Externalizable {

    final static public Logger log = Logger.getLogger(BaseVocabulary.class);

    /**
     * The database that is the authority for the defined terms and term
     * identifiers. This will be <code>null</code> when the de-serialization
     * ctor is used.
     */
    final private transient AbstractTripleStore database;

    /**
     * The {@link Value}s together with their assigned term identifiers.
     */
    private Map<Value, IV> values;
    
    /**
     * De-serialization ctor. 
     */
    protected BaseVocabulary() {
        
        this.database = null;
        
    }
    
    /**
     * Ctor used by {@link AbstractTripleStore#create()}.
     * 
     * @param database
     *            The database.
     */
    protected BaseVocabulary(AbstractTripleStore database) {
        
        if (database == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
    }

    /**
     * Uses {@link #addAxioms(Collection)} to collect the declared axioms and
     * then writes the axioms onto the database specified to the
     * {@link BaseVocabulary#BaseVocabulary(AbstractTripleStore)} ctor.
     * 
     * @throws IllegalStateException
     *             if that ctor was not used.
     * @throws IllegalStateException
     *             if {@link #init()} has already been invoked.
     */
    final public void init() {

        if (database == null)
            throw new IllegalStateException();

        if (values != null)
            throw new IllegalStateException();
        
        // setup [values] collection.
        values = new HashMap<Value, IV>(200);

        // obtain collection of values to be used.
        addValues();

        // write values onto the database lexicon.
        writeValues();
        
    }
    
    /**
     * Add all {@link Value}s declared by this class.
     * <p>
     * Note: Subclasses MUST extend this method to add their {@link Value}s.
     */
    protected void addValues() {

        if (values == null)
            throw new IllegalStateException();
        
        // NOP.
        
    }
    
    /**
     * Adds a {@link Value} into the internal collection.
     * 
     * @param value
     *            The value.
     * 
     * @throws IllegalArgumentException
     *             if the value is <code>null</code>.
     */
    final protected void add(Value value) {

        if (database == null)
            throw new IllegalStateException();

        if (values == null)
            throw new IllegalStateException();

        if (value == null)
            throw new IllegalArgumentException();
        
        // convert to BigdataValues when adding to the map.
        values.put(database.getValueFactory().asValue(value), null);
        
    }
    
    /**
     * Writes the values onto the lexicon. Note that the {@link Value}s are
     * converted to {@link BigdataValue}s by {@link #add(Value)} so that we can
     * invoke {@link AbstractTripleStore#addTerms(BigdataValue[])} directly and
     * get back the assigned term identifiers. However, we can not de-serialize
     * the {@link Value}s as {@link BigdataValue}s because we do not have the
     * {@link AbstractTripleStore} reference on hand at that time.
     */
    private void writeValues() {
        
        if (database == null)
            throw new IllegalStateException();
        
        // the distinct set of values to be defined.
        final BigdataValue[] a = values.keySet().toArray(new BigdataValue[] {});

        // write on the database.
        database.getLexiconRelation()
                .addTerms(a, a.length, false/* readOnly */);

        // pair values with their assigned term identifiers.
        for (BigdataValue v : a) {
            
            values.put(v, v.getIV());
            
        }
        
    }
    
    final public int size() {
        
        if (values == null)
            throw new IllegalStateException();
        
        return values.size();
        
    }

    final public Iterator<Value> values() {
        
        return Collections.unmodifiableMap(values).keySet().iterator();
        
    }
    
    final public IV get(Value value) {

        if (values == null)
            throw new IllegalStateException();
        
        if (value == null)
            throw new IllegalArgumentException();
        
        final IV iv = values.get(value);
        
        if (iv == null)
            throw new IllegalArgumentException("Not defined: " + value);

        return iv;

    }

    final public IConstant<IV> getConstant(Value value) {

        if (values == null)
            throw new IllegalStateException();

        if (value == null)
            throw new IllegalArgumentException();

        final IV iv = values.get(value);

        if (iv == null)
            throw new IllegalArgumentException("Not defined: " + value);

        return new Constant<IV>(iv);

    }

    /**
     * Note: The de-serialized state contains {@link Value}s but not
     * {@link BigdataValue}s since the {@link AbstractTripleStore} reference is
     * not available and we can not obtain the appropriate
     * {@link BigdataValueFactory} instance without it. This should not matter
     * since the only access to the {@link Value}s is via {@link #get(Value)}
     * and {@link #getConstant(Value)}.
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        if (values != null)
            throw new IllegalStateException();
        
        final ValueFactory valueFactory = new ValueFactoryImpl();

        final BigdataValueSerializer<Value> valueSer = new BigdataValueSerializer<Value>(
                valueFactory);

        // read in the #of values.
        final int nvalues = in.readInt();
        
        if (nvalues < 0)
            throw new IOException();
        
        // allocate the map with sufficient capacity.
        values = new HashMap<Value,IV>(nvalues);
        
        for (int i = 0; i < nvalues; i++) {
            
            // #of bytes in the serialized value.
            int nbytes = in.readInt();

            // allocate array of that many bytes.
            byte[] b = new byte[nbytes];
            
            // read the data for the serialized value.
            in.readFully(b);

            // de-serialize value (NOT a BigdataValue!)
            final Value value = valueSer.deserialize(b);

            nbytes = in.readInt();
            
            b = new byte[nbytes];
            
            in.readFully(b);
            
            final IV iv = IVUtility.decode(b);

            // stuff in the map.
            values.put(value, iv);
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        if (values == null)
            throw new IllegalStateException();
        
        final int nvalues = values.size();

        // write on the #of values.
        out.writeInt(nvalues);
        
        // reused for each serialized term.
        final DataOutputBuffer buf = new DataOutputBuffer(256);
        
        final BigdataValueSerializer<Value> valueSer = new BigdataValueSerializer<Value>(
                new ValueFactoryImpl());
        
        final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
        
        final Iterator<Map.Entry<Value,IV>> itr = values.entrySet().iterator();

        while(itr.hasNext()) {
            
            final Map.Entry<Value, IV> entry = itr.next();
            
            final Value value = entry.getKey();
            
            final IV iv = entry.getValue();

            assert value != null;
            
            assert iv != null;

            // reset the buffer.
            buf.reset();
            
            // serialize the Value onto the buffer.
            valueSer.serialize(value, buf);
            
            // #of bytes in the serialized value.
            final int nbytes = buf.limit();
            
            // write #of bytes on the output stream.
            out.writeInt(nbytes);
            
            // copy serialized value onto the output stream.
            out.write(buf.array(), 0, buf.limit());
            
            final byte[] b = iv.encode(keyBuilder.reset()).getKey(); 
            
            out.writeInt(b.length);
            
            out.write(b);
            
        }
        
    }

}
