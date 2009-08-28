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
 * Created on May 28, 2008
 */

package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import com.bigdata.btree.compression.DefaultDataSerializer;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.compression.PrefixSerializer;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.ThreadLocalKeyBuilderFactory;
import com.bigdata.io.SerializerUtil;

/**
 * Default implementation uses the {@link KeyBuilder} to format the object as a
 * key and uses Java default serialization for the value. You only need to
 * subclass this if you want to use custom (de-)serialization of the value,
 * custom conversion of the application key to an unsigned byte[], or if you
 * have a special type of application key such that you are able to decode the
 * unsigned byte[] and materialize the corresponding application key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultTupleSerializer<K extends Object, V extends Object>
        implements ITupleSerializer<K, V>, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 2211020411074955099L;

    /**
     * The default for {@link IKeyBuilderFactory}.
     * 
     * @deprecated by {@link IndexMetadata.Options#KEY_BUILDER_FACTORY}
     */
    static public final IKeyBuilderFactory getDefaultKeyBuilderFactory() {
        
        return new DefaultKeyBuilderFactory(new Properties());
    }
    
    /**
     * The default for {@link #getLeafKeySerializer()} (compression for the keys
     * stored in a leaf).
     * 
     * @deprecated by {@link IndexMetadata.Options#LEAF_KEY_SERIALIZER}
     */
    static public final IDataSerializer getDefaultLeafKeySerializer() {
        
        return PrefixSerializer.INSTANCE;
        
    }
    
    /**
     * The default for {@link #getLeafValueSerializer()} (compression for
     * the values stored in a leaf).
     * 
     * @deprecated by {@link IndexMetadata.Options#LEAF_VALUE_SERIALIZER}
     */
    static public final IDataSerializer getDefaultValueKeySerializer() {
        
        return DefaultDataSerializer.INSTANCE;
        
    }
    
    private IDataSerializer leafKeySer;
    private IDataSerializer leafValSer;

    final public IDataSerializer getLeafKeySerializer() {
        
        return leafKeySer;
        
    }

    final public IDataSerializer getLeafValueSerializer() {

        return leafValSer;
        
    }

    /**
     * Override the {@link #getLeafKeySerializer()}. It is NOT safe to change
     * this value once data have been stored in an {@link IIndex} using another
     * value as existing data MAY become unreadable.
     * 
     * @param leafKeySer
     *            The new value.
     */
    final public void setLeafKeySerializer(final IDataSerializer leafKeySer) {

        if (leafKeySer == null)
            throw new IllegalArgumentException();

        this.leafKeySer = leafKeySer;
        
    }

    /**
     * Override the {@link #getLeafValueSerializer()}. It is NOT safe to change
     * this value once data have been stored in an {@link IIndex} using another
     * value as existing data MAY become unreadable.
     * 
     * @param valueSer
     *            The new value.
     */
    final public void setLeafValueSerializer(final IDataSerializer valueSer) {
        
        if (valueSer == null)
            throw new IllegalArgumentException();

        this.leafValSer = valueSer;
        
    }

    /**
     * Factory for a new instance using default values for the
     * {@link #getKeyBuilder()}, the {@link #getLeafKeySerializer()}, and the
     * {@link #getLeafValueSerializer()}.
     */
    public static ITupleSerializer newInstance() {

        return new DefaultTupleSerializer(getDefaultKeyBuilderFactory());
        
    }

    /**
     * The factory specified to the ctor.
     */
    private IKeyBuilderFactory delegateKeyBuilderFactory;

    /**
     * The {@link #delegateKeyBuilderFactory} wrapped up in thread-local
     * factory.
     */
    private transient IKeyBuilderFactory threadLocalKeyBuilderFactory;
    
    /**
     * De-serialization ctor <em>only</em>.
     */
    public DefaultTupleSerializer() {
        
    }

    /**
     * 
     * @param keyBuilderFactory
     *            The {@link IKeyBuilderFactory}, which will be automatically
     *            wrapped up by a {@link ThreadLocalKeyBuilderFactory}.
     */
    public DefaultTupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {
        
        this(keyBuilderFactory, getDefaultLeafKeySerializer(),
                getDefaultValueKeySerializer());
        
    }

    public DefaultTupleSerializer(final IKeyBuilderFactory keyBuilderFactory,
            final IDataSerializer leafKeySer, final IDataSerializer leafValSer) {

        if (keyBuilderFactory == null)
            throw new IllegalArgumentException();

        if (leafKeySer == null)
            throw new IllegalArgumentException();

        if (leafValSer == null)
            throw new IllegalArgumentException();
        
        threadLocalKeyBuilderFactory = new ThreadLocalKeyBuilderFactory(
                keyBuilderFactory);
        
        this.delegateKeyBuilderFactory = keyBuilderFactory;

        this.leafKeySer = leafKeySer;
        
        this.leafValSer = leafValSer;
        
//        this.leafKeySer = SimplePrefixSerializer.INSTANCE;
//        
//        this.leafValSer = DefaultDataSerializer.INSTANCE;
        
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName()+"{");
        sb.append(", keyBuilderFactory="+delegateKeyBuilderFactory);
        sb.append(", leafKeySerializer=" + leafKeySer.getClass().getName());
        sb.append(", leafValueSerializer=" + leafValSer.getClass().getName());
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * A thread-local {@link IKeyBuilder} instance.
     * <p>
     * Note: By default, the {@link #getKeyBuilder()} uses whatever default is
     * in place on the host/JVM where the {@link DefaultTupleSerializer}
     * instance was first created. That backing {@link IKeyBuilderFactory}
     * object is serialized along with the {@link DefaultTupleSerializer} so
     * that the specific configuration values are persisted, even when the
     * {@link DefaultTupleSerializer} is de-serialized on a different host.
     */
    final public IKeyBuilder getKeyBuilder() {

        if(threadLocalKeyBuilderFactory == null) {
            
            /*
             * This can happen if you use the de-serialization ctor by mistake.
             */
            
            throw new IllegalStateException();
            
        }
        
        return threadLocalKeyBuilderFactory.getKeyBuilder();

    }
    
    public byte[] serializeKey(Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        return getKeyBuilder().reset().append(obj).getKey();
        
    }

    /**
     * Serializes the object as a byte[] using Java default serialization.
     * 
     * @param obj
     *            The object to be serialized (MAY be <code>null</code>).
     * 
     * @return The serialized representation of the object as a byte[] -or-
     *         <code>null</code> if the reference is <code>null</code>.
     */
    public byte[] serializeVal(Object obj) {

        return SerializerUtil.serialize(obj);
        
    }

    /**
     * De-serializes an object from the {@link ITuple#getValue() value} stored
     * in the tuple (ignores the key stored in the tuple).
     */
    public V deserialize(ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();

        // @todo tuple.getValueStream()
        return (V)SerializerUtil.deserialize(tuple.getValue());
        
    }

    /**
     * This is an unsupported operation. Additional information is required to
     * either decode the internal unsigned byte[] keys or to extract the key
     * from the de-serialized value (if it is being stored in that value). You
     * can either write your own {@link ITupleSerializer} or you can specialize
     * this one so that it can de-serialize your keys using whichever approach
     * makes the most sense for your data.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public K deserializeKey(ITuple tuple) {
        
        throw new UnsupportedOperationException();
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        delegateKeyBuilderFactory = (IKeyBuilderFactory)in.readObject();
        
        threadLocalKeyBuilderFactory = new ThreadLocalKeyBuilderFactory(delegateKeyBuilderFactory);
        
        leafKeySer = (IDataSerializer)in.readObject();

        leafValSer = (IDataSerializer)in.readObject();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeObject(delegateKeyBuilderFactory);
        
        out.writeObject(leafKeySer);

        out.writeObject(leafValSer);
        
    }

}
