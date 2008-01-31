/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 7, 2007
 */

package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.SerializerUtil;

/**
 * <p>
 * The base class for variable length metadata and extension metadata for an
 * {@link IndexSegment} as persisted on an {@link IndexSegmentFileStore}.
 * </p>
 * <p>
 * Note: The {@link IndexSegmentMetadata} class is NOT extensible and is used
 * solely for fixed length metadata common to all {@link IndexSegment}s,
 * including the root addresses required to bootstrap the load of an
 * {@link IndexSegment} from a file. In contrast, this class provides for both
 * required variable length metadata and arbitrary extension metadata for an
 * {@link IndexSegment}.
 * </p>
 * <p>
 * Note: Derived classes SHOULD extend the {@link Externalizable} interface and
 * explicitly manage serialization versions so that their metadata may evolve in
 * a backward compatible manner.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentExtensionMetadata implements Serializable, Externalizable {

    private static final long serialVersionUID = 4846316492768402991L;

    private String className;
    private IKeySerializer keySer;
    private IValueSerializer valSer;
    private RecordCompressor recordCompressor;
    
    /**
     * Either {@link IndexSegment} or a derived class that will be instantiated
     * when the index segment is loaded using
     * {@link IndexSegmentFileStore#load()}
     */
    public final String getClassName() {
        
        return className;
        
    }
    
    /**
     * The serializer used for the keys in the nodes and leaves of the index.
     */
    public final IKeySerializer getKeySerializer() {
        
        return keySer;
        
    }
    
    /**
     * The serializer used for the values in the leaves of the index.
     */
    public final IValueSerializer getValueSerializer() {
        
        return valSer;
        
    }
    
    /**
     * When non-null, a {@link RecordCompressor} that was used to write the
     * nodes and leaves of the {@link IndexSegment}.
     * 
     * @todo modify to use an interface.
     */
    final public RecordCompressor getRecordCompressor() {
        
        return recordCompressor;
        
    }
    
    /**
     * De-serialization constructor.
     */
    public IndexSegmentExtensionMetadata() {
        
    }
    
    /**
     * 
     * @param cl
     *            The name of the {@link IndexSegment} class that will be
     *            instantiated when the {@link IndexSegment} is loaded from the
     *            file.
     * 
     * @param valSer
     *            The object responsible for (de-)serializing the values in the
     *            leaves of the B+-Tree.
     * 
     * @param recordCompressor
     *            When non-null, a {@link RecordCompressor} that was used to
     *            write the nodes and leaves of the {@link IndexSegment}.
     */
    public IndexSegmentExtensionMetadata(Class cl, IKeySerializer keySer,
            IValueSerializer valSer, RecordCompressor recordCompressor) {

        if( cl == null ) throw new IllegalArgumentException();
        
        if( ! IndexSegment.class.isAssignableFrom(cl) ) {
            
            throw new IllegalArgumentException("Does not extend: "
                    + IndexSegment.class);
            
        }

        if( valSer == null ) throw new IllegalArgumentException();

        this.className = cl.getName();
        
        this.keySer = keySer;
        
        this.valSer = valSer;
        
        this.recordCompressor = recordCompressor;
        
    }

    /**
     * Read the extension metadataMap record from the store.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the extension metadataMap record.
     * 
     * @return the extension metadataMap record.
     * 
     * @see IndexSegmentFileStore#load(), which will return an
     *      {@link IndexSegment} that is ready for use.
     */
    public static IndexSegmentExtensionMetadata read(IndexSegmentFileStore store, long addr) {
        
        return (IndexSegmentExtensionMetadata) SerializerUtil.deserialize(store.read(addr));
        
    }

    private static final transient int VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    
        final int version = (int)LongPacker.unpackLong(in);
        
        if (version != VERSION0) {

            throw new IOException("Unknown version: version=" + version);
            
        }
        
        className = in.readUTF();
        
        keySer = (IKeySerializer) in.readObject();
        
        valSer = (IValueSerializer) in.readObject();
        
        recordCompressor = (RecordCompressor) in.readObject();
        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {

        LongPacker.packLong(out,VERSION0);

        out.writeUTF(className);

        out.writeObject(keySer);
        
        out.writeObject(valSer);
        
        out.writeObject(recordCompressor);
        
    }

}
