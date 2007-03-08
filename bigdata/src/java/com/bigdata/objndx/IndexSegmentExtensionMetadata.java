/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 7, 2007
 */

package com.bigdata.objndx;

import java.io.Serializable;

import com.bigdata.io.SerializerUtil;

/**
 * The base class for variable length metadataMap and extension metadataMap for an
 * {@link IndexSegment} as persisted on an {@link IndexSegmentFileStore}. The
 * {@link IndexSegmentMetadata} class is NOT extensible and is used solely for
 * fixed length metadataMap common to all {@link IndexSegment}s, including the
 * root addresses required to bootstrap the load of an {@link IndexSegment} from
 * a file. In contrast, this class provides for both required variable length
 * metadataMap and arbitrary extension metadataMap for an {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentExtensionMetadata implements Serializable {

    private static final long serialVersionUID = 4846316492768402991L;

    /**
     * Either {@link IndexSegment} or a derived class that will be instantiated
     * when the index segment is loaded using
     * {@link IndexSegmentFileStore#load()}
     */
    public final String className;
    
    /**
     * The serializer used for the values in the leaves of the index.
     */
    public final IValueSerializer valSer;
    
    /**
     * When non-null, a {@link RecordCompressor} that was used to write the
     * nodes and leaves of the {@link IndexSegment}.
     * 
     * @todo modify to use an interface.
     */
    final public RecordCompressor recordCompressor;

//    /**
//     * When non-null, a map containing extension metadata.
//     * 
//     * @see #getMetadata(String name)
//     */
//    final private Map<String, Serializable> metadataMap;
//
//    /**
//     * Return the metadata object stored under the key.
//     * 
//     * @param name
//     *            The key.
//     * 
//     * @return The metadata object or <code>null</code> if there is nothing
//     *         stored under that key.
//     */
//    public Serializable getMetadata(String name) {
//        
//        if(metadataMap==null) return null;
//        
//        return metadataMap.get(name);
//        
//    }
    
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
//    * 
//    * @param metadataMap
//    *            An optional serializable map containing application defined
//    *            extension metadata. The map will be serialized with the
//    *            {@link IndexSegmentExtensionMetadata} object as part of the
//    *            {@link IndexSegmentFileStore}.
    public IndexSegmentExtensionMetadata(Class cl, IValueSerializer valSer,
            RecordCompressor recordCompressor) {
//            Map<String, Serializable> metadataMap) {

        if( cl == null ) throw new IllegalArgumentException();
        
        if( ! IndexSegment.class.isAssignableFrom(cl) ) {
            
            throw new IllegalArgumentException("Does not extend: "
                    + IndexSegment.class);
            
        }

        if( valSer == null ) throw new IllegalArgumentException();

        this.className = cl.getName();
        
        this.valSer = valSer;
        
        this.recordCompressor = recordCompressor;
        
//        this.metadataMap = metadataMap;
        
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

}
