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
 * Created on Oct 22, 2006
 */

package com.bigdata.journal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.CognitiveWeb.extser.AbstractExtensibleSerializer;
import org.CognitiveWeb.extser.IExtensibleSerializer;
import org.CognitiveWeb.extser.ISerializer;
import org.CognitiveWeb.extser.LongPacker;

/**
 * Concrete class knows how to maintain its state.
 * 
 * @see ExtensibleSerializerSingleton
 * 
 * @author thompsonbry
 * 
 * FIXME This implementation is NOT restart safe.
 * 
 * FIXME A standalone implementation (in which the journal is the database) can
 * support extser using metadata in the root block. The metadata is just the
 * {@link ISlotAllocation} on which the current extser state resides. In order
 * to keep the {@link ISlotAllocation} to a fixed size in the root block, we can
 * define another implementation that requires the slot allocation to be
 * contiguous and uses just an offset (of the first slot) and the size (of the
 * allocation in bytes). The #of slots is computed from the size plus the offset
 * of the first slot.
 * 
 * FIXME For bigdata, extser needs to be a high concurrency service so that
 * binary data may be migrated among semantics without needing to deserialize
 * and re-serialize the data. Registration of new classes and versions MUST be
 * atomic. There is no means available to unregister a class (logically a write
 * once collection). The extser state can be cached locally by clients, with
 * updates being delivered along with responses to requests. The service itself
 * needs to be replicated and highly available. Persistence for the service can
 * be a journal that grounds out with a specialized static extser instance to
 * handle serialization of index nodes (this is pretty much the only thing that
 * the journal uses extser for itself - the other uses are application facing).
 * 
 * @todo Provide for registration of several classes or versions at once?
 */

public class ExtensibleSerializer
   extends AbstractExtensibleSerializer
{

    private static final long serialVersionUID = -62406796750184962L;

    /**
     * A reference to the {@link Journal}.
     */
    transient private Journal m_recman;

    /**
     * The recid of this serializer.
     */
    transient private long m_recid;
    
    /**
     * Return the record manager.
     */
    public Journal getJournal()
    {
     
        return m_recman;
        
    }

    /**
     * Return the logical row id of this serializer.
     */
    public long getRecid()
    {
        return m_recid;
    }

    /**
     * Deserialization constructor.
     */
    
    public ExtensibleSerializer()
    {
        super();
    }
    
    /**
     * Create a new instance an insert it into the store.
     * 
     * @param journal
     *            The journal.
     * @return The new instance.
     */
    public static ExtensibleSerializer createInstance( Journal journal )
    {
        ExtensibleSerializer ser = new ExtensibleSerializer();
        ser.m_recman = journal;
//        ser.m_recid = recman.insert
//            ( ser,
//              DefaultSerializer.INSTANCE
//              );
        ser.registerSerializers(); // register serializers.
        return ser;
    }

//    /**
//     * Load an existing instance from the store.
//     * 
//     * @param recman
//     * @param recid
//     * @return
//     */
//    
//    public static ExtensibleSerializer load( RecordManager recman, long recid )
//    	throws IOException
//    {
//        ExtensibleSerializer ser = (ExtensibleSerializer) recman.fetch
//            ( recid,
//              DefaultSerializer.INSTANCE
//              );
//        ser.m_recman = recman;
//        ser.m_recid = recid;
//        return ser;
//    }
    
    synchronized protected void update()
    {

//        m_recman.update
//		( m_recid,
//		  this,
//		  DefaultSerializer.INSTANCE
//		  );
        
//        System.err.println
//		( "Updated state: #classes="+getClassCount()+", m_recid="+m_recid+", m_recman="+m_recman
//		  );
        
    }

    // @todo implement.
    public ISerializer getSerializer( long serializerId ) {

        throw new UnsupportedOperationException();
        
//        return (ISerializer) getJournal().fetch( serializerId );
        
    }

    /**
     * Extends the default behavior to also register serializers for the classes
     * with persistent state.
     */
    
    protected void setupSerializers()
    {

        // extend default behavior.
        super.setupSerializers();
        
//        _registerClass(com.bigdata.btree.BTree.class,
//                com.bigdata.btree.BTree.Serializer0.class, (short) 0,
//                false);
//        _registerClass(com.bigdata.btree.BPage.class,
//                com.bigdata.btree.BPage.Serializer0.class, (short) 0,
//                false);
	
        // @todo ??? HashMap is used for the named object directory, so
        // we pre-register a classId for it now.
        _registerClass( HashMap.class );
        
    }

    public DataOutputStream getDataOutputStream(long recid,
            ByteArrayOutputStream baos) throws IOException {
        return new MyDataOutputStream(recid, this, baos);
    }

    public DataInputStream getDataInputStream(long recid,
            ByteArrayInputStream bais) throws IOException {
        return new MyDataInputStream(recid, this, bais);
    }

    public static class MyDataOutputStream extends DataOutputStream {

        protected MyDataOutputStream(long recid,
                IExtensibleSerializer serializer, ByteArrayOutputStream out)
                throws IOException {
            super(recid, serializer, out);
        }

        // @todo are bigdata int64 oids positive integers?
        public int writePackedOId(long oid) throws IOException {
            return LongPacker.packLong(this, oid);
        }

    }

    public static class MyDataInputStream extends DataInputStream {

        protected MyDataInputStream(long recid,
                IExtensibleSerializer serializer, ByteArrayInputStream is)
                throws IOException {
            super(recid, serializer, is);
        }

        // @todo are bigdata int64 oids positive integers?
        public long readPackedOId() throws IOException {
            return LongPacker.unpackLong(this);
        }

    }

}
