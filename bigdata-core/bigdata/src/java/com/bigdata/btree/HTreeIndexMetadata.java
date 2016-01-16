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
package com.bigdata.btree;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.LongPacker;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * HTree specific implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeIndexMetadata extends IndexMetadata {

    private static final long serialVersionUID = -1L;

    /**
     * {@link HTree} specific options.
     */
    public interface Options extends IndexMetadata.Options {
        
        /**
         * The name of a class derived from {@link HTree} that will be used to
         * re-load the index. 
         */
        String HTREE_CLASS_NAME = HTree.class.getName()+".className";

        /**
         * The name of an optional property whose value specifies the number of
         * address bits for an {@link HTree} (default
         * {@value #DEFAULT_HTREE_ADDRESS_BITS}).
         * <p>
         * The #of children for a directory is <code>2^addressBits</code>. For
         * example, a value of <code>10</code> means a <code>10</code> bit
         * address space in the directory. Such a directory would provide direct
         * addressing for <code>1024</code> child references. Given an overhead
         * of <code>8</code> bytes per child address, that would result in an
         * expected page size of 8k before compression.
         * 
         * @see #DEFAULT_HTREE_ADDRESS_BITS
         */
        String HTREE_ADDRESS_BITS = HTree.class.getPackage().getName()
                + ".addressBits";

        String DEFAULT_HTREE_ADDRESS_BITS = "10";

        /**
         * The name of an optional property whose value specifies the fixed by
         * length of the keys in the {@link HTree} -or- ZERO (0) if the key
         * length is unconstrained, in which case variable length keys may be
         * used (default {@value #DEFAULT_HTREE_KEY_LEN}). This may be used in
         * combination with an appropriate {@link IRabaCoder} to optimize to
         * search and encoding of int32 or int64 keys.
         * 
         * @see #DEFAULT_HTREE_KEY_LEN
         */
        String HTREE_KEY_LEN = HTree.class.getPackage().getName()
                + ".keyLen";

        String DEFAULT_HTREE_KEY_LEN = "0";

    }

    /**
     * @see Options#HTREE_CLASS_NAME
     */
    private String htreeClassName;

    /**
     * @see Options#HTREE_ADDRESS_BITS
     */
    private int addressBits;
    
    /**
     * @see Options#HTREE_KEY_LEN
     */
    private int keyLen;
    
    /**
     * The name of a class derived from {@link HTree} that will be used to
     * re-load the index.
     * 
     * @see Options#HTREE_CLASS_NAME
     */
    public final String getHTreeClassName() {

        return htreeClassName;

    }
    
    public void setHTreeClassName(final String className) {

        if (className == null)
            throw new IllegalArgumentException();

        this.htreeClassName = className;
        
    }

    public int getAddressBits() {

        return addressBits;

    }
    
    public void setAddressBits(final int addressBits) {

        this.addressBits = addressBits;

    }
    
    public int getKeyLen() {

        return keyLen;

    }
    
    public void setKeyLen(final int keyLen) {

        this.keyLen = keyLen;

    }
    
    /**
     * <strong>De-serialization constructor only</strong> - DO NOT use this ctor
     * for creating a new instance! It will result in a thrown exception,
     * typically from {@link #firstCheckpoint()}.
     */
	public HTreeIndexMetadata() {
		
		super();
		
	}

    /**
	 * Constructor used to configure a new <em>unnamed</em> {@link HTree}. The
	 * index UUID is set to the given value and all other fields are defaulted
	 * as explained at {@link #HTreeIndexMetadata(Properties, String, UUID)}.
	 * Those defaults may be overridden using the various setter methods, but
	 * some values can not be safely overridden after the index is in use.
	 * 
	 * @param indexUUID
	 *            The indexUUID.
	 * 
	 * @throws IllegalArgumentException
	 *             if the indexUUID is <code>null</code>.
	 */
	public HTreeIndexMetadata(final UUID indexUUID) {
		
		this(null/* name */, indexUUID);
		
	}

	/**
	 * Constructor used to configure a new <em>named</em> {@link BTree}. The
	 * index UUID is set to the given value and all other fields are defaulted
	 * as explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
	 * defaults may be overridden using the various setter methods, but some
	 * values can not be safely overridden after the index is in use.
	 * 
	 * @param name
	 *            The index name. When this is a scale-out index, the same
	 *            <i>name</i> is specified for each index resource. However they
	 *            will be registered on the journal under different names
	 *            depending on the index partition to which they belong.
	 * 
	 * @param indexUUID
	 *            The indexUUID. The same index UUID MUST be used for all
	 *            component indices in a scale-out index.
	 * 
	 * @throws IllegalArgumentException
	 *             if the indexUUID is <code>null</code>.
	 */
	public HTreeIndexMetadata(final String name, final UUID indexUUID) {

		this(null/* name */, System.getProperties(), name, indexUUID);

	}

    /**
	 * Constructor used to configure a new <em>named</em> B+Tree. The index UUID
	 * is set to the given value and all other fields are defaulted as explained
	 * at {@link #getProperty(Properties, String, String, String)}. Those
	 * defaults may be overridden using the various setter methods.
	 * 
	 * @param indexManager
	 *            Optional. When given and when the {@link IIndexManager} is a
	 *            scale-out {@link IBigdataFederation}, this object will be used
	 *            to interpret the {@link Options#INITIAL_DATA_SERVICE}
	 *            property.
	 * @param properties
	 *            Properties object used to overridden the default values for
	 *            this {@link IndexMetadata} instance.
	 * @param namespace
	 *            The index name. When this is a scale-out index, the same
	 *            <i>name</i> is specified for each index resource. However they
	 *            will be registered on the journal under different names
	 *            depending on the index partition to which they belong.
	 * @param indexUUID
	 *            component indices in a scale-out index.
	 *            The indexUUID. The same index UUID MUST be used for all
	 * @param indexType
	 *            Type-safe enumeration specifying the type of the persistence
	 *            class data structure (historically, this was always a B+Tree).
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>properties</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if <i>indexUUID</i> is <code>null</code>.
	 */
	public HTreeIndexMetadata(final IIndexManager indexManager,
			final Properties properties, final String namespace,
			final UUID indexUUID) {

		super(indexManager, properties, namespace, indexUUID,
				IndexTypeEnum.HTree);
		
        /*
         * Intern'd to reduce duplication on the heap.
         */
        this.htreeClassName = getProperty(indexManager, properties, namespace,
                Options.HTREE_CLASS_NAME, HTree.class.getName()).intern();

        this.addressBits = Integer.parseInt(getProperty(indexManager,
                properties, namespace, Options.HTREE_ADDRESS_BITS,
                Options.DEFAULT_HTREE_ADDRESS_BITS));

        this.keyLen = Integer
                .parseInt(getProperty(indexManager, properties, namespace,
                        Options.HTREE_KEY_LEN, Options.DEFAULT_HTREE_KEY_LEN));

	}

	@Override
	protected void toString(final StringBuilder sb) {

		super.toString(sb);
		
		// htree
        sb.append(", htreeClassName=" + htreeClassName);
        sb.append(", addressBits=" + addressBits);
        sb.append(", keyLen=" + keyLen);

	}

    /**
     * The initial version.
     */
    private static transient final int VERSION0 = 0x0;

    /**
     * The version that will be serialized by this class.
     */
    private static transient final int CURRENT_VERSION = VERSION0;

    @Override
	public void readExternal(final ObjectInput in) throws IOException,
			ClassNotFoundException {

		super.readExternal(in);

		final int version = LongPacker.unpackInt(in);
		
		keyLen = LongPacker.unpackInt(in);

		addressBits = LongPacker.unpackInt(in);

		htreeClassName = in.readUTF();

	}

	@Override
	public void writeExternal(final ObjectOutput out) throws IOException {

		super.writeExternal(out);

        final int version = CURRENT_VERSION;

		LongPacker.packLong(out, version);
		
		LongPacker.packLong(out, keyLen);

		LongPacker.packLong(out, addressBits);

		out.writeUTF(htreeClassName);

	}

	@Override
	public HTreeIndexMetadata clone() {

		return (HTreeIndexMetadata) super.clone();
		
	}
	
}
