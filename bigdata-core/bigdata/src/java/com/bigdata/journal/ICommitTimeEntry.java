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
package com.bigdata.journal;


/**
 * Interface for access to the snapshot metadata.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ICommitTimeEntry {

    /**
     * Return the bytes on the disk for the snapshot file.
     */
    public long sizeOnDisk();

    /**
     * The commit counter associated with the index entry.
     */
    public long getCommitCounter();

    /**
     * The commit time associated with the index entry.
     */
    public long getCommitTime();
    
    /**
     * Return the {@link IRootBlockView} of the snapshot.
     */
    public IRootBlockView getRootBlock();
  
//public static class SnapshotRecord implements ISnapshotRecord,
//        Externalizable {
//
//    private static final int VERSION0 = 0x0;
//
//    private static final int currentVersion = VERSION0;
//    
//    /**
//     * Note: This is NOT {@link Serializable}.
//     */
//    private IRootBlockView rootBlock;
//
//    private long sizeOnDisk;
//
//    /**
//     * De-serialization constructor.
//     */
//    public SnapshotRecord() {
//    }
//    
//    public SnapshotRecord(final IRootBlockView rootBlock,
//            final long sizeOnDisk) {
//
//        if (rootBlock == null)
//            throw new IllegalArgumentException();
//
//        if (sizeOnDisk < 0L)
//            throw new IllegalArgumentException();
//
//        this.rootBlock = rootBlock;
//
//        this.sizeOnDisk = sizeOnDisk;
//
//    }
//    
//    @Override
//    public long sizeOnDisk() {
//        return sizeOnDisk;
//    }
//
//    @Override
//    public IRootBlockView getRootBlock() {
//        return rootBlock;
//    }
//
//    @Override
//    public boolean equals(final Object o) {
//        if (this == o)
//            return true;
//        if (!(o instanceof ISnapshotRecord))
//            return false;
//        final ISnapshotRecord t = (ISnapshotRecord) o;
//        if (sizeOnDisk() != t.sizeOnDisk())
//            return false;
//        if (!getRootBlock().equals(t.getRootBlock()))
//            return false;
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        return getRootBlock().hashCode();
//    }
//
//    @Override
//    public void writeExternal(final ObjectOutput out) throws IOException {
//
//        out.writeInt(currentVersion);
//
//        final byte[] a = BytesUtil.getBytes(rootBlock.asReadOnlyBuffer());
//
//        final int sizeOfRootBlock = a.length;
//
//        out.writeInt(sizeOfRootBlock);
//
//        out.write(a, 0, sizeOfRootBlock);
//
//        out.writeLong(sizeOnDisk);
//
//    }
//
//    @Override
//    public void readExternal(final ObjectInput in) throws IOException,
//            ClassNotFoundException {
//
//        final int version = in.readInt();
//
//        switch (version) {
//        case VERSION0:
//            break;
//        default:
//            throw new IOException("Unknown version: " + version);
//        }
//
//        final int sizeOfRootBlock = in.readInt();
//
//        final byte[] a = new byte[sizeOfRootBlock];
//
//        in.readFully(a, 0, sizeOfRootBlock);
//        
//        rootBlock = new RootBlockView(false/* rootBlock0 */,
//                ByteBuffer.wrap(a), ChecksumUtility.getCHK());
//
//        sizeOnDisk = in.readLong();
//        
//    }
//    
//} // SnapshotRecord
//
///**
// * Encapsulates key and value formation.
// * 
// * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
// */
//static protected class TupleSerializer extends
//        DefaultTupleSerializer<Long, ISnapshotRecord> {
//
//    /**
//     * 
//     */
//    private static final long serialVersionUID = -2851852959439807542L;
//
//    /**
//     * De-serialization ctor.
//     */
//    public TupleSerializer() {
//
//        super();
//        
//    }
//
//    /**
//     * Ctor when creating a new instance.
//     * 
//     * @param keyBuilderFactory
//     */
//    public TupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {
//
//        super(keyBuilderFactory);
//
//    }
//    
//    /**
//     * Decodes the key as a commit time.
//     */
//    @Override
//    @SuppressWarnings("rawtypes") 
//    public Long deserializeKey(final ITuple tuple) {
//
//        return KeyBuilder
//                .decodeLong(tuple.getKeyBuffer().array(), 0/* offset */);
//
//    }
//
//    /**
//     * The initial version (no additional persistent state).
//     */
//    private final static transient byte VERSION0 = 0;
//
//    /**
//     * The current version.
//     */
//    private final static transient byte VERSION = VERSION0;
//
//    public void readExternal(final ObjectInput in) throws IOException,
//            ClassNotFoundException {
//
//        super.readExternal(in);
//        
//        final byte version = in.readByte();
//        
//        switch (version) {
//        case VERSION0:
//            break;
//        default:
//            throw new UnsupportedOperationException("Unknown version: "
//                    + version);
//        }
//
//    }
//
//    public void writeExternal(final ObjectOutput out) throws IOException {
//
//        super.writeExternal(out);
//        
//        out.writeByte(VERSION);
//        
//    }

}

