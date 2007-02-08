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
package com.bigdata.scaleup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.IndexSegment;

/**
 * A description of the {@link IndexSegment}s containing the user data for
 * a partition.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionMetadata {

    /**
     * The unique partition identifier.
     */
    final int partId;
    
    /**
     * The next unique within partition segment identifier to be assigned.
     */
    final int nextSegId;

    /**
     * Zero or more files containing {@link IndexSegment}s holding live
     * data for this partition. The entries in the array reflect the
     * creation time of the index segments. The earliest segment is listed
     * first. The most recently created segment is listed last.
     */
    final SegmentMetadata[] segs;

    public PartitionMetadata(int partId) {

        this(partId, 0, new SegmentMetadata[] {});

    }

    /**
     * 
     * @param partId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param segs
     *            A description of each {@link IndexSegment} associated with
     *            that partition.
     */
    public PartitionMetadata(int partId, int nextSegId, SegmentMetadata[] segs) {

        this.partId = partId;

        this.nextSegId = nextSegId;
        
        this.segs = segs;

    }

    /**
     * The #of live index segments (those having data that must be included
     * to construct a fused view representing the current state of the
     * partition).
     * 
     * @return The #of live index segments.
     */
    public int getLiveCount() {

        int count = 0;

        for (int i = 0; i < segs.length; i++) {

            if (segs[i].state == IndexSegmentLifeCycleEnum.LIVE)
                count++;

        }

        return count;

    }

    /**
     * Return an ordered array of the filenames for the live index segments.
     */
    public String[] getLiveSegmentFiles() {

        int n = getLiveCount();

        String[] files = new String[n];

        for (int i = 0; i < segs.length; i++) {

            if (segs[i].state == IndexSegmentLifeCycleEnum.LIVE)
                files[i] = segs[i].filename;

        }

        return files;

    }

    // Note: used by assertEquals in the test cases.
    public boolean equals(Object o) {

        if (this == o)
            return true;

        PartitionMetadata o2 = (PartitionMetadata) o;

        if (partId != o2.partId)
            return false;

        if (segs.length != o2.segs.length)
            return false;

        for (int i = 0; i < segs.length; i++) {

            if (!segs[i].equals(o2.segs[i]))
                return false;

        }

        return true;

    }

    public String toString() {

        return "" + partId;

    }

    //        /**
    //         * The metadata about an index segment life cycle as served by a
    //         * specific service instance on some host.
    //         * 
    //         * @todo we need to track load information for the service and the host.
    //         *       however that information probably does not need to be restart
    //         *       safe so it is easily maintained within a rather small hashmap
    //         *       indexed by the service address.
    //         * 
    //         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    //         *         Thompson</a>
    //         * @version $Id$
    //         */
    //        public static class IndexSegmentServiceMetadata {
    //
    //            /**
    //             * The service that is handling this index segment. This service
    //             * typically handles many index segments and multiplexes them on a
    //             * single journal.
    //             * 
    //             * @todo When a client looks up an index segment in the metadata index,
    //             *       what we send them is the set of key-addr entries from the leaf
    //             *       in which the index segment was found. If a request by the
    //             *       client to that service discovers that the service no longer
    //             *       handles a key range, that the service is dead, etc., then the
    //             *       client will have to invalidate its cache entry and lookup the
    //             *       current location of the index segment in the metadata index.
    //             */
    //            public InetSocketAddress addr;
    //            
    //        }
    //
    //        /**
    //         * An array of the services that are registered as handling this index
    //         * segment. One of these services is the master and accepts writes from
    //         * the client. The other services mirror the segment and provide
    //         * redundency for failover and load balancing. The order in which the
    //         * segments are listed in this array could reflect the master (at
    //         * position zero) and the write pipeline from the master to the
    //         * secondaries could be simply the order of the entries in the array.
    //         */
    //        public IndexSegmentServiceMetadata services[];
    //        
    //        /**
    //         * The time that the index segment was started on that service.
    //         */
    //        public long startTime;
    //
    //        /**
    //         * A log of events for the index segment. This could just be a linked
    //         * list of strings that get serialized as a single string. Each event is
    //         * then a semi-structured string, typically generated by a purpose
    //         * specific logging appender.
    //         */
    //        public Vector<Event> eventLog;
    //        
    //        public PartitionMetadata(InetSocketAddress addr) {
    //        }
    //
    //        /**
    //         * An event for an index segment.
    //         * 
    //         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
    //         * @version $Id$
    //         */
    //        public static class Event {
    //        
    ////            public long timestamp;
    //            
    //            public String msg;
    //            
    //            /**
    //             * Serialization for an event.
    //             * 
    //             * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
    //             * @version $Id$
    //             */
    //            public static class Serializer /*implements ...*/{
    //                
    //            }
    //            
    //        }

    /**
     * Serialization for an index segment metadata entry.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Serializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = 4307076612127034103L;

        public transient static final PartitionMetadata.Serializer INSTANCE = new Serializer();

        public Serializer() {
        }

        public void putValues(DataOutputStream os, Object[] values, int nvals)
                throws IOException {

            for (int i = 0; i < nvals; i++) {

                PartitionMetadata val = (PartitionMetadata) values[i];

                final int nsegs = val.segs.length;

                os.writeInt(val.partId);

                os.writeInt(val.nextSegId);

                os.writeInt(nsegs);

                for (int j = 0; j < nsegs; j++) {

                    SegmentMetadata segmentMetadata = val.segs[j];

                    os.writeUTF(segmentMetadata.filename);

                    os.writeLong(segmentMetadata.nbytes);

                    os.writeInt(segmentMetadata.state.valueOf());

                }

            }

        }

        public void getValues(DataInputStream is, Object[] values, int nvals)
                throws IOException {

            for (int i = 0; i < nvals; i++) {

                final int partId = is.readInt();

                final int nextSegId = is.readInt();

                final int nsegs = is.readInt();
                
                PartitionMetadata val = new PartitionMetadata(partId,
                        nextSegId, new SegmentMetadata[nsegs]);

                for (int j = 0; j < nsegs; j++) {

                    String filename = is.readUTF();

                    long nbytes = is.readLong();

                    IndexSegmentLifeCycleEnum state = IndexSegmentLifeCycleEnum
                            .valueOf(is.readInt());

                    val.segs[j] = new SegmentMetadata(filename, nbytes, state);

                }

                values[i] = val;

            }

        }

    }

}
