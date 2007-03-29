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
import java.io.Externalizable;
import java.io.IOException;
import java.util.UUID;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.Journal;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.IndexSegment;

/**
 * A description of the {@link IndexSegment}s containing the user data for a
 * partition.
 * 
 * @todo provide a persistent event log or just integrate the state changes over
 *       the historical states of the partition description in the metadata
 *       index?
 * 
 * @todo aggregate resource load statistics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionMetadata /*implements Externalizable*/ {

    /**
     * The unique partition identifier.
     */
    final protected int partId;

    /**
     * The ordered list of data services on which data for this partition will
     * be written and from which data for this partition may be read.
     */
    final protected UUID[] dataServices;
    
    /**
     * Zero or more files containing {@link Journal}s or {@link IndexSegment}s
     * holding live data for this partition. The entries in the array reflect
     * the creation time of the index segments. The earliest segment is listed
     * first. The most recently created segment is listed last. Only the
     * {@link ResourceState#Live} resources must be read in order to provide a
     * consistent view of the data for the index partition.
     * {@link ResourceState#Dead} resources will eventually be scheduled for
     * restart-safe deletion.
     * 
     * @see ResourceState
     */
    final protected IResourceMetadata[] resources;

    public PartitionMetadata(int partId, UUID[] dataServices ) {

        this(partId, dataServices, new IResourceMetadata[] {});

    }

    /**
     * 
     * @param partId
     *            The unique partition identifier assigned by the
     *            {@link MetadataIndex}.
     * @param dataServices
     *            The ordered array of data service identifiers on which data
     *            for this partition will be written and from which data for
     *            this partition may be read.
     * @param resources
     *            A description of each {@link Journal} or {@link IndexSegment}
     *            resource associated with that partition.
     */
    public PartitionMetadata(int partId, UUID[] dataServices,
            IResourceMetadata[] resources) {

        if (dataServices == null)
            throw new IllegalArgumentException();

        if (resources == null)
            throw new IllegalArgumentException();
        
        this.partId = partId;
        
        this.dataServices = dataServices;
        
        this.resources = resources;

    }

    /**
     * The #of data services on which the data for this partition will be
     * written and from which they may be read.
     * 
     * @return The replication count for the index partition.
     */
    public int getDataServiceCount() {
        
        return dataServices.length;
        
    }
    
    /**
     * The ordered list of data services on which the data for this partition
     * will be written and from which the data for this partition may be read.
     * The first data service is always the primary. Writes SHOULD be pipelined
     * from the primary to the secondaries in the same order as they appear in
     * this array.
     * 
     * @return A copy of the array of data service identifiers.
     */
    public UUID[] getDataServices() {
        
        return dataServices.clone();
        
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

        for (int i = 0; i < resources.length; i++) {

            if (resources[i].state() == ResourceState.Live)
                count++;

        }

        return count;

    }

    /**
     * Return an ordered array of the filenames for the live index segments.
     */
    public String[] getLiveSegmentFiles() {

        final int n = getLiveCount();

        String[] files = new String[n];

        int k = 0;
        
        for (int i = 0; i < resources.length; i++) {

            if (resources[i].state() == ResourceState.Live) {

                files[k++] = resources[i].getFile();
                
            }

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

        if (dataServices.length != o2.dataServices.length)
            return false;

        if (resources.length != o2.resources.length)
            return false;

        for (int i = 0; i < dataServices.length; i++) {

            if (!dataServices[i].equals(o2.dataServices[i]))
                return false;

        }
        
        for (int i = 0; i < resources.length; i++) {

            if (!resources[i].equals(o2.resources[i]))
                return false;

        }

        return true;

    }

    public String toString() {

        return "" + partId;

    }

    /**
     * Serialization for an index segment metadata entry.
     * 
     * FIXME convert to use {@link UnisolatedBTree} (so byte[] values that we
     * (de-)serialize one a one-by-one basis ourselves), implement
     * {@link Externalizable} and use explicit versioning and packed integers.
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

//        private static final transient int VERSION0 = 0x0;
        
        public Serializer() {
        }

        public void putValues(DataOutputStream os, Object[] values, int nvals)
                throws IOException {
            
            for (int i = 0; i < nvals; i++) {

                PartitionMetadata val = (PartitionMetadata) values[i];

                final int nservices = val.dataServices.length;
                
                final int nresources = val.resources.length;
                
                os.writeInt(val.partId);
                
                os.writeInt(nservices);

                os.writeInt(nresources);

                for( int j=0; j<nservices; j++) {
                    
                    final UUID serviceUUID = val.dataServices[j];
                    
                    os.writeLong(serviceUUID.getMostSignificantBits());
                    
                    os.writeLong(serviceUUID.getLeastSignificantBits());
                    
                }
                
                for (int j = 0; j < nresources; j++) {

                    IResourceMetadata rmd = val.resources[j];

                    os.writeBoolean(rmd.isIndexSegment());
                    
                    os.writeUTF(rmd.getFile());

                    os.writeLong(rmd.size());

                    os.writeInt(rmd.state().valueOf());

                    final UUID resourceUUID = rmd.getUUID();
                    
                    os.writeLong(resourceUUID.getMostSignificantBits());
                    
                    os.writeLong(resourceUUID.getLeastSignificantBits());

                }

            }

        }

        public void getValues(DataInputStream is, Object[] values, int nvals)
                throws IOException {

            for (int i = 0; i < nvals; i++) {

                final int partId = is.readInt();

                final int nservices = is.readInt();
                
                final int nresources = is.readInt();
                
                final UUID[] services = new UUID[nservices];
                
                final IResourceMetadata[] resources = new IResourceMetadata[nresources];

                for (int j = 0; j < nservices; j++) {

                    services[j] = new UUID(is.readLong()/*MSB*/,is.readLong()/*LSB*/);
                    
                }
                
                for (int j = 0; j < nresources; j++) {

                    boolean isIndexSegment = is.readBoolean();
                    
                    String filename = is.readUTF();

                    long nbytes = is.readLong();

                    ResourceState state = ResourceState.valueOf(is.readInt());

                    UUID uuid = new UUID(is.readLong()/*MSB*/,is.readLong()/*LSB*/);

                    resources[j] = (isIndexSegment ? new SegmentMetadata(
                            filename, nbytes, state, uuid)
                            : new JournalMetadata(filename, nbytes, state, uuid));

                }

                values[i] = new PartitionMetadata(partId, services, resources);

            }

        }

    }

}
