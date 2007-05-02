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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.Journal;

/**
 * 
 * @todo provide a persistent event log or just integrate the state changes over
 *       the historical states of the partition description in the metadata
 *       index?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionMetadata implements IPartitionMetadata, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 5234405541356126104L;

    /**
     * The unique partition identifier.
     */
    private int partId;

    /**
     * The ordered list of data services on which data for this partition will
     * be written and from which data for this partition may be read.
     * 
     * @todo refactor into a dataService UUID (required) and an array of zero or
     *       more media replication services for failover.
     */
    private UUID[] dataServices;
    
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
    private IResourceMetadata[] resources;
    
    /**
     * De-serialization constructor.
     */
    public PartitionMetadata() {
        
    }

    /**
     * @deprecated this is used by the test suite only.
     */
    /*public*/ PartitionMetadata(int partId, UUID[] dataServices ) {

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

    public int getPartitionId() {
        return partId;
    }

    public IResourceMetadata[] getResources() {
        return resources;
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

    private static final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = ShortPacker.unpackShort(in);
        
        if(version!=VERSION0) {
            
            throw new IOException("Unknown version: "+version);
            
        }
        
        partId = (int) LongPacker.unpackLong(in);

        final int nservices = ShortPacker.unpackShort(in);
        
        final int nresources = ShortPacker.unpackShort(in);
        
        dataServices = new UUID[nservices];
        
        resources = new IResourceMetadata[nresources];

        for (int j = 0; j < nservices; j++) {

            dataServices[j] = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);
            
        }
        
        for (int j = 0; j < nresources; j++) {

            boolean isIndexSegment = in.readBoolean();
            
            String filename = in.readUTF();

            long nbytes = LongPacker.unpackLong(in);

            ResourceState state = ResourceState.valueOf(ShortPacker.unpackShort(in));

            UUID uuid = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);

            resources[j] = (isIndexSegment ? new SegmentMetadata(
                    filename, nbytes, state, uuid)
                    : new JournalMetadata(filename, nbytes, state, uuid));

        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);
        
        final int nservices = dataServices.length;
        
        final int nresources = resources.length;
        
        assert nservices < Short.MAX_VALUE;

        assert nresources < Short.MAX_VALUE;
        
        LongPacker.packLong(out,partId);
        
        ShortPacker.packShort(out,(short)nservices);

        ShortPacker.packShort(out,(short)nresources);

        for( int j=0; j<nservices; j++) {
            
            final UUID serviceUUID = dataServices[j];
            
            out.writeLong(serviceUUID.getMostSignificantBits());
            
            out.writeLong(serviceUUID.getLeastSignificantBits());
            
        }
        
        /*
         * Note: we serialize using the IResourceMetadata interface so that we
         * can handle different subclasses and then special case the
         * deserialization based on the boolean flag. This is significantly more
         * compact than using an Externalizable for each ResourceMetadata object
         * since we do not have to write the class names for those objects.
         */
        for (int j = 0; j < nresources; j++) {

            IResourceMetadata rmd = resources[j];

            out.writeBoolean(rmd.isIndexSegment());
            
            out.writeUTF(rmd.getFile());

            LongPacker.packLong(out,rmd.size());

            ShortPacker.packShort(out,rmd.state().valueOf());

            final UUID resourceUUID = rmd.getUUID();
            
            out.writeLong(resourceUUID.getMostSignificantBits());
            
            out.writeLong(resourceUUID.getLeastSignificantBits());

        }

    }

}
