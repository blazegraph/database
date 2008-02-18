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
package com.bigdata.mdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.Journal;

/**
 * An immutable object whose state describes an index partition.
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
     */
    private UUID[] dataServices;
    
    /**
     * Zero or more files containing {@link Journal}s or {@link IndexSegment}s
     * holding live data for this partition. The entries in the array reflect
     * the creation time of the resources. The oldest resource is listed first.
     * The most recent resource is listed last. Only the
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
     *            resource associated with that partition. The entries in the
     *            array reflect the creation time of the resources. The earliest
     *            resource is listed first. The most recently created resource
     *            is listed last.
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

        long lastTimestamp = Long.MIN_VALUE;

        for (int i = 0; i < resources.length; i++) {

            final long thisTimestamp = resources[i].getCommitTime();
            
            if ( thisTimestamp <= lastTimestamp) {

                throw new RuntimeException(
                        "Resources are out of timestamp order @ index=" + i+", lastTimestamp="+lastTimestamp+", this timestamp="+thisTimestamp);

            }

            lastTimestamp = resources[i].getCommitTime();
            
        }

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
     * @return An array of the data service identifiers.
     */
    public UUID[] getDataServices() {
        
        return dataServices;
        
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
     * Return only the live resources.
     * 
     * @return The live resources in order from the oldest to the most recent.
     */
    public IResourceMetadata[] getLiveResources() {

        final int n = getLiveCount();
        
        IResourceMetadata[] a = new IResourceMetadata[n];
        
        int j = 0;
        
        for(int i=0; i<resources.length; i++) {
            
            if(resources[i].state()==ResourceState.Live) {
                
                a[j++] = resources[i];
                
            }
            
        }
        
        return a;
        
    }
    
    /**
     * Return an ordered array of the filenames for the live index resources.
     * 
     * @todo is this only the live {@link IndexSegment} filenames? Who uses this
     *       information?
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

    // The hash code of an int is the int.
    public int hashCode() {
        
        return partId;
        
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

        return 
            "{ partitionId="+partId+
            ", dataServices="+Arrays.toString(dataServices)+
            ", resourceMetadata="+Arrays.toString(resources)+
            "}"
            ;

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

            long commitTime = in.readLong();
            
            resources[j] = (isIndexSegment ? new SegmentMetadata(
                    filename, nbytes, state, uuid, commitTime)
                    : new JournalMetadata(filename, nbytes, state, uuid, commitTime));

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
            
            out.writeLong(rmd.getCommitTime());

        }

    }

}
