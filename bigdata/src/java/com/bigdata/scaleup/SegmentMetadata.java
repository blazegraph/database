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

import java.util.UUID;

import com.bigdata.btree.IndexSegment;

/**
 * Metadata for a single {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo make fields protected/private.
 */
public class SegmentMetadata implements IResourceMetadata {

    /**
     * The name of the file containing the {@link IndexSegment}.
     */
    private String filename;
    
    /**
     * The size of that file in bytes.
     */
    private long nbytes;
    
    /**
     * The life-cycle state for that {@link IndexSegment}.
     */
    private ResourceState state;
    
    private UUID uuid;
    
    public final boolean isIndexSegment() {
        
        return true;
        
    }
    
    public final boolean isJournal() {
        
        return false;
        
    }
    
    public SegmentMetadata(String filename,long nbytes,ResourceState state, UUID uuid ) {

        this.filename = filename;
        
        this.nbytes = nbytes;
        
        this.state = state;
        
        this.uuid = uuid;
        
    }

    // Note: used by assertEquals in the test cases.
    public boolean equals(Object o) {
        
        if(this == o)return true;
        
        SegmentMetadata o2 = (SegmentMetadata)o;
        
        if (filename.equals(o2.filename) && nbytes == o2.nbytes
                && state == o2.state && uuid.equals(o2.uuid))
            return true;
        
        return false;
        
    }

    final public String getFile() {
        
        return filename;
        
    }

    final public long size() {
        
        return nbytes;
    }

    final public ResourceState state() {
        
        return state;
        
    }

    final public UUID getUUID() {
        
        return uuid;
        
    }
    
}
