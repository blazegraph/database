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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;


/**
 * A read-only view of an {@link ICommitRecord}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CommitRecord implements ICommitRecord {

    private final long timestamp;
    private final long commitCounter;
    private final long[] roots;

    /**
     * @todo this may not be the correct commit counter unless this method is
     *       synchronized with the writeService.
     * 
     * @todo are commit counters global or local?
     */
    public CommitRecord() {

        this(0L,0L,new long[ICommitRecord.MAX_ROOT_ADDRS]);
        
    }

    public CommitRecord(long timestamp, long commitCounter, long[] roots) {
        
//        assert timestamp != 0L; // @todo what constraint?
        
        assert roots != null;
        
        assert roots.length == ICommitRecord.MAX_ROOT_ADDRS;
        
        this.timestamp = timestamp;
        
        this.commitCounter = commitCounter;
        
        this.roots = roots;
        
    }

    final public long getTimestamp() {
        
        return timestamp;
        
    }

    final public long getCommitCounter() {
        
        return commitCounter;
        
    }
    
    final public int getRootAddrCount() {
        
        return roots.length;
        
    }
    
    final public long getRootAddr(int index) {
        
        return roots[index];
        
    }

    public String toString() {
        
        StringBuffer sb = new StringBuffer();
        
        sb.append("CommitRecord");
        
        sb.append("{timestamp="+timestamp);
        
        sb.append(", commitCounter="+commitCounter);
        
        sb.append(", roots=[");
        
        for( int i=0; i< roots.length; i++) {
            
            if(i>0) sb.append(", ");
            
            sb.append(roots[i]);
            
        }
        
        sb.append("]}");
        
        return sb.toString();
        
    }
    
}
