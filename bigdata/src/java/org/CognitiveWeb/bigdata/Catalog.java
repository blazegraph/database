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
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata;

import java.net.InetAddress;
import java.util.Vector;

/**
 * The database catalog service. The database catalog is stored in segment one
 * (1). It contains entries for each copy of each segment, including itself.
 * While operations on the catalog are atomic using the commit protocol of the
 * segment server, catalog operations occur in separate very short lived
 * transactions and MUST NOT be combined with application transactions. The
 * state of the catalog undergoes atomic changes in which segments are added,
 * replicated, or deleted. The catalog maps segments to segment locators that
 * segment servers.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public class Catalog {

    /**
     * 
     */
    public Catalog() {
        super();
    }
    
    public static class SegmentEntry {

        private long _segmentId;
        private Vector _instances;
        
        public long getSegmentId() {
            return _segmentId;
        }
        
        public InetAddress[] getInstances() {
            return (InetAddress[]) _instances.toArray(new InetAddress[]{});
        }
        
    }

}
