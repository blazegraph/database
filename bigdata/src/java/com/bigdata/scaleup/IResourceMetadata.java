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

import java.io.File;
import java.util.UUID;

import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentMetadata;
import com.bigdata.journal.Journal;

/**
 * Interface for metadata about a {@link Journal} or {@link IndexSegment}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IResourceMetadata {

    /**
     * True iff this resource is an {@link IndexSegment}. Each
     * {@link IndexSegment} contains historical read-only data for exactly one
     * partition of a scale-out index.
     */
    public boolean isIndexSegment();
    
    /**
     * True iff this resource is a {@link Journal}. When the resource is a
     * {@link Journal}, there will be a named mutable btree on the journal that
     * is absorbing writes for one or more index partition of a scale-out index.
     */
    public boolean isJournal();
    
    /**
     * The name of the file containing the resource.
     */
    public String getFile();
    
    /**
     * The #of bytes in the store file.
     */
    public long size();

    /**
     * The life cycle state of that store file.
     */
    public ResourceState state();

    /**
     * The unique identifier for the resource (the UUID found in either the
     * journal root block or the {@link IndexSegmentMetadata}).
     */
    public UUID getUUID();
    
//    public int hashCode();
//    
//    public boolean equals(IResourceMetadata o);

}
