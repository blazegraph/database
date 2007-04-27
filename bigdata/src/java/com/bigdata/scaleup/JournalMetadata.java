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

import com.bigdata.journal.Journal;

/**
 * Metadata required to locate a {@link Journal} resource.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JournalMetadata extends AbstractResourceMetadata {

    public final boolean isIndexSegment() {
        
        return false;
        
    }
    
    public final boolean isJournal() {
        
        return true;
        
    }

    /**
     * Used to verify that the journal is backed by a file.
     * 
     * @param journal
     *            The journal.
     * 
     * @return The journal.
     * 
     * @exception IllegalArgumentException
     *                if the journal is not persistent.
     */
    private static Journal assertPersistent(Journal journal) {

        if(journal.getFile()==null) {
            
            throw new IllegalArgumentException("Journal is not persistent.");
            
        }
        
        return journal;
        
    }
    
    /**
     * Note: this assigns a size of zero (0L) since we can not accurately
     * estimate the #of bytes on the journal dedicated to a given partition of a
     * named index.
     */
    public JournalMetadata(Journal journal, ResourceState state) {

        super(assertPersistent(journal).getFile().toString(), 0L, state,
                journal.getRootBlockView().getUUID());

    }

    public JournalMetadata(String file, long nbytes, ResourceState state,
            UUID uuid) {

        super(file, nbytes, state, uuid);

    }

}
