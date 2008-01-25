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

import java.io.File;
import java.util.UUID;

import com.bigdata.journal.Journal;

/**
 * Metadata required to locate a {@link Journal} resource.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JournalMetadata extends AbstractResourceMetadata {

    /**
     * 
     */
    private static final long serialVersionUID = 3783897093328558238L;

    public final boolean isIndexSegment() {
        
        return false;
        
    }
    
    public final boolean isJournal() {
        
        return true;
        
    }

    /**
     * Return the file whose contents are the persistent state for the 
     * journal.
     * 
     * @param journal
     *            The journal.
     *
     * @return The file.
//     * 
//     * @exception IllegalArgumentException
//     *                if the journal is not persistent.
     */
    private static String getFileString(Journal journal) {
    
//        if(journal.getFile()==null) {
//            
//            throw new IllegalArgumentException("Journal is not persistent.");
//            
//        }
        
        File file = journal.getFile();
        
        if(file==null) return "";
        
        return file.toString();
                
    }
    
    /**
     * De-serialization constructor.
     */
    public JournalMetadata() {
        
    }

    /**
     * Note: this assigns a size of zero (0L) since we can not accurately
     * estimate the #of bytes on the journal dedicated to a given partition of a
     * named index.
     */
    public JournalMetadata(Journal journal, ResourceState state) {

        super(getFileString(journal), 0L, state,
                journal.getRootBlockView().getUUID());

    }

    public JournalMetadata(String file, long nbytes, ResourceState state,
            UUID uuid) {

        super(file, nbytes, state, uuid);

    }

}
