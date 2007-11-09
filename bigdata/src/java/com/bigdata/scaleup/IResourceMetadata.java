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
package com.bigdata.scaleup;

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
    
    /**
     * The hash code of the {@link #getUUID() resource UUID}.
     */
    public int hashCode();

    /**
     * Compares two resource metadata objects for consistent state.
     * @param o
     * @return
     */
    public boolean equals(IResourceMetadata o);

}
