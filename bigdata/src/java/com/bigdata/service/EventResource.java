/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Feb 12, 2009
 */

package com.bigdata.service;

import java.io.File;
import java.io.Serializable;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * Semi-structured representation of the data service on which the event
 * occurred, the name of the index, and the index partition identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EventResource implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -4809586705672043577L;

    /**
     * The name of the scale-out index and an empty string when not known /
     * available.
     */
    public final String indexName;

    /**
     * The index partition identifier and an empty string when not known /
     * available.
     */
    public final String partitionId;

    /**
     * The file when the resource is a component of an index partition view (the
     * journal or index segment file) and an empty string when not known /
     * available.
     */
    public final String file;

    /**
     * Ctor when there is no index resource for the event (the service on which
     * the event is generated is always supplied).
     */
    public EventResource() {

        this.indexName = BLANK;

        this.partitionId = BLANK;

        this.file = BLANK;

    }

    public EventResource(String indexName) {

        if (indexName == null)
            throw new IllegalArgumentException();

        this.indexName = indexName;

        this.partitionId = BLANK;

        this.file = BLANK;

    }

    /**
     * Forgiving ctor that does the right thing when the {@link IndexMetadata}
     * object exists.
     * 
     * @param md
     */
    public EventResource(IndexMetadata md) {

        if (md == null) {

            this.indexName = BLANK;

            this.partitionId = BLANK;

            this.file = BLANK;

        } else {

            this.indexName = md.getName();

            final LocalPartitionMetadata pmd = md.getPartitionMetadata();

            this.partitionId = pmd == null ? BLANK : Integer.toString(pmd
                    .getPartitionId());

            this.file = BLANK;

        }

    }

    public EventResource(String indexName, int partitionId) {

        if (indexName == null)
            throw new IllegalArgumentException();

        this.indexName = indexName;

        this.partitionId = BLANK;

        this.file = BLANK;

    }

    /**
     * Forgiving ctor does the right thing if the {@link IndexMetadata} exists.
     * 
     * @param md
     *            Optional.
     * @param file
     *            Required.
     */
    public EventResource(IndexMetadata md, File file) {

        if (file == null)
            throw new IllegalArgumentException();

        if (md == null) {

            this.indexName = BLANK;
            
            this.partitionId = BLANK;
            
        } else {

            this.indexName = md.getName();

            final LocalPartitionMetadata pmd = md.getPartitionMetadata();

            this.partitionId = pmd == null ? BLANK : Integer.toString(pmd
                    .getPartitionId());

        }
        
        this.file = file.toString();
        
    }
    
    public EventResource(String indexName, int partitionId, File file) {

        if (indexName == null)
            throw new IllegalArgumentException();

        if (file == null)
            throw new IllegalArgumentException();

        this.indexName = indexName;

        this.partitionId = Integer.toString(partitionId);

        this.file = file.toString();

    }
    
    public EventResource(String indexName, String partitionId, String file) {
        
        this.indexName = indexName;
        
        this.partitionId = partitionId;
        
        this.file = file;
        
    }

    private static transient final String BLANK = "";
    
}
