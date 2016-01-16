/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Nov 5, 2011
 */

package com.bigdata.bop;

import com.bigdata.btree.IndexMetadata;

/**
 * Annotations for operators using a persistence capable index.
 * 
 * TODO Annotations for key and value raba coders.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IndexAnnotations {

    /**
     * @see IndexMetadata.Options#WRITE_RETENTION_QUEUE_CAPACITY
     */
    String WRITE_RETENTION_QUEUE_CAPACITY = IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY;

    final int DEFAULT_WRITE_RETENTION_QUEUE_CAPACITY = 4000;

    /**
     * When <code>true</code> raw record references will be written on the
     * backing store and the index will manage the mapping between the keys and
     * the storage addresses rather than having the byte[] values inline in the
     * bucket page (default {@link #DEFAULT_RAW_RECORDS}).
     * 
     * @see IndexMetadata#getRawRecords()
     */
    String RAW_RECORDS = IndexAnnotations.class.getName() + ".rawRecords";

    boolean DEFAULT_RAW_RECORDS = false;

    /**
     * When {@link #RAW_RECORDS} are used, this will be the maximum byte length
     * of a byte[] value before it is written as a raw record on the backing
     * store rather than inlined within the bucket page (default
     * {@value #DEFAULT_MAX_RECLEN} .
     * 
     * @see IndexMetadata#getMaxRecLen()
     */
    String MAX_RECLEN = IndexAnnotations.class.getName() + ".maxRecLen";

    int DEFAULT_MAX_RECLEN = 128;
    
}
