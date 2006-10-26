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
 * Created on Oct 9, 2006
 */

package com.bigdata.istore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for operations on a store file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public interface IStore {

    /**
     * Insert an object into the store.
     * 
     * @param obj The object (required).
     * 
     * @return The int64 persistent identifier.
     */

    public long insert(Object obj);

    /**
     * Read an object from the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @return The object.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved.
     */
    public Object read(long id);

    /**
     * Update an object on the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @param obj
     *            The new object.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved.
     */
    public void update(long id, Object obj);

    /**
     * Delete an object in the store.
     * 
     * @param id
     *            The int64 persistent identifier.
     * 
     * @throws NotFoundException
     *             if the identifier can not be resolved, including if the data
     *             has already been deleted.
     */
    public void delete(long id);

//    /**
//     * Write a stream onto the store.
//     * 
//     * @return A stream on which the data can be written.
//     */
//    public StoreOutputStream getOutputStream();
//
//    /**
//     * Read data from the store.
//     * 
//     * @param id
//     *            The persistent identifier.
//     * 
//     * @return An input stream from which you can read the data.
//     * 
//     * @throws NotFoundException
//     *             if the identifier can not be resolved.
//     */
//    public InputStream getInputStream(long id);
//
//    /**
//     * Update data in the store.
//     * 
//     * @param id
//     *            The persistent identifier.
//     *            
//     * @return A stream on which the new data can be written.
//     */
//    public StoreOutputStream getUpdateStream(long id);
//
//    /**
//     * <p>
//     * An output stream used to insert or update data in the store. The caller
//     * writes on the stream. When the stream is closed, the data is saved to the
//     * store. Internally, data is incrementally written against the store so
//     * that this mechanism may be used to write very large objects. The data is
//     * saved by invoking {@link #close()}.
//     * </p>
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * 
//     * @todo Use factory to obtain pre-existing instances where possible.
//     * 
//     * @todo Support blobs transparently by writing on this stream.
//     * 
//     * @todo Consider protocol for aborting an operation. Certainly the
//     *       application can just thrown an exception forcing a transaction to
//     *       rollback.
//     */
//    
//    public abstract static class StoreOutputStream extends OutputStream {
//
//        /**
//         * The persistent identifier for the data. This is assigned by the store
//         * when inserting new data and provided by the application when updating
//         * existing data.
//         * 
//         * @return The persistent identifier.
//         */
//        abstract public long getId();
//
//    }

}
