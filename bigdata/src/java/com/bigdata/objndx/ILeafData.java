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
 * Created on Dec 15, 2006
 */

package com.bigdata.objndx;

/**
 * Interface for low-level data access for the leaves of a B+-Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILeafData extends IAbstractNodeData {

    /**
     * The #of values in the leaf (this MUST be equal to
     * {@link IAbstractNodeData#getKeyCount()}.
     * 
     * @return The #of values in the leaf.
     */
    public int getValueCount();

    /**
     * The backing array in which the values are stored. Only the first
     * {@link #getValueCount()} entries in the array are defined. A
     * <code>null</code> value for a define entry is used to indicate that the
     * entry has been deleted. Non-deleted values MUST be non-null.
     * 
     * The use of this array is dangerous since mutations are directly reflected
     * in the leaf, but it may be highly efficient. Callers MUST excercise are
     * to perform only read-only operations against the returned array.
     * 
     * @return The backing array in which the values are stored.
     */
    public Object[] getValues();

//    /**
//     * The backing arra in which the timestamps are stored. The semantics of the
//     * timestamps are defined by the concurrency control algorithm. Some common
//     * examples are treated the timestamp as the moment when the entry was
//     * updated, treating the timestamp as the time at which the measurement
//     * represented by the value was taken, etc.
//     * 
//     * When used to support transactional isolation the following rules must be
//     * followed: if the key did not exist in the global scope, then the
//     * timestamp is set to zero in the isolated btree; if the key did exist and
//     * is being overwritten, then copy the timestamp from the global scope into
//     * the entry in the isolated tree;
//     * 
//     * during validation, we test the timestamp in the global scope. remember,
//     * if the isolated timestamp is zero then the key did not exist in the
//     * global state when this transaction was started and we simply verify that
//     * there is still no entry in the global scope. if the isolated timestamp is
//     * non-zero then there was an entry in the global scope. we then test the
//     * global scope - if the timestamp is the same then no intervening
//     * concurrent transaction has written this key and commited, hence, there is
//     * no write conflict for the key. if the timestamp is different (it must be
//     * greater when it is different) then a write-write conflict exists (it may
//     * be possible to validate that conflict using a state-based technique). if
//     * there is no conflict or if the conflict can be validated then set the
//     * timestamp in the isolated tree to the commit timestamp of the transaction
//     * (which must be distinct and greater than from the commit time of all
//     * previous committed transactions); when merging down into the global state
//     * the timestamp is simply copied.
//     * 
//     * @todo modify the api to pass in timestamps along with keys and values.
//     * 
//     * @todo modify the api to pass in arrays to be filled by values and
//     *       timestamps on lookup.
//     * 
//     * @todo modify the api to support stride at the same time.
//     * 
//     * @todo support (de-)serialization
//     * 
//     * @todo support in node factory.
//     * 
//     * @todo provide api variants that fill in timestamps based on the current
//     *       time.
//     * 
//     * @todo locate this javadoc in a useful place.
//     * 
//     * @return The timestamps.
//     */
//    public long[] getTimestamps();
    
}
