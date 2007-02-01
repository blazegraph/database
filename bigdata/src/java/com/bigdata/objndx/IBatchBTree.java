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
 * Created on Feb 1, 2007
 */

package com.bigdata.objndx;

/**
 * <p>
 * Interface for batch operations a B+-Tree mapping variable length unsigned
 * byte[] keys to arbitrary values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME add batch api for rangeCount and rangeQuery.
 * 
 * @todo support batch api for indexOf(), keyAt(), valueAt()?
 */
public interface IBatchBTree {

    /**
     * Batch insert operation of N tuples presented in sorted order. This
     * operation can be very efficient if the tuples are presented sorted by key
     * order.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in). Additional elements of
     *            the parameter arrays will be ignored.
     * 
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.<br>
     *            The individual byte[] keys provided to this method MUST be
     *            immutable - if the content of a given byte[] in <i>keys</i>
     *            is changed after the method is invoked then the change MAY
     *            have a side-effect on the keys stored in leaves of the tree.
     *            While this constraint applies to the individual byte[] keys,
     *            the <i>keys</i> byte[][] itself may be reused from invocation
     *            to invocation without side-effect.
     * 
     * @param values
     *            Values (one element per key) (in/out). Null elements are
     *            allowed. On output, each element is either null (if there was
     *            no entry for that key) or the old value stored under that key
     *            (which may be null).
     * 
     * @param values
     *            An array of values, one per tuple (in/out).
     * 
     * @exception IllegalArgumentException
     *                if the dimensions of the arrays are not sufficient for the
     *                #of tuples declared.
     */
    public void insert(int ntuples, byte[][] keys, Object[] values);

    /**
     * Batch lookup operation for N tuples returns the most recent timestamp and
     * value for each key. This operation can be very efficient if the keys are
     * presented in sorted order.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * 
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.
     * 
     * @param values
     *            An array of values, one per tuple (out). The array element
     *            corresponding to a tuple will be null if the key does not
     *            exist -or- if the key exists with a null value.
     * 
     * @todo consider returning the #of keys that were found in the btree. this
     *       either requires passing an additional counter through the
     *       implementation of defining the value as always being non-null
     *       (which is too restrictive).
     */
    public void lookup(int ntuples, byte[][] keys, Object[] values);

    /**
     * Batch lookup operation for N tuples returns the most recent timestamp and
     * value for each key. This operation can be very efficient if the keys are
     * presented in sorted order.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * 
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.
     * 
     * @param contains
     *            An array of boolean flags, one per tuple (in,out). On input,
     *            the tuple will be tested iff the corresponding element is
     *            <code>false</code> (this supports chaining of this operation
     *            on a view over multiple btrees). On output, the array element
     *            corresponding to a tuple will be true iff the key exists.
     * 
     * @todo consider returning the #of keys that were found in the btree.
     */
    public void contains(int ntuples, byte[][] keys, boolean[] contains);

    /**
     * Batch remove of N tuples. This operation can be very efficient if the
     * keys are presented in sorted order.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * 
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.
     * 
     * @param values
     *            An array of values, one per tuple (out). The array element
     *            corresponding to a tuple will be null if the key did not exist
     *            -or- if the key existed with a null value (null values are
     *            used to mark deleted keys in an isolated btree).
     */
    public void remove(int ntuples, byte[][] keys, Object[] values);

}
