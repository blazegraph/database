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
 * Created on Dec 9, 2006
 */

package com.bigdata.objndx;


/**
 * Interface defining the legal bounds on the value of keys for an index, 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyBounds {

    /**
     * Assert that a key is within the legal bounds.
     */
    public void assertBounds();

    /**
     * The key that is used as negative infinity. When the key type is a
     * primitive data type such as <code>long</code> this MUST return
     * non-null. For non-primitive key types this MUST return <code>null</code>.
     * 
     * @todo consider renaming this to getInvalidKey() or getNullKey().  the
     * concept of the key bounds is captured by a different methods on this
     * api.
     */
    public Object getNegInf();

    /**
     * The enumeration value indicating the data type used to store keys in
     * the nodes and leaves of the tree.  When a key corresponds to a primitive
     * data type, then you generally want to use an array of that primitive data
     * type.  The only exception would be an application which needed to use the
     * full value range for the primitive data type and which was therefore forced
     * to use an Object to store each key value.
     */
    public ArrayType getArrayType();

//    /**
//     * The comparator that will be used to compare keys iff the keys are NOT
//     * primitive data types.
//     */
//    public Comparator getComparator();
    
}
