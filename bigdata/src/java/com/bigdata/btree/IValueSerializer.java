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
 * Created on Nov 20, 2006
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IAddressManager;

/**
 * (De-)serialize the values in a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IValueSerializer extends Serializable {

    /**
     * De-serialize the values.
     * 
     * @param is
     *            The input stream.
     * @param values
     *            The array into which the values must be written.
     * @param nvals
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be read from the buffer and
     *            written on the array.
     */
    public void getValues(DataInput is, Object[] values, int nvals)
            throws IOException;

    /**
     * Serialize the values.
     * 
     * @param os
     *            The output stream.
     * @param values
     *            The array of values from the {@link Leaf}.
     * @param nvals
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be written.
     */
    public void putValues(DataOutputBuffer os, Object[] values, int nvals)
            throws IOException;

}
