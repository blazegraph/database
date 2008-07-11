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
 * Created on Jul 5, 2008
 */

package com.bigdata.rdf.model;

import java.util.Comparator;

/**
 * Places {@link BigdataValue}s into an ordering determined by their assigned
 * {@link BigdataValue#getTermId() term identifiers}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueIdComparator implements Comparator<BigdataValue> {

    public static final transient Comparator<BigdataValue> INSTANCE = new BigdataValueIdComparator();

    /**
     * Note: comparison avoids possible overflow of <code>long</code> by
     * not computing the difference directly.
     */
    public int compare(BigdataValue term1, BigdataValue term2) {

        final long id1 = term1.getTermId();
        final long id2 = term2.getTermId();

        if (id1 < id2)
            return -1;
        if (id1 > id2)
            return 1;
        return 0;

    }

}
