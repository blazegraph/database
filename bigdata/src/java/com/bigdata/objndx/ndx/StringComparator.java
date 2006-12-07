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
 * Created on May 9, 2006
 */
package com.bigdata.objndx.ndx;

import java.io.Serializable;
import java.util.Comparator;

import org.CognitiveWeb.extser.Stateless;

/**
 * Comparator for {@link String} values using {@link java.lang.String#compareTo(java.lang.String)}.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public final class StringComparator implements Comparator, Serializable, Stateless {

    private static final long serialVersionUID = 3162584716956701139L;

    public StringComparator() {
        super();
    }

    public int compare(Object o1, Object o2) {
        if (o1 == null ) {
            throw new IllegalArgumentException();
        }
        if (o2 == null) {
            throw new IllegalArgumentException();
        }
        return ((String) o1).compareTo((String) o2);
    }

    public boolean equals( Object o ) {
        return this == o || o instanceof StringComparator;
    }

}
