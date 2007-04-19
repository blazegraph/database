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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf;

/**
 * Represents the key order used by an index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum KeyOrder {

    /**
     * @todo make the case of the index name and the case of the enums the same
     *       (either both upper or both lower). Enums naturally convert to
     *       strings based on their case, so SPO.toString() is "SPO" but
     *       SPO.name is "spo", which is confusing.
     */
    SPO("spo",0),
    POS("pos",1),
    OSP("osp",2);

    public final String name;
    
    public final int order;
    
    private KeyOrder(String name,int order) {
        this.name = name;
        this.order = order;
    }
    
    private static final long NULL = TripleStore.NULL;
    
    /**
     * Return the access path that should be used for the triple pattern.
     * 
     * @param s
     *            The optional subject identifier or {@link TripleStore#NULL}.
     * @param p
     *            The optional subject identifier or {@link TripleStore#NULL}.
     * @param o
     *            The optional subject identifier or {@link TripleStore#NULL}.
     * 
     * @return The KeyOrder that identifies the index to use for that triple
     *         pattern.
     */
    public static KeyOrder getKeyOrder(long s, long p, long o) {

        if (s != NULL && p != NULL && o != NULL) {

            return SPO;

        } else if (s != NULL && p != NULL) {

            return SPO;

        } else if (s != NULL && o != NULL) {

            return OSP;

        } else if (p != NULL && o != NULL) {

            return POS;

        } else if (s != NULL) {

            return SPO;

        } else if (p != NULL) {

            return POS;

        } else if (o != NULL) {

            return OSP;

        } else {

            return SPO;

        }

    }
    
}
