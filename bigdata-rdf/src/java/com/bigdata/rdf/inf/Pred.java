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
package com.bigdata.rdf.inf;

/**
 * A predicate is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we
 * are only concerned with predicates of the form <code>triple(s,p,o)</code>
 * or <code>magic(triple(s,p,o))</code>. Since this is a boolean
 * distinction, we capture it with a boolean flag rather than allowing a
 * predicate name and arity.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Pred {

    public final boolean magic;

    public final VarOrId s;

    public final VarOrId p;

    public final VarOrId o;

    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isFact() {

        return !s.isVar() && !p.isVar() && !o.isVar();

    }

    /**
     * Create a triple/3 predicate.
     * 
     * @param s
     * @param p
     * @param o
     */
    public Pred(VarOrId s, VarOrId p, VarOrId o) {

        this(false,s,p,o);
        
    }

    /**
     * Create either a magic/1 or a triple/3 predicate.
     * 
     * @param magic
     * @param s
     * @param p
     * @param o
     */
    public Pred(boolean magic, VarOrId s, VarOrId p, VarOrId o) {
        assert s != null;
        assert p != null;
        assert o != null;
        this.magic = magic;
        this.s = s;
        this.p = p;
        this.o = o;
    }

}