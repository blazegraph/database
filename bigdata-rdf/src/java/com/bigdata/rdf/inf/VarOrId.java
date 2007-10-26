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

import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;

/**
 * A class that models either a constant identifier for an RDF {@link Value} or
 * an unbound variable.
 */
abstract public class VarOrId implements Comparable<VarOrId>{

    static protected final transient long NULL = ITripleStore.NULL;
    
    /**
     * The reserved value {@link #NULL} is used to denote variables, otherwise
     * this is the long integer assigned by {@link ITripleStore#addTerm(Value)}.
     */
    public final long id;

    abstract public boolean isVar();

    abstract public boolean isConstant();
    
    protected VarOrId(long id) {
        
        this.id = id;
        
    }

    abstract public boolean equals(VarOrId o);

    abstract public int hashCode();
    
    abstract public String toString();
    
    abstract public String toString(AbstractTripleStore db);
    
}
