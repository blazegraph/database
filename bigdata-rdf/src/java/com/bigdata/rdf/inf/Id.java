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

import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A constant (a term identifier for the lexicon).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class Id extends VarOrId {
    
    /**
     * Label is cached iff the constant is resolved against the db.
     */
    private String label = null;
    
    final public boolean isVar() {
        
        return false;
        
    }

    final public boolean isConstant() {
        
        return true;
        
    }

    public Id(long id) {
        
        super(id);
        
        assert id>0;
        
    }
    
    public String toString() {
        
        return ""+id;
        
    }

    public String toString(AbstractTripleStore db) {

        if (label != null) {
            
            // cached.
            return label;
            
        }
        
        if (db == null) {

            return toString();
            
        } else {

            // cache label.
            label = db.toString(id);

            return label;
            
        }
        
    }
    
    final public boolean equals(VarOrId o) {
    
        if (o instanceof Id && id == ((Id) o).id) {

            return true;
            
        }
        
        return false;
        
    }
    
    final public int hashCode() {
        
        // same has function as Long.hashCode().
        
        return (int) (id ^ (id >>> 32));
        
    }
    
    public int compareTo(VarOrId arg0) {

        // order vars before ids
        if(arg0 instanceof Var) return 1;
        
        Id o = (Id)arg0;
        
        /*
         * Note: logic avoids possible overflow of [long] by not computing the
         * difference between two longs.
         */
        
        int ret = id < o.id ? -1 : id > o.id ? 1 : 0;
        
        return ret;
        
    }

}
