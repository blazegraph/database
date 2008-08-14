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
 * Created on Apr 16, 2008
 */

package com.bigdata.rdf.model;

import org.openrdf.model.BNode;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A blank node.
 * <p>
 * Note: When {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} is
 * enabled blank nodes in the context position of a statement are recognized as
 * statement identifiers by {@link StatementBuffer}. It coordinates with this
 * class in order to detect when a blank node is a statement identifier and to
 * defer the assertion of statements made using a statement identifier until
 * that statement identifier becomes defined by being paired with a statement.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataBNodeImpl extends BigdataResourceImpl implements
        BigdataBNode {

    /**
     * 
     */
    private static final long serialVersionUID = 2675602437833048872L;
    
    private final String id;

    /**
     * Boolean flag is set during conversion from an RDF interchange syntax
     * into the internal {@link SPO} model if the blank node is a statement
     * identifier.
     */
    public boolean statementIdentifier;

    public BigdataBNodeImpl(String id) {

        this(null, id);

    }

    /**
     * Used by {@link BigdataValueFactoryImpl}.
     */
    BigdataBNodeImpl(BigdataValueFactory valueFactory, String id) {

        super(valueFactory, NULL);

        if (id == null)
            throw new IllegalArgumentException();

        this.id = id;

    }

    public String toString() {
        
        return "_:"+id;
        
    }
    
    public String stringValue() {

        return id;

    }

    final public boolean equals(Object o) {

        if (!(o instanceof BNode))
            return false;
        
        return equals((BNode) o);

    }

    final public boolean equals(BNode o) {

        if (this == o)
            return true;

        if (o == null)
            return false;
        
        return id.equals(o.getID());

    }

    final public int hashCode() {

        return id.hashCode();

    }

    final public String getID() {

        return id;
        
    }

}
