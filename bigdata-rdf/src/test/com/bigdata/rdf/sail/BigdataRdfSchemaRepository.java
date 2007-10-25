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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Map;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.sesame.sail.LiteralIterator;
import org.openrdf.sesame.sail.NamespaceIterator;
import org.openrdf.sesame.sail.RdfSchemaRepository;
import org.openrdf.sesame.sail.SailChangedListener;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailUpdateException;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sail.query.Query;

/**
 * FIXME This is a stub - nothing has been implemented.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRdfSchemaRepository implements RdfSchemaRepository {

    /**
     * 
     */
    public BigdataRdfSchemaRepository() {
        super();
        // TODO Auto-generated constructor stub
    }

    public StatementIterator getExplicitStatements(Resource arg0, URI arg1,
            Value arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean hasExplicitStatement(Resource arg0, URI arg1, Value arg2) {
        // TODO Auto-generated method stub
        return false;
    }

    public StatementIterator getClasses() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isClass(Resource arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    public StatementIterator getProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isProperty(Resource arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    public StatementIterator getSubClassOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getDirectSubClassOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isSubClassOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isDirectSubClassOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public StatementIterator getSubPropertyOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getDirectSubPropertyOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isSubPropertyOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isDirectSubPropertyOf(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public StatementIterator getDomain(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getRange(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getType(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getDirectType(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isType(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isDirectType(Resource arg0, Resource arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    public LiteralIterator getLiterals(String arg0, String arg1, URI arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    public ValueFactory getValueFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    public StatementIterator getStatements(Resource arg0, URI arg1, Value arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean hasStatement(Resource arg0, URI arg1, Value arg2) {
        // TODO Auto-generated method stub
        return false;
    }

    public Query optimizeQuery(Query arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    public NamespaceIterator getNamespaces() {
        // TODO Auto-generated method stub
        return null;
    }

    public void initialize(Map arg0) throws SailInitializationException {
        // TODO Auto-generated method stub

    }

    public void shutDown() {
        // TODO Auto-generated method stub

    }

    public void startTransaction() {
        // TODO Auto-generated method stub

    }

    public void commitTransaction() {
        // TODO Auto-generated method stub

    }

    public boolean transactionStarted() {
        // TODO Auto-generated method stub
        return false;
    }

    public void addStatement(Resource arg0, URI arg1, Value arg2)
            throws SailUpdateException {
        // TODO Auto-generated method stub

    }

    public int removeStatements(Resource arg0, URI arg1, Value arg2)
            throws SailUpdateException {
        // TODO Auto-generated method stub
        return 0;
    }

    public void clearRepository() throws SailUpdateException {
        // TODO Auto-generated method stub

    }

    public void changeNamespacePrefix(String arg0, String arg1)
            throws SailUpdateException {
        // TODO Auto-generated method stub

    }

    public void addListener(SailChangedListener arg0) {
        // TODO Auto-generated method stub

    }

    public void removeListener(SailChangedListener arg0) {
        // TODO Auto-generated method stub

    }

}
