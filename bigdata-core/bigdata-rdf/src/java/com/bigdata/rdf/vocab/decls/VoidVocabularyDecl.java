/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jul 23, 2012
 */

package com.bigdata.rdf.vocab.decls;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.vocab.VocabularyDecl;

/**
 * Vocabulary and namespace for VOID.
 * 
 * @see <a href="http://www.w3.org/TR/void/"> Describing Linked Datasets with
 *      the VoiD Vocabulary </a>
 * @see <a href="http://vocab.deri.ie/void/"> Vocabulary of Interlinked Datasets
 *      (VoID) </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RDFVocabularyDecl.java 4631 2011-06-06 15:06:48Z thompsonbry $
 */
public class VoidVocabularyDecl implements VocabularyDecl {

    public static final String NAMESPACE = "http://rdfs.org/ns/void#";

    // Classes.
    public static final URI //
            Dataset = new URIImpl(NAMESPACE + "Dataset"),//
            DatasetDescription = new URIImpl(NAMESPACE + "DatasetDescription"),//
            Linkset = new URIImpl(NAMESPACE + "Linkset"),//
            TechnicalFeature = new URIImpl(NAMESPACE + "TechnicalFeature")//
            ;

    // Properties.
    public static final URI //
            class_ = new URIImpl(NAMESPACE+"class"),//
            classPartition = new URIImpl(NAMESPACE+"classPartition"),//
            classes = new URIImpl(NAMESPACE+"classes"),//
            dataDump = new URIImpl(NAMESPACE+"dataDump"),//
            distinctObjects = new URIImpl(NAMESPACE+"distinctObjects"),//
            distinctSubjects = new URIImpl(NAMESPACE+"distinctSubjects"),//
            documents = new URIImpl(NAMESPACE+"documents"),//
            entities = new URIImpl(NAMESPACE+"entities"),//
            exampleResource = new URIImpl(NAMESPACE+"exampleResource"),//
            feature = new URIImpl(NAMESPACE+"feature"),//
            inDataset = new URIImpl(NAMESPACE+"inDataset"),//
            linkPredicate = new URIImpl(NAMESPACE+"linkPredicate"),//
            objectsTarget = new URIImpl(NAMESPACE+"objectsTarget"),//
            openSearchDescription = new URIImpl(NAMESPACE+"openSearchDescription"),//
            properties = new URIImpl(NAMESPACE+"properties"),//
            property = new URIImpl(NAMESPACE+"property"),//
            propertyPartition = new URIImpl(NAMESPACE+"propertyPartition"),//
            rootResource = new URIImpl(NAMESPACE+"rootResource"),//
            sparqlEndpoint = new URIImpl(NAMESPACE+"sparqlEndpoint"),//
            subjectsTarget = new URIImpl(NAMESPACE+"subjectsTarget"),//
            subset = new URIImpl(NAMESPACE+"subset"),//
            target = new URIImpl(NAMESPACE+"target"),//
            triples = new URIImpl(NAMESPACE+"triples"),//
            uriLookupEndpoint = new URIImpl(NAMESPACE+"uriLookupEndpoint"),//
            uriRegexPattern = new URIImpl(NAMESPACE+"uriRegexPattern"),//
            uriSpace = new URIImpl(NAMESPACE+"uriSpace"),//
            vocabulary = new URIImpl(NAMESPACE+"vocabulary")//
    ;

    static private final URI[] uris = new URI[]{
            new URIImpl(NAMESPACE),
            // classes
            Dataset,
            DatasetDescription,
            Linkset,
            TechnicalFeature,
            // properties
            class_, classPartition, classes, dataDump, distinctObjects,
            distinctSubjects, documents, entities, exampleResource, feature,
            inDataset, linkPredicate, objectsTarget, openSearchDescription,
            properties, property, propertyPartition, rootResource,
            sparqlEndpoint, subjectsTarget, subset, target, triples,
            uriLookupEndpoint, uriRegexPattern, uriSpace, vocabulary//
    };

    public VoidVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
