/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on September 24, 2015
 */
package com.blazegraph.vocab.pubchem;

/**
 * 
 * Use the vocabulary by adding a property to your journal file per below.
 * 
 * com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.blazegraph.vocab.pubchem.PubChemVocabulary
 * 
 */
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;
import com.bigdata.rdf.vocab.decls.DCAllVocabularyDecl;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;
import com.bigdata.rdf.vocab.decls.OWLVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFSVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFVocabularyDecl;
import com.bigdata.rdf.vocab.decls.SKOSVocabularyDecl;
import com.bigdata.rdf.vocab.decls.XMLSchemaVocabularyDecl;

/**
 * A {@link Vocabulary} covering the PubChem data from https://pubchem.ncbi.nlm.nih.gov/rdf/.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class PubChemVocabulary extends BigdataCoreVocabulary_v20151210 {

    /**
     * De-serialization ctor.
     */
    public PubChemVocabulary() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public PubChemVocabulary(final String namespace) {

        super( namespace );
        
    }

    @Override
    protected void addValues() {

        addDecl(new RDFVocabularyDecl());
        addDecl(new RDFSVocabularyDecl());
        addDecl(new FOAFVocabularyDecl());
        addDecl(new SKOSVocabularyDecl());
        addDecl(new OWLVocabularyDecl());
        addDecl(new DCAllVocabularyDecl());
        addDecl(new XMLSchemaVocabularyDecl());
        addDecl(new PubChemVocabularyDecl());

    }

}
