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
package com.bigdata.rdf.sail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.journal.IJournal;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.rdf.lexicon.IValueCentricTextIndexer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * 
 * 
 * 
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-1893" >.
 * 
 */
public class TestTicket1893 extends
	ProxyBigdataSailTestCase {
    
     private final static String DATA = "<http://s> <http://p> 1 .\n" + 
     		"<http://s> <http://p> \"2\"^^xsd:int .\n" + 
     		"<http://s> <http://p> 3.0 .\n" + 
     		"<http://s> <http://p> \"4.0\"^^xsd:double .\n" + 
     		"<http://s> <http://p> true .\n" + 
     		"<http://s> <http://p> \"false\"^^xsd:boolean .\n" + 
     		"<http://s> <http://p> \"plain string\" .\n" + 
     		"<http://s> <http://p> \"very long literal which length exceeds MAX_INLINE_TEXT_LENGTH\" .\n" +
     		"<http://s> <http://p> \"datatyped string\"^^xsd:string .\n" + 
     		"<http://s> <http://p> \"english string\"@en .\n" + 
     		"_:s1 <http://refers> _:s2 .\n"; 
    
    private final static RDFFormat DATA_FORMAT = RDFFormat.TURTLE;
    
    private final static String INSERT_SPARQL = "INSERT DATA {" //
    		+ DATA //
    		+ "}";

    public TestTicket1893() {

    }

    public TestTicket1893(final String name) {

        super(name);
        
    }
    /**
     * case 1:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is true
     * data loaded from file
     */
    public void test_1() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 2:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * TEXT_INDEX_DATATYPE_LITERALS is true
     * data entered via SPARQL UPDATE
     */
    public void test_2() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 3:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is true
     * data loaded from file
     */
    public void test_3() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(1, searchEngine.count(query("1")));
        assertEquals(1, searchEngine.count(query("2")));
        assertEquals(1, searchEngine.count(query("3.0")));
        assertEquals(1, searchEngine.count(query("4.0")));
        assertEquals(1, searchEngine.count(query("true")));
        assertEquals(1, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 4:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * TEXT_INDEX_DATATYPE_LITERALS is true
     * data entered via SPARQL UPDATE
     */
    public void test_4() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(1, searchEngine.count(query("1")));
        assertEquals(1, searchEngine.count(query("2")));
        assertEquals(1, searchEngine.count(query("3.0")));
        assertEquals(1, searchEngine.count(query("4.0")));
        assertEquals(1, searchEngine.count(query("true")));
        assertEquals(1, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 5:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is true
     * data loaded from file
     */
    public void test_5() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3.0")));
        assertEquals(0, searchEngine.count(query("4.0")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 6:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * TEXT_INDEX_DATATYPE_LITERALS is true
     * data entered via SPARQL UPDATE
     */
    public void test_6() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3.0")));
        assertEquals(0, searchEngine.count(query("4.0")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 7:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is true
     * data loaded from file
     */
    public void test_7() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 8:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * TEXT_INDEX_DATATYPE_LITERALS is true
     * data entered via SPARQL UPDATE
     */
    public void test_8() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/ , true /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }

    /**
     * case 9:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is false
     * data loaded from file
     */
    public void test_9() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 10:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * TEXT_INDEX_DATATYPE_LITERALS is false
     * data entered via SPARQL UPDATE
     */
    public void test_10() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 11:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is false
     * data loaded from file
     */
    public void test_11() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        {
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
        }
    }
    
	/**
     * case 12:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * TEXT_INDEX_DATATYPE_LITERALS is false
     * data entered via SPARQL UPDATE
     */
    public void test_12() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 13:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is false
     * data loaded from file
     */
    public void test_13() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 14:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * TEXT_INDEX_DATATYPE_LITERALS is false
     * data entered via SPARQL UPDATE
     */
    public void test_14() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    /**
     * case 15:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * {@link Options#TEXT_INDEX_DATATYPE_LITERALS} is false
     * data loaded from file
     */
    public void test_15() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        loadData(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
	/**
     * case 16:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * TEXT_INDEX_DATATYPE_LITERALS is false
     * data entered via SPARQL UPDATE
     */
    public void test_16() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/ , false /*textIndexDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        IValueCentricTextIndexer<?> searchEngine = cxn.getTripleStore().getLexiconRelation().getSearchEngine();
        assertEquals(0, searchEngine.count(query("1")));
        assertEquals(0, searchEngine.count(query("2")));
        assertEquals(0, searchEngine.count(query("3")));
        assertEquals(0, searchEngine.count(query("4")));
        assertEquals(0, searchEngine.count(query("true")));
        assertEquals(0, searchEngine.count(query("false")));
        assertEquals(1, searchEngine.count(query("plain")));
        assertEquals(1, searchEngine.count(query("datatyped")));
        assertEquals(1, searchEngine.count(query("english")));
        
        endTest(cxn);
        
    }
    
    //////////////////////
    
    /**
     * case 17:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * data loaded from file
     */
    public void test_17() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/);
        
        loadData(cxn);
        
        final BigdataValueFactory vf = cxn.getValueFactory();
        final BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertTrue(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertTrue(values[7].getIV().isInline()); //    	"plain string"
        assertTrue(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertTrue(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
	/**
     * case 18:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * data entered via SPARQL UPDATE
     */
    public void test_18() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertTrue(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertTrue(values[7].getIV().isInline()); //    	"plain string"
        assertTrue(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertTrue(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
    /**
     * case 19:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * data loaded from file
     */
    public void test_19() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/);
        
        loadData(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertFalse(values[0].getIV().isInline()); //    	http://s
        assertFalse(values[1].getIV().isInline()); //    	1
        assertFalse(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertFalse(values[3].getIV().isInline()); //    	3.0
        assertFalse(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertFalse(values[5].getIV().isInline()); //    	true
        assertFalse(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertFalse(values[7].getIV().isInline()); //    	"plain string"
        assertFalse(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertFalse(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
	/**
     * case 20:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * data entered via SPARQL UPDATE
     */
    public void test_20() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
				vf.createBNode("_:s1"),
				vf.createBNode("_:s2"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertFalse(values[0].getIV().isInline()); //    	http://s
        assertFalse(values[1].getIV().isInline()); //    	1
        assertFalse(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertFalse(values[3].getIV().isInline()); //    	3.0
        assertFalse(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertFalse(values[5].getIV().isInline()); //    	true
        assertFalse(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertFalse(values[7].getIV().isInline()); //    	"plain string"
        assertFalse(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertFalse(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
    /**
     * case 21:
     * {@link Options#INLINE_TEXT_LITERALS} is true 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is false 
     * data loaded from file
     */
    public void test_21() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/);
        
        loadData(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
				vf.createLiteral("very long literal which length exceeds MAX_INLINE_TEXT_LENGTH"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
		// Note, that all literals got inlined according to code of:
		// com.bigdata.rdf.internal.LexiconConfiguration.createInlineLiteralIV(Literal)
		// as INLINE_TEXT_LITERALS is true and MAX_INLINE_TEXT_LENGTH != 0
        assertTrue(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertTrue(values[7].getIV().isInline()); //    	"plain string"
        assertTrue(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertTrue(values[9].getIV().isInline()); //    	"english string"@en
        assertFalse(values[10].getIV().isInline()); //    	"very long literal which length exceeds max inline text length"
        
        endTest(cxn);
        
    }
    
	/**
     * case 22:
     * INLINE_TEXT_LITERALS is true 
     * INLINE_XSD_DATATYPE_LITERALS is false 
     * data entered via SPARQL UPDATE
     */
    public void test_22() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, true /*inlineTextLiterals*/ , 
        		false /*inlineXSDDatatypeLiterals*/);
        
        insertSparql(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
				vf.createLiteral("very long literal which length exceeds MAX_INLINE_TEXT_LENGTH"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
		// Note, that all literals got inlined according to code of:
		// com.bigdata.rdf.internal.LexiconConfiguration.createInlineLiteralIV(Literal)
		// as INLINE_TEXT_LITERALS is true and MAX_INLINE_TEXT_LENGTH != 0
        assertTrue(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertTrue(values[7].getIV().isInline()); //    	"plain string"
        assertTrue(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertTrue(values[9].getIV().isInline()); //    	"english string"@en
        assertFalse(values[10].getIV().isInline()); //    	"very long literal which length exceeds max inline text length"
        
        endTest(cxn);
        
    }
    
    /**
     * case 23:
     * {@link Options#INLINE_TEXT_LITERALS} is false 
     * {@link Options#INLINE_XSD_DATATYPE_LITERALS} is true 
     * data loaded from file
     */
    public void test_23() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
            true /*inlineXSDDatatypeLiterals*/ );
        
        loadData(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertFalse(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertFalse(values[7].getIV().isInline()); //    	"plain string"
        assertFalse(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertFalse(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
	/**
     * case 24:
     * INLINE_TEXT_LITERALS is false 
     * INLINE_XSD_DATATYPE_LITERALS is true 
     * data entered via SPARQL UPDATE
     */
    public void test_24() throws Exception {
        
        final String namespace = "test" + UUID.randomUUID();
        
        final BigdataSailRepositoryConnection cxn = prepareTest(namespace, false /*inlineTextLiterals*/ , 
                true /*inlineXSDDatatypeLiterals*/ );
        
        insertSparql(cxn);
        
        BigdataValueFactory vf = cxn.getValueFactory();
		BigdataValue[] values = new BigdataValue[]{
				vf.createURI("http://s"),
				vf.createLiteral("1", XMLSchema.INTEGER),
				vf.createLiteral(2),
				vf.createLiteral("3.0", XMLSchema.DECIMAL),
				vf.createLiteral(4.0),
				vf.createLiteral(true),
				vf.createLiteral(false),
				vf.createLiteral("plain string"),
				vf.createLiteral("datatyped string", XMLSchema.STRING),
				vf.createLiteral("english string", "en"),
		};

		cxn.getTripleStore().getLexiconRelation().addTerms(values, values.length, true /* readOnly */);
        
        assertFalse(values[0].getIV().isInline()); //    	http://s
        assertTrue(values[1].getIV().isInline()); //    	1
        assertTrue(values[2].getIV().isInline()); //    	"2"^^xsd:int
        assertTrue(values[3].getIV().isInline()); //    	3.0
        assertTrue(values[4].getIV().isInline()); //    	"4.0"^^xsd:double
        assertTrue(values[5].getIV().isInline()); //    	true
        assertTrue(values[6].getIV().isInline()); //    	"false"^^xsd:boolean
        assertFalse(values[7].getIV().isInline()); //    	"plain string"
        assertFalse(values[8].getIV().isInline()); //    	"datatyped string"^^xsd:string
        assertFalse(values[9].getIV().isInline()); //    	"english string"@en
        
        endTest(cxn);
        
    }
    
    
    

    
    private BigdataSailRepositoryConnection prepareTest(final String namespace, final boolean inlineTextLiterals, 
            final boolean inlineXSDDatatypeLiterals) throws Exception {
        return prepareTest(namespace, inlineTextLiterals, 
                inlineXSDDatatypeLiterals, false /* textIndexDatatypeLiterals */);
    }
    

    private BigdataSailRepositoryConnection prepareTest(final String namespace, final boolean inlineTextLiterals, 
            final boolean inlineXSDDatatypeLiterals, final boolean textIndexDatatypeLiterals) throws Exception {

        final Properties properties = getProperties();
        
        {
            
            properties.setProperty(com.bigdata.rdf.sail.BigdataSail.Options.NAMESPACE, namespace);
            
            properties.setProperty("com.bigdata.namespace."+namespace+".lex."+Options.INLINE_TEXT_LITERALS, Boolean.toString(inlineTextLiterals));

            if (inlineTextLiterals) {
            	properties.setProperty("com.bigdata.namespace."+namespace+".lex."+Options.MAX_INLINE_TEXT_LENGTH, Integer.toString(45));
            }

            properties.setProperty("com.bigdata.namespace."+namespace+".lex."+Options.INLINE_XSD_DATATYPE_LITERALS, Boolean.toString(inlineXSDDatatypeLiterals));

            properties.setProperty("com.bigdata.namespace."+namespace+".lex."+Options.TEXT_INDEX_DATATYPE_LITERALS, Boolean.toString(textIndexDatatypeLiterals));

            properties.setProperty("com.bigdata.namespace."+namespace+".lex."+Options.STORE_BLANK_NODES, Boolean.toString(true));

        }

        final BigdataSail sail = getSail(properties);

        sail.initialize();
        
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        return (BigdataSailRepositoryConnection) repo.getConnection();
        
    }
    
    private void endTest(BigdataSailRepositoryConnection cxn) throws Exception {
        
    	cxn.close();
    	
    	cxn.getTripleStore().close();
    	
        IJournal store = ((LocalTripleStore)cxn.getTripleStore()).getIndexManager();
        
        store.shutdownNow();
        
        store.destroy();
        
    }
    
    private void loadData(final BigdataSailRepositoryConnection cxn) throws RepositoryException, RDFParseException, IOException {
        
    	cxn.clear();
    	
        cxn.add(new ByteArrayInputStream(DATA.getBytes()), "", DATA_FORMAT);
        
        cxn.commit();
        
    }
    
    private void insertSparql(final BigdataSailRepositoryConnection cxn) throws Exception {

        cxn.prepareUpdate(QueryLanguage.SPARQL, INSERT_SPARQL).execute();
        
    }
    
    
    private FullTextQuery query(String query) {
    	return new FullTextQuery(query, null /*languageCode*/, false /*prefixMatch*/);
    }
    
}
