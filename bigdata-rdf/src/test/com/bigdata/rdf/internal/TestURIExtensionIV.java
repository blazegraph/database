package com.bigdata.rdf.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.vocab.BaseVocabulary;
import com.bigdata.rdf.vocab.VocabularyDecl;

/**
 * Test suite for {@link URIExtensionIV}.
 */
public class TestURIExtensionIV extends TestCase2 {

	public TestURIExtensionIV() {
	}

	public TestURIExtensionIV(String name) {
		super(name);
	}

	/*
	 * Must test using Vocabulary to test asValue().
	 */

	private String namespace;
	private BaseVocabulary vocab;
//	private BigdataValueFactory valueFactory;
    
    protected void setUp() throws Exception {
        
        super.setUp();
        
        namespace = getName();
        
//        valueFactory  = BigdataValueFactoryImpl.getInstance(namespace);
        
        vocab = new MockVocabulary(namespace);
        
        vocab.init();
        
    }

    protected void tearDown() throws Exception {
        
        super.tearDown();

        vocab = null;
        
        namespace = null;
        
//        if (valueFactory != null)
//            valueFactory.remove();
//        
//        valueFactory = null;

    }
    
    private static class MockVocabularyDecl implements VocabularyDecl {

        static private final URI[] uris = new URI[]{//
            new URIImpl("http://www.bigdata.com/"), //
            new URIImpl(RDFS.NAMESPACE), //
            new URIImpl("http://www.Department0.University0.edu/"),//
        };
        
        @Override
        public Iterator<URI> values() {
            return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        }
        
    }
    
    public static class MockVocabulary extends BaseVocabulary {

        /**
         * De-serialization. 
         */
        public MockVocabulary() {
            super();
        }

        /**
         * @param namespace
         */
        public MockVocabulary(String namespace) {
            super(namespace);
        }

        @Override
        protected void addValues() {
            addDecl(new MockVocabularyDecl());
//            addDecl(new LUBMVocabularyDecl());
        }
        
    }

    private URIExtensionIV<BigdataURI> newFixture(final URI uri) {

        final String namespace = uri.getNamespace();

        final URI namespaceURI = new URIImpl(namespace);
        
        final IV<?, ?> namespaceIV = vocab.get(namespaceURI);

        if (namespaceIV == null) {

            fail("Not declared by vocabulary: namespace: " + namespace);
            
        }
        
        final FullyInlineTypedLiteralIV<BigdataLiteral> localNameIV = new FullyInlineTypedLiteralIV<BigdataLiteral>(
                uri.getLocalName());

        final URIExtensionIV<BigdataURI> iv = new URIExtensionIV<BigdataURI>(
                localNameIV, namespaceIV);
        
        return iv;
        
    }
    
	public void test_InlineURIIV() {

//        doTest(new URIImpl("http://www.bigdata.com"));
        doTest(new URIImpl("http://www.bigdata.com/"));
        doTest(new URIImpl("http://www.bigdata.com/foo"));
        doTest(RDFS.CLASS);
        doTest(RDFS.SUBPROPERTYOF);
        doTest(new URIImpl("http://www.Department0.University0.edu/UndergraduateStudent488"));
        doTest(new URIImpl("http://www.Department0.University0.edu/GraduateStudent15"));
//        doTest(new URIImpl("http://www.bigdata.com:80/foo"));

	}

	private void doTest(final URI uri) {

        final URIExtensionIV<BigdataURI> iv = newFixture(uri);
        
		assertEquals(VTE.URI, iv.getVTE());
		
		assertTrue(iv.isInline());
		
		assertTrue(iv.isExtension());

		assertEquals(DTE.XSDString, iv.getDTE());
		
		final BlobsIndexHelper h = new BlobsIndexHelper();
		
		final IKeyBuilder keyBuilder = h.newKeyBuilder();
		
		final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
		
		final IV<?,?> actual = IVUtility.decode(key);
		
		assertEquals(iv, actual);
		
		assertEquals(key.length, iv.byteLength());

		assertEquals(key.length, actual.byteLength());
		
	}
	
    public void test_encodeDecode_comparator() {
        
        final List<IV<?,?>> ivs = new LinkedList<IV<?,?>>();
        {

//            ivs.add(newFixture(new URIImpl("http://www.bigdata.com")));
            ivs.add(newFixture(new URIImpl("http://www.bigdata.com/")));
            ivs.add(newFixture(new URIImpl("http://www.bigdata.com/foo")));
            ivs.add(newFixture(RDFS.CLASS));
            ivs.add(newFixture(RDFS.SUBPROPERTYOF));
            ivs.add(newFixture(new URIImpl("http://www.Department0.University0.edu/UndergraduateStudent488")));
            ivs.add(newFixture(new URIImpl("http://www.Department0.University0.edu/GraduateStudent15")));
//            ivs.add(newFixture(new URIImpl("http://www.bigdata.com:80/foo")));

        }
        
        final IV<?, ?>[] e = ivs.toArray(new IV[0]);

        AbstractEncodeDecodeKeysTestCase.doEncodeDecodeTest(e);

        AbstractEncodeDecodeKeysTestCase.doComparatorTest(e);
    
    }
    
}
