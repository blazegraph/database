/**
Copyright (C) SYSTAP, LLC 2006-Infinity.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.InlineURIFactory;
import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.InlineUUIDURIHandler;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.IPv4AddrIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.BaseVocabularyDecl;
import com.bigdata.rdf.vocab.RDFSVocabulary;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestInlineURIs extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestInlineURIs.class);

    /**
     * Please set your database properties here, except for your journal file,
     * please DO NOT SPECIFY A JOURNAL FILE. 
     */
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        /*
         * Turn off inference.
         */
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    public TestInlineURIs() {
    }

    public TestInlineURIs(String arg0) {
        super(arg0);
    }
    
    public void testInlineUUIDs() throws Exception {
    	
        /*
         * The bigdata store, backed by a temporary journal file.
         */
	  	final BigdataSail sail = getSail();
	  	
	  	try {
	  	
	  		sail.initialize();
	  		
  			final BigdataSailRepository repo = new BigdataSailRepository(sail);  			
  			
  			final BigdataSailRepositoryConnection cxn = repo.getConnection();
  			cxn.setAutoCommit(false);
  			
  			try {
  			    
        		final BigdataValueFactory vf = cxn.getValueFactory();
        
                final URI uri1 = vf.createURI("urn:uuid:"+UUID.randomUUID().toString());
                final URI uri2 = vf.createURI("urn:uuid:"+UUID.randomUUID().toString());
                final URI uri3 = vf.createURI("urn:uuid:foo");
        
                cxn.add(uri1, RDF.TYPE, XSD.UUID);
                cxn.add(uri2, RDF.TYPE, XSD.UUID);
                cxn.add(uri3, RDF.TYPE, XSD.UUID);
      			cxn.commit();
      			
      			if (log.isDebugEnabled())
      			    log.debug(cxn.getTripleStore().dumpStore());
      			
      			final TupleQuery query = cxn.prepareTupleQuery(
      			        QueryLanguage.SPARQL, "select * { ?s ?p ?o }");
      			
      			final TupleQueryResult result = query.evaluate();
      			
      			while (result.hasNext()) {
      			    
      			    final BigdataURI uri = (BigdataURI) 
      			            result.next().getBinding("s").getValue();
      			    
      			    if (uri.equals(uri1)) {
      			        assertTrue(uri.getIV().isInline());
      			    } else if (uri.equals(uri2)) {
                        assertTrue(uri.getIV().isInline());
                    } else if (uri.equals(uri3)) {
                        assertFalse(uri.getIV().isInline());
                    }  
      			    
      			}

  			} finally {
  			    cxn.close();
  			}

        } finally {
            sail.__tearDownUnitTest();
        }
    	
    }

    public void testInlineIPv4s() throws Exception {
        
        /*
         * The bigdata store, backed by a temporary journal file.
         */
        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            final BigdataSailRepository repo = new BigdataSailRepository(sail);             
            
            final BigdataSailRepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            
            try {
                
                final BigdataValueFactory vf = cxn.getValueFactory();
    
                final URI uri1 = vf.createURI("urn:ipv4:10.128.1.2");
                final URI uri2 = vf.createURI("urn:ipv4:10.128.1.2/24");
                final URI uri3 = vf.createURI("urn:ipv4:500.425.1.2");
                final URI uri4 = vf.createURI("urn:ipv4");
    
                final Literal l = vf.createLiteral("10.128.1.2", XSD.IPV4);
                
                cxn.add(uri1, RDF.TYPE, XSD.IPV4);
                cxn.add(uri2, RDF.TYPE, XSD.IPV4);
                cxn.add(uri3, RDF.TYPE, XSD.IPV4);
                cxn.add(uri4, RDFS.LABEL, l);
                cxn.commit();
                
                if (log.isDebugEnabled())
                    log.debug(cxn.getTripleStore().dumpStore());
                
                final TupleQuery query = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, "select * { ?s ?p ?o }");
                
                final TupleQueryResult result = query.evaluate();
                
                while (result.hasNext()) {
                    
                    final BigdataURI uri = (BigdataURI) 
                            result.next().getBinding("s").getValue();
                    
                    if (uri.equals(uri1)) {
                        assertTrue(uri.getIV().isInline());
                    } else if (uri.equals(uri2)) {
                        assertTrue(uri.getIV().isInline());
                    } else if (uri.equals(uri3)) {
                        assertFalse(uri.getIV().isInline());
                    } else if (uri.equals(uri4)) {
                        assertFalse(uri.getIV().isInline());
                    }  
                    
                }

            } finally {
                cxn.close();
            }

        } finally {
            sail.__tearDownUnitTest();
        }
        
    }
    
    public void testCustomUUIDNamespace() throws Exception {
        
        final Properties props = getProperties();
        
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, 
                CustomVocab.class.getName());
        props.setProperty(AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS, 
                CustomInlineURIFactory.class.getName());
        
        
        /*
         * The bigdata store, backed by a temporary journal file.
         */
        final BigdataSail sail = getSail(props);
        
        try {
        
            sail.initialize();
            
            final BigdataSailRepository repo = new BigdataSailRepository(sail);             
            
            final BigdataSailRepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            
            try {

                final BigdataValueFactory vf = cxn.getValueFactory();

                final URI uri1 = vf.createURI(CUSTOM_NAMESPACE + UUID.randomUUID().toString());
                final URI uri2 = vf.createURI(CUSTOM_NAMESPACE + UUID.randomUUID().toString());
                final URI uri3 = vf.createURI(CUSTOM_NAMESPACE + "foo");
    
                cxn.add(uri1, RDF.TYPE, XSD.UUID);
                cxn.add(uri2, RDF.TYPE, XSD.UUID);
                cxn.add(uri3, RDF.TYPE, XSD.UUID);
                cxn.commit();
                
                if (log.isDebugEnabled())
                    log.debug(cxn.getTripleStore().dumpStore());
            
                final TupleQuery query = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, "select * { ?s ?p ?o }");
                
                final TupleQueryResult result = query.evaluate();
                
                while (result.hasNext()) {
                    
                    final BigdataURI uri = (BigdataURI) 
                            result.next().getBinding("s").getValue();
                    
                    if (uri.equals(uri1)) {
                        assertTrue(uri.getIV().isInline());
                    } else if (uri.equals(uri2)) {
                        assertTrue(uri.getIV().isInline());
                    } else if (uri.equals(uri3)) {
                        assertFalse(uri.getIV().isInline());
                    }  
                    
                }

            } finally {
                cxn.close();
            }
    
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }

    public void testMultipurposeIDNamespace() throws Exception {
        
        final Properties props = getProperties();
        
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, 
                CustomVocab.class.getName());
        props.setProperty(AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS, 
                MultipurposeInlineIDFactory.class.getName());
        
        
        /*
         * The bigdata store, backed by a temporary journal file.
         */
        final BigdataSail sail = getSail(props);
        
        try {
        
            sail.initialize();
            
            final BigdataSailRepository repo = new BigdataSailRepository(sail);             
            
            final BigdataSailRepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            
            try {

                final BigdataValueFactory vf = cxn.getValueFactory();

                final URI uri1 = vf.createURI(CUSTOM_NAMESPACE + UUID.randomUUID().toString());
                final URI uri2 = vf.createURI(CUSTOM_NAMESPACE + "1");
                final URI uri3 = vf.createURI(CUSTOM_NAMESPACE + Short.MAX_VALUE);
                final URI uri4 = vf.createURI(CUSTOM_NAMESPACE + Integer.MAX_VALUE);
                final URI uri5 = vf.createURI(CUSTOM_NAMESPACE + Long.MAX_VALUE);
                final URI uri6 = vf.createURI(CUSTOM_NAMESPACE + "2.3");
                final URI uri7 = vf.createURI(CUSTOM_NAMESPACE + "foo");
    
                cxn.add(uri1, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri2, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri3, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri4, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri5, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri6, RDF.TYPE, RDFS.RESOURCE);
                cxn.add(uri7, RDF.TYPE, RDFS.RESOURCE);
                cxn.commit();
                
                if (log.isDebugEnabled())
                    log.debug(cxn.getTripleStore().dumpStore());
            
                final TupleQuery query = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, "select * { ?s ?p ?o }");
                
                final TupleQueryResult result = query.evaluate();
                
                while (result.hasNext()) {
                    
                    final BigdataURI uri = (BigdataURI) 
                            result.next().getBinding("s").getValue();
                    
                    assertTrue(uri.getIV().isInline());
                    
                    if (uri.equals(uri1)) {
                        assertTrue(uri.getIV().getDTE() == DTE.UUID);
                    } else if (uri.equals(uri2)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDByte);
                    } else if (uri.equals(uri3)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDShort);
                    } else if (uri.equals(uri4)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDInt);
                    } else if (uri.equals(uri5)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDLong);
                    } else if (uri.equals(uri6)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDDouble);
                    } else if (uri.equals(uri7)) {
                        assertTrue(uri.getIV().getDTE() == DTE.XSDString);
                    }
                    
                }
                
            } finally {
                cxn.close();
            }
    
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }

    public static final String CUSTOM_NAMESPACE = "application:id:"; 
    
    public static class CustomVocab extends RDFSVocabulary {
        
        public CustomVocab() {
            super();
        }
        
        public CustomVocab(final String namespace) {
            super(namespace);
        }
        
        @Override
        protected void addValues() {
            super.addValues();
            
            addDecl(new BaseVocabularyDecl(CUSTOM_NAMESPACE));
        }        
        
    }
    
    public static class CustomInlineURIFactory extends InlineURIFactory {
        
        public CustomInlineURIFactory() {
            super();
            addHandler(new InlineUUIDURIHandler(CUSTOM_NAMESPACE));
        }
        
        
    }

    public static class MultipurposeInlineIDFactory extends InlineURIFactory {
        
        public MultipurposeInlineIDFactory() {
            super();
            addHandler(new MultipurposeInlineIDHandler(CUSTOM_NAMESPACE));
        }
        
    }

    public static class MultipurposeInlineIDHandler extends InlineURIHandler {
        
        public MultipurposeInlineIDHandler(final String namespace) {
            super(namespace);
        }

        @Override
        protected AbstractLiteralIV createInlineIV(final String localName) {
            
            try {
                return new IPv4AddrIV(localName);
            } catch (Exception ex) {
                // ok, not an ip address
            }
            
            try {
                return new UUIDLiteralIV<>(UUID.fromString(localName));
            } catch (Exception ex) {
                // ok, not a uuid
            }
            
            try {
                return new XSDNumericIV(Byte.parseByte(localName));
            } catch (Exception ex) {
                // ok, not a byte
            }
            
            try {
                return new XSDNumericIV(Short.parseShort(localName));
            } catch (Exception ex) {
                // ok, not a short
            }
            
            try {
                return new XSDNumericIV(Integer.parseInt(localName));
            } catch (Exception ex) {
                // ok, not a int
            }
            
            try {
                return new XSDNumericIV(Long.parseLong(localName));
            } catch (Exception ex) {
                // ok, not a long
            }
            
            try {
                return new XSDNumericIV(Double.parseDouble(localName));
            } catch (Exception ex) {
                // ok, not a double
            }

            // just use a UTF encoded string, this is expensive
            return new FullyInlineTypedLiteralIV<BigdataLiteral>(localName);
            
        }
        
    }


}
