package com.bigdata.gom;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;
import junit.framework.TestCase;

public class ProxyGOMTest extends TestCase implements IProxyTest {

	Test m_delegate;
	protected ValueFactory m_vf;
	protected IObjectManager om;

	public ProxyGOMTest() {
		
	}
	
	public ProxyGOMTest(String testName) {
		super(testName);
	}

	@Override
	public void setDelegate(Test delegate) {
		assert delegate instanceof IGOMProxy;

		m_delegate = delegate;

	}
	
   protected void setUp() throws Exception {

	   ((IGOMProxy) m_delegate).proxySetup();
	   
		om = ((IGOMProxy) m_delegate).getObjectManager();
		m_vf = ((IGOMProxy) m_delegate).getValueFactory();
    }
    
    protected void tearDown() throws Exception {
    	((IGOMProxy) m_delegate).proxyTearDown();
    }

	@Override
	public Test getDelegate() {
		return m_delegate;
	}

    protected void showClassHierarchy(final Iterator<IGPO> classes,
            final int indent) {
        StringBuilder out = new StringBuilder();
        showClassHierarchy(out, classes, indent);
        System.out.println("Hierarchy: " + out.toString());
    }

    private void showClassHierarchy(StringBuilder out, Iterator<IGPO> classes,
            int indent) {
        while (classes.hasNext()) {
            final IGPO clss = classes.next();
            out.append(indentOut(clss, indent + 1));
            showClassHierarchy(out,
                    clss.getLinksIn(RDFS.SUBCLASSOF).iterator(), indent + 1);
        }
    }

    String indents = "\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";

    private Object indentOut(IGPO clss, int indent) {
        Value lbl = clss.getValue(RDFS.LABEL);
        final String display = lbl == null ? clss.getId().stringValue() : lbl
                .stringValue();
        return indents.substring(0, indent) + display;
    }

    protected void showOntology(IGPO onto) {
        System.out.println("Ontology: " + onto.pp());
        Iterator<IGPO> parts = onto.getLinksIn().iterator();
        while (parts.hasNext()) {
            IGPO part = parts.next();
            System.out.println("Onto Part: " + part.pp());
        }
    }

}
