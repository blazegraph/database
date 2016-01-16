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
 * Created on May 7, 2014
 */
package com.bigdata.search;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

public abstract class AbstractSearchTest  extends ProxyTestCase<IIndexManager>  {
    private String namespace;  
    private IIndexManager indexManager;
    private FullTextIndex<Long> ndx;
    private Properties properties;
    
    public AbstractSearchTest() {
	}
    
    public AbstractSearchTest(String arg0) {
    	super(arg0);
	}

	void init(String ...propertyValuePairs) {
        namespace = getClass().getName()+"#"+getName(); 
        indexManager = getStore();
        properties = (Properties) getProperties().clone();
        ndx = createFullTextIndex(namespace, properties, propertyValuePairs);
    }
	
	private FullTextIndex<Long> createFullTextIndex(String namespace, Properties properties, String ...propertyValuePairs) {
        for (int i=0; i<propertyValuePairs.length; ) {
        	properties.setProperty(propertyValuePairs[i++], propertyValuePairs[i++]);
        }
        FullTextIndex<Long> ndx = new FullTextIndex<Long>(indexManager, namespace, ITx.UNISOLATED, properties);
        ndx.create();
        return ndx;
	}

	FullTextIndex<Long> createFullTextIndex(String namespace, String ...propertyValuePairs) {
        return createFullTextIndex(namespace, (Properties)getProperties().clone(), propertyValuePairs);
	}
	
	public void tearDown() throws Exception {
		if (indexManager != null) {
           indexManager.destroy();
		}
		super.tearDown();
	}

	String getNamespace() {
		return namespace;
	}

	IIndexManager getIndexManager() {
		return indexManager;
	}

	void setIndexManager(IIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	FullTextIndex<Long> getNdx() {
		return ndx;
	}

	Properties getSearchProperties() {
		return properties;
	}

	protected Analyzer getAnalyzer(String lang, boolean filterStopWords) {
		return getNdx().getAnalyzer(lang, filterStopWords);
	}

	protected void comparisonTest(String lang, boolean filterStopWords, String text, String spaceSeparated)
			throws IOException {
		if (spaceSeparated == null) {
			String rslt = getTokenStream(getAnalyzer(lang, filterStopWords), text);
			throw new RuntimeException("Got \"" + rslt+ "\"");
		}
			compareTokenStream(getAnalyzer(lang, filterStopWords), text,
					split(spaceSeparated)); //$NON-NLS-1$
			}

	private String[] split(String spaceSeparated) {
		if (spaceSeparated.length()==0) {
			return new String[0];
		}
		return spaceSeparated.split(" ");
	}

	protected String getTokenStream(Analyzer a, String text) throws IOException {
		StringBuffer sb = new StringBuffer();
		TokenStream s = a.tokenStream(null, new StringReader(text));
	    while (s.incrementToken()) {
	        final TermAttribute term = s.getAttribute(TermAttribute.class);
	        if (sb.length()!=0) {
	        	sb.append(" ");
	        }
	        sb.append(term.term());
	    }
		return sb.toString();
	}

	private void compareTokenStream(Analyzer a, String text, String expected[]) throws IOException {
		TokenStream s = a.tokenStream(null, new StringReader(text));
		int ix = 0;
		while (s.incrementToken()) {
			final TermAttribute term = s.getAttribute(TermAttribute.class);
			final String word = term.term();
			assertTrue(ix < expected.length);
			assertEquals(expected[ix++], word);
		}
		assertEquals(ix, expected.length);
	}

	protected void checkConfig(boolean threeLetterOnly, String classname, String ...langs) {
		for (String lang:langs) {
			// The DefaultAnalyzerFactory only works for language tags of length exactly three.
			if ((!threeLetterOnly) || (lang != null && lang.length()==3)) {
				assertEquals(classname, getAnalyzer(lang,true).getClass().getSimpleName());
				if (!threeLetterOnly) {
					assertEquals(classname, getAnalyzer(lang+"-x-foobar",true).getClass().getSimpleName()); //$NON-NLS-1$
				}
			}
		}
	}
	protected void checkConfig(String classname, String ...langs) {
		checkConfig(false, classname, langs);
	}

}
