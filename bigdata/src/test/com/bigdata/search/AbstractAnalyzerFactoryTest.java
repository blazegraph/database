/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
/*
 * Created on May 7, 2014
 */
package com.bigdata.search;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public abstract class AbstractAnalyzerFactoryTest extends AbstractSearchTest {

    public AbstractAnalyzerFactoryTest() {
	}
    
    public AbstractAnalyzerFactoryTest(String arg0) {
    	super(arg0);
	}
    
    public void setUp() throws Exception {
    	super.setUp();
	    init(getExtraProperties());
    }
    abstract String[] getExtraProperties();
    
	private Analyzer getAnalyzer(String lang, boolean filterStopWords) {
		return getNdx().getAnalyzer(lang, filterStopWords);
	}
	
	private void comparisonTest(String lang, 
			boolean stopWordsSignificant, 
			String text, 
			String spaceSeparated)  throws IOException {
		compareTokenStream(getAnalyzer(lang, stopWordsSignificant), text,
				spaceSeparated.split(" "));
		}
	private void compareTokenStream(Analyzer a, String text, String expected[]) throws IOException {
		TokenStream s = a.tokenStream(null, new StringReader(text));
		int ix = 0;
        while (s.incrementToken()) {
            final TermAttribute term = s.getAttribute(TermAttribute.class);
            final String word = term.term();
            assertTrue(ix < expected.length);
        	assertEquals(word, expected[ix++]);
        }
        assertEquals(ix, expected.length);
	}
	

    public void testEnglishFilterStopWords() throws IOException {
    	for (String lang: new String[]{ "eng", null, "" }) {
    	    comparisonTest(lang,
    			true,
    			"The test to end all tests! Forever.",
    			"test end all tests forever"
    			);
    	}
    }
    public void testEnglishNoFilter() throws IOException {
    	for (String lang: new String[]{ "eng", null, "" }) {
    	    comparisonTest(lang,
    			false,
    			"The test to end all tests! Forever.",
    			"the test to end all tests forever"
    			);
    	}
    }
    
    // Note we careful use a three letter language code for german.
    // 'de' is more standard, but the DefaultAnalyzerFactory does not
    // implement 'de' correctly.
    public void testGermanFilterStopWords() throws IOException {
    	comparisonTest("ger",
    			true,
    			"Hanoi - Im Streit um die Vorherrschaft im Südchinesischen Meer ist es zu einer " +
    			"erneuten Auseinandersetzung gekommen:",
    			"hanoi strei um vorherrschaf sudchinesisch meer zu erneu auseinandersetzung gekomm"
    			);
    	
    }

    // Note we careful use a three letter language code for Russian.
    // 'ru' is more standard, but the DefaultAnalyzerFactory does not
    // implement 'ru' correctly.
    public void testRussianFilterStopWords() throws IOException {
    	comparisonTest("rus",
    			true,
				// I hope this is not offensive text.
			"Они ответственны полностью и за ту, и за другую трагедию. " +
		    "Мы уже получили данные от сочувствующих нам офицеров СБУ.",
    			"ответствен полност ту друг трагед получ дан сочувств нам офицер сбу"
    			);
    	
    }
    public void testGermanNoStopWords() throws IOException {
    	comparisonTest("ger",
    			false,
    			"Hanoi - Im Streit um die Vorherrschaft im Südchinesischen Meer ist es zu einer " +
    			"erneuten Auseinandersetzung gekommen:",
    			"hanoi im strei um die vorherrschaf im sudchinesisch meer ist es zu ein erneu auseinandersetzung gekomm"
    			);
    	
    }
    public void testRussianNoStopWords() throws IOException {
    	comparisonTest("rus",
    			false,
				// I hope this is not offensive text.
    			"Они ответственны полностью и за ту, и за другую трагедию. " +
    		    "Мы уже получили данные от сочувствующих нам офицеров СБУ.",
    			"он ответствен полност и за ту и за друг трагед мы уж получ дан от сочувств нам офицер сбу"
    			);
    	
    }
    public void testJapanese() throws IOException {
    	for (boolean filterStopWords: new Boolean[]{true, false}) {
    	comparisonTest("jpn",
      filterStopWords,
		// I hope this is not offensive text.
	"高林純示 生態学研究センター教授らの研究グループと松井健二 山口大学医学系研究科（農学系）教授らの研究グループは、",
    "高林 林純 純示 生態 態学 学研 研究 究セ セン ンタ ター ー教 教授 授ら らの の研 研究 究グ グル ルー " +
	"ープ プと と松 松井 井健 健二 山口 口大 大学 学医 医学 学系 系研 " +
    "研究 究科 農学 学系 教授 授ら らの の研 研究 究グ グル ルー ープ プは");
    	}
    }
    public void testConfiguredLanguages() {
    	checkConfig("BrazilianAnalyzer", "por", "pt");
        checkConfig("ChineseAnalyzer", "zho", "chi", "zh");
        checkConfig("CJKAnalyzer", "jpn", "ja", "kor", "ko");
        checkConfig("CzechAnalyzer", "ces", "cze", "cs");
        checkConfig("DutchAnalyzer", "dut", "nld", "nl");
        checkConfig("GermanAnalyzer", "deu", "ger", "de");
        checkConfig("GreekAnalyzer", "gre", "ell", "el");
        checkConfig("RussianAnalyzer", "rus", "ru");
        checkConfig("ThaiAnalyzer", "th", "tha");
        checkConfig("StandardAnalyzer", "en", "eng", "", null);
    }

	private void checkConfig(String classname, String ...langs) {
		for (String lang:langs) {
			// The DefaultAnalyzerFactory only works for language tags of length exactly three.
//			if (lang != null && lang.length()==3)
			{
			assertEquals(classname, getAnalyzer(lang,true).getClass().getSimpleName());
			assertEquals(classname, getAnalyzer(lang+"-x-foobar",true).getClass().getSimpleName());
			}
		}
		
	}
}
