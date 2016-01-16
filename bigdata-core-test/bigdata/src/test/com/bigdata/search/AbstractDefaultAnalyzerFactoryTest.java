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


public abstract class AbstractDefaultAnalyzerFactoryTest extends AbstractAnalyzerFactoryTest  {

    public AbstractDefaultAnalyzerFactoryTest() {
	}
    
    public AbstractDefaultAnalyzerFactoryTest(String arg0) {
    	super(arg0);
	}
    
    public void testEnglishFilterStopWords() throws IOException {
    	for (String lang: new String[]{ "eng", null, "" }) { //$NON-NLS-1$ //$NON-NLS-2$
    	    comparisonTest(lang,
    			true,
    			"The test to end all tests! Forever.", //$NON-NLS-1$
    			"test end all tests forever" //$NON-NLS-1$
    			);
    	}
    }
    public void testEnglishNoFilter() throws IOException {
    	for (String lang: new String[]{ "eng", null, "" }) { //$NON-NLS-1$ //$NON-NLS-2$
    	    comparisonTest(lang,
    			false,
    			"The test to end all tests! Forever.", //$NON-NLS-1$
    			"the test to end all tests forever" //$NON-NLS-1$
    			);
    	}
    }
    
    // Note we careful use a three letter language code for german.
    // 'de' is more standard, but the DefaultAnalyzerFactory does not
    // implement 'de' correctly.
    public void testGermanFilterStopWords() throws IOException {
    	comparisonTest("ger", //$NON-NLS-1$
    			true,
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.10") + //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.11"), //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.12") //$NON-NLS-1$
    			);
    	
    }

    // Note we careful use a three letter language code for Russian.
    // 'ru' is more standard, but the DefaultAnalyzerFactory does not
    // implement 'ru' correctly.
    public void testRussianFilterStopWords() throws IOException {
    	comparisonTest("rus", //$NON-NLS-1$
    			true,
				// I hope this is not offensive text.
			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.14") + //$NON-NLS-1$
		    NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.15"), //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.16") //$NON-NLS-1$
    			);
    	
    }
    public void testGermanNoStopWords() throws IOException {
    	comparisonTest("ger", //$NON-NLS-1$
    			false,
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.18") + //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.19"), //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.20") //$NON-NLS-1$
    			);
    	
    }
    public void testRussianNoStopWords() throws IOException {
    	comparisonTest("rus", //$NON-NLS-1$
    			false,
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.22") + //$NON-NLS-1$
    		    NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.23"), //$NON-NLS-1$
    			NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.24") //$NON-NLS-1$
    			);
    	
    }
    public void testJapanese() throws IOException {
    	for (boolean filterStopWords: new Boolean[]{true, false}) {
    	comparisonTest("jpn", //$NON-NLS-1$
      filterStopWords,
	NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.26"), //$NON-NLS-1$
    NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.27") + //$NON-NLS-1$
	NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.28") + //$NON-NLS-1$
    NonEnglishExamples.getString("AbstractAnalyzerFactoryTest.29")); //$NON-NLS-1$
    	}
    }
    public void testConfiguredLanguages() {
    	checkConfig("BrazilianAnalyzer", "por", "pt"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        checkConfig("ChineseAnalyzer", "zho", "chi", "zh"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        checkConfig("CJKAnalyzer", "jpn", "ja", "kor", "ko"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        checkConfig("CzechAnalyzer", "ces", "cze", "cs"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        checkConfig("DutchAnalyzer", "dut", "nld", "nl"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        checkConfig("GermanAnalyzer", "deu", "ger", "de"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        checkConfig("GreekAnalyzer", "gre", "ell", "el"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        checkConfig("RussianAnalyzer", "rus", "ru"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        checkConfig("ThaiAnalyzer", "th", "tha"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        checkConfig("StandardAnalyzer", "en", "eng", "", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Override
	protected void checkConfig(String classname, String ...langs) {
		checkConfig(isBroken(), classname, langs);
		
	}
	abstract boolean isBroken() ;
}
