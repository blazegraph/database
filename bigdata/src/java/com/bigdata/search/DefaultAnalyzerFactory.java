/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Dec 21, 2010
 */

package com.bigdata.search;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.util.Version;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * Default implementation registers a bunch of {@link Analyzer}s for various
 * language codes and then serves the appropriate {@link Analyzer} based on
 * the specified language code.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultAnalyzerFactory implements IAnalyzerFactory {

    private final FullTextIndex fullTextIndex;
    
    public DefaultAnalyzerFactory(final FullTextIndex fullTextIndex) {

        if (fullTextIndex == null)
            throw new IllegalArgumentException();
        
        this.fullTextIndex = fullTextIndex;
        
    }
    
    public Analyzer getAnalyzer(final String languageCode) {

        final IKeyBuilder keyBuilder = fullTextIndex.getKeyBuilder();

        Map<String, AnalyzerConstructor> map = getAnalyzers();
        
        AnalyzerConstructor ctor = null;
        
        if (languageCode == null) {
        
            if (keyBuilder.isUnicodeSupported()) {

                // The configured local for the database.
                final Locale locale = ((KeyBuilder) keyBuilder)
                        .getSortKeyGenerator().getLocale();

                // The analyzer for that locale.
                Analyzer a = getAnalyzer(locale.getLanguage());

                if (a != null)
                    return a;
            
            }
            
            // fall through
            
        } else {
            
            /*
             * Check the declared analyzers. We first check the three letter
             * language code. If we do not have a match there then we check the
             * 2 letter language code.
             */
            
            String code = languageCode;

            if (code.length() > 3) {

                code = code.substring(0, 2);

                ctor = map.get(languageCode);

            }

            if (ctor == null && code.length() > 2) {

                code = code.substring(0, 1);

                ctor = map.get(languageCode);
                
            }
            
        }
        
        if (ctor == null) {

            // request the default analyzer.
            
            ctor = map.get("");
            
            if (ctor == null) {

                throw new IllegalStateException("No entry for empty string?");
                
            }
            
        }

        Analyzer a = ctor.newInstance();
        
        return a;
        
    }

    abstract private static class AnalyzerConstructor {
        
        abstract public Analyzer newInstance();
        
    }

    /**
     * A map containing instances of the various kinds of analyzers that we know
     * about.
     * <p>
     * Note: There MUST be an entry under the empty string (""). This entry will
     * be requested when there is no entry for the specified language code.
     */
    private Map<String,AnalyzerConstructor> analyzers;
    
    /**
     * Initializes the various kinds of analyzers that we know about.
     * <p>
     * Note: Each {@link Analyzer} is registered under both the 3 letter and the
     * 2 letter language codes. See <a
     * href="http://www.loc.gov/standards/iso639-2/php/code_list.php">ISO 639-2</a>.
     * 
     * @todo get some informed advice on which {@link Analyzer}s map onto which
     *       language codes.
     * 
     * @todo thread safety? Analyzers produce token processors so maybe there is
     *       no problem here once things are initialized. If so, maybe this
     *       could be static.
     * 
     * @todo configuration. Could be configured by a file containing a class
     *       name and a list of codes that are handled by that class.
     * 
     * @todo strip language code down to 2/3 characters during lookup.
     * 
     * @todo There are a lot of pidgins based on french, english, and other
     *       languages that are not being assigned here.
     */
    synchronized private Map<String,AnalyzerConstructor> getAnalyzers() {
        
        if (analyzers != null) {

            return analyzers;
            
        }

        analyzers = new HashMap<String, AnalyzerConstructor>();

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new BrazilianAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("por", a);
            analyzers.put("pt", a);
        }

        /*
         * Claims to handle Chinese. Does single character extraction. Claims to
         * produce smaller indices as a result.
         * 
         * Note: you can not tokenize with the Chinese analyzer and the do
         * search using the CJK analyzer and visa versa.
         * 
         * Note: I have no idea whether this would work for Japanese and Korean
         * as well. I expect so, but no real clue.
         */
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new ChineseAnalyzer();
                }
            };
            analyzers.put("zho", a);
            analyzers.put("chi", a);
            analyzers.put("zh", a);
        }
        
        /*
         * Claims to handle Chinese, Japanese, Korean. Does double character
         * extraction with overlap.
         */
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new CJKAnalyzer(Version.LUCENE_CURRENT);
                }
            };
//            analyzers.put("zho", a);
//            analyzers.put("chi", a);
//            analyzers.put("zh", a);
            analyzers.put("jpn", a);
            analyzers.put("ja", a);
            analyzers.put("jpn", a);
            analyzers.put("kor",a);
            analyzers.put("ko",a);
        }

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new CzechAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("ces",a);
            analyzers.put("cze",a);
            analyzers.put("cs",a);
        }

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new DutchAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("dut",a);
            analyzers.put("nld",a);
            analyzers.put("nl",a);
        }
        
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new FrenchAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("fra",a); 
            analyzers.put("fre",a); 
            analyzers.put("fr",a);
        }

        /*
         * Note: There are a lot of language codes for German variants that
         * might be useful here.
         */
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new GermanAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("deu",a); 
            analyzers.put("ger",a); 
            analyzers.put("de",a);
        }
        
        // Note: ancient greek has a different code (grc).
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new GreekAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("gre",a); 
            analyzers.put("ell",a); 
            analyzers.put("el",a);
        }        

        // @todo what about other Cyrillic scripts?
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new RussianAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("rus",a); 
            analyzers.put("ru",a); 
        }        
        
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new ThaiAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("tha",a); 
            analyzers.put("th",a); 
        }

        // English
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new StandardAnalyzer(Version.LUCENE_CURRENT);
                }
            };
            analyzers.put("eng", a);
            analyzers.put("en", a);
            /*
             * Note: There MUST be an entry under the empty string (""). This
             * entry will be requested when there is no entry for the specified
             * language code.
             */
            analyzers.put("", a);
        }

        return analyzers;
        
    }

}
