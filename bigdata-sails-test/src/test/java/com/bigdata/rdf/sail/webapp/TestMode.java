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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.DefaultBigdataVocabulary;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * The test mode.
 */
enum TestMode {
   /**
    * Plain triples mode. Inference is disabled by default.
    */
   triples,
   /**
    * Triples with support for statements about statements.
    */
   sids,
   /**
    * Quads (does not support inference/truth maintenance).
    */
   quads,
   /**
    * Plain triples mode with incremental truth maintenance enabled by default.
    */
   triplesPlusTruthMaintenance;
   /**
    * Return <code>true</code> iff it is possible to setup truth maintenance for
    * this configuration.
    */
   public boolean isTruthMaintenanceSupported() {
      return this != quads;
   }

   public boolean isQuads() {

       return this == quads;

   }
   
   public Properties getProperties() {

      return getProperties(new Properties());
   }

   /**
    * Override the caller's properties object with the settings required to
    * establish the {@link TestMode}. To avoid side-effects
    * <>
    * 
    * 
    * @param properties
    *           The caller's properties object.
    * 
    * @return The caller's properties object with the overriden settings.
    */
   public Properties getProperties(final Properties properties) {

      switch (this) {
      case quads:
         properties.setProperty(AbstractTripleStore.Options.QUADS_MODE, "true");
         properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
         properties.setProperty(BigdataSail.Options.JUSTIFY, "false");
         properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
               NoAxioms.class.getName());
         properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
               NoVocabulary.class.getName());
         properties.setProperty(
               AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
         break;
      case triples:
         properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
         properties.setProperty(BigdataSail.Options.JUSTIFY, "false");
         properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
               NoAxioms.class.getName());
         properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
               NoVocabulary.class.getName());
         properties.setProperty(
               AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
         break;
      case sids:
         properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
         properties.setProperty(BigdataSail.Options.JUSTIFY, "false");
         properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
               NoAxioms.class.getName());
         properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
               NoVocabulary.class.getName());
         properties.setProperty(
               AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "true");
         break;
      case triplesPlusTruthMaintenance:
         properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "true");
         properties.setProperty(BigdataSail.Options.JUSTIFY, "true");
         properties.setProperty(BigdataSail.Options.AXIOMS_CLASS,
               OwlAxioms.class.getName());
         properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
               DefaultBigdataVocabulary.class.getName());
         properties.setProperty(
               AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
         break;
      default:
         throw new UnsupportedOperationException("Unknown mode: " + this);
      }
      // if (false/* triples w/ truth maintenance */) {
      // properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
      // "false");
      // }
      // if (false/* sids w/ truth maintenance */) {
      // properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
      // "true");
      // }

      return properties;
   }

}
