/**
 * by Yuanbo Guo
 * Semantic Web and Agent Technology Lab, CSE Department, Lehigh University, USA
 * Copyright (C) 2004
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.lehigh.swat.bench.uba;

import java.io.*;

public abstract class RdfWriter implements Writer {
  /** abbreviation of univ-bench ontology namesapce */
  static final String T_ONTO_NS = "ub";
  /** prefix of univ-bench ontology namespace */
  static final String T_ONTO_PREFIX = T_ONTO_NS + ":";
  /** abbreviation of RDF namespace */
  static final String T_RDF_NS = "rdf";
  /** prefix of RDF namespace */
  static final String T_RDF_PREFIX = T_RDF_NS + ":";
  /** abbreviation of RDFS namespace */
  static final String T_RDFS_NS = "rdfs";
  /** prefix of RDFS namespace */
  static final String T_RDFS_PREFIX = T_RDF_NS + ":";
  /** string of "rdf:Id" */
  static final String T_RDF_ID = T_RDF_PREFIX + "ID";
  /** string of "rdf:about" */
  static final String T_RDF_ABOUT = T_RDF_PREFIX + "about";
  /** string of "rdf:resource */
  static final String T_RDF_RES = T_RDF_PREFIX + "resource";
  /** white space string */
  static final String T_SPACE = " ";

  /** output stream */
  PrintStream out = null;
  /** the generator */
  Generator generator;

  /**
   * Constructor.
   * @param generator The generator object.
   */
  public RdfWriter(Generator generator) {
    this.generator = generator;
  }


  /** print [s] unless suppressed. */
  public void println(String s) {
        if (out != null)
            out.println(s);
  }

  /** print newline unless suppressed. */
  public void println() {
        if (out != null)
            out.println();
  }

  public void close() {
      if(out!=null)
          out.close();
  }
  
  /**
   * Implementation of Writer:start.
   */
  public void start() {}

  /**
   * Implementation of Writer:end.
   */
  public void end() {}

  /**
   * Implementation of Writer:startFile.
   */
  public void startFile(String fileName,boolean suppress) {
      if(suppress) {
          out = null;
          return;
      }
      /*
       * Make sure the directory exists.
       */
      {
          File file = new File(fileName);
          File dir = file.getParentFile();
          if(!dir.exists()) {
              if(!dir.mkdir()) {
                  throw new RuntimeException("Could not create directory: "+dir);
              }
          }
      }
    String s;
    try {
      out = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)),false/*autoflush*/);
      s = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";
      println(s);
      s = "<" + T_RDF_PREFIX + "RDF";
      println(s);
      writeHeader();
    }
    catch (IOException e) {
//      System.out.println("Create file failure!");
        throw new RuntimeException("Create file failure: file="+fileName,e);
    }
  }

  /**
   * Implementation of Writer:endFile.
   */
  public void endFile() {
    String s;
    s = "</" + T_RDF_PREFIX + "RDF>";
    println(s);
    close();
  }

  /**
   * Implementation of Writer:startSection.
   */
  public void startSection(int classType, String id) {
    generator.startSectionCB(classType);
    println();
    String s = "<" + T_ONTO_PREFIX + Generator.CLASS_TOKEN[classType] + T_SPACE +
        T_RDF_ABOUT + "=\"" + id + "\">";
    println(s);
  }

  /**
   * Implementation of Writer:startAboutSection.
   */
  public void startAboutSection(int classType, String id) {
    generator.startAboutSectionCB(classType);
    println();
    String s = "<" + T_ONTO_PREFIX + Generator.CLASS_TOKEN[classType] + T_SPACE +
        T_RDF_ABOUT + "=\"" + id + "\">";
    println(s);
  }

  /**
   * Implementation of Writer:endSection.
   */
  public void endSection(int classType) {
    String s = "</" + T_ONTO_PREFIX + Generator.CLASS_TOKEN[classType] + ">";
    println(s);
  }

  /**
   * Implementation of Writer:addProperty.
   */
  public void addProperty(int property, String value, boolean isResource) {
    generator.addPropertyCB(property);

    String s;
    if (isResource) {
      s = "   <" + T_ONTO_PREFIX + Generator.PROP_TOKEN[property] + T_SPACE +
          T_RDF_RES + "=\"" + value + "\" />";
    }
    else { //literal
      s = "   <" + T_ONTO_PREFIX + Generator.PROP_TOKEN[property] + ">" + value +
          "</" + T_ONTO_PREFIX + Generator.PROP_TOKEN[property] + ">";
    }

    println(s);
  }

  /**
   * Implementation of Writer:addProperty.
   */
  public void addProperty(int property, int valueClass, String valueId) {
    generator.addPropertyCB(property);
    generator.addValueClassCB(valueClass);

    String s;
    s = "   <" + T_ONTO_PREFIX + Generator.PROP_TOKEN[property] + ">\n" +
        "      <" + T_ONTO_PREFIX + Generator.CLASS_TOKEN[valueClass] + T_SPACE +
        T_RDF_ABOUT + "=\"" + valueId + "\" />" +
        "   </" + T_ONTO_PREFIX + Generator.PROP_TOKEN[property] + ">";

    println(s);
  }

  /**
   * Writes the header part.
   */
  abstract void writeHeader();
}