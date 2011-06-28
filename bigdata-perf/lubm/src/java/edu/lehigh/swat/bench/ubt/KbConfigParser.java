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

package edu.lehigh.swat.bench.ubt;

import java.io.*;
import java.util.*;

public class KbConfigParser
    extends ConfigParser {
  /** delimiter between the attribute id and value. */
  public static final char C_ATTR_DELIM = '=';
  /** name of class attribute */
  public static final String S_ATTR_CLASS = "class";
  /** name of database attribute */
  public static final String S_ATTR_DB = "database";
  /** name of data attribute */
  public static final String S_ATTR_DATA = "data";
  /** name of ontology attribute */
  public static final String S_ATTR_ONT = "ontology";

  /** KbSpecification object representing the current section */
  private KbSpecification kb_ = null;
  /** list of target systems */
  private Vector kbList_;
  /** flags whether the target system is memory-based or not */
  private boolean isMemory_;

  /**
   * constructor
   */
  public KbConfigParser() {
  }

  /**
   * Creates the target system list by parsing the specified knowledge base config file.
   * @param fileName Name of the knowledge base config file.
   * @return The created target system list.
   * @throws java.lang.Exception if there are IO errors or syntax/content errors.
   */
  public Vector createKbList(String fileName, boolean isMemory) throws Exception {
    isMemory_ = isMemory;
    kbList_ = new Vector();
    parse(fileName);
    return kbList_;
  }

  /**
   * Line processor.
   * @param line A line retrieved from the file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  void handleLine(String line) throws Exception {
    //comment line
    if (line.startsWith(COMMENT_PREFIX))
      return;

    String value = null;
    String attribute = null;
    int n;

    if (isStartOfSection(line)) {
      endPrevSection();
      //new section
      kb_ = new KbSpecification();
      //Memo: The substring begins at the specified beginIndex and extends to the
      //character at (endIndex-1).
      kb_.id_ = line.substring(1, line.length() - 1);
      if (kb_.id_.length() <= 0) {
        error("Empty id!");
      }
    }
    else {
      if ( (n = line.indexOf(C_ATTR_DELIM)) < 0) {
        error("Invalid attribute!");
      }
      else {
        attribute = line.substring(0, n).trim();
        value = line.substring(n + 1, line.length()).trim();
        if (value.length() <= 0) {
          error("Empty value!");
        }
      }
      if (attribute.equals(S_ATTR_CLASS)) {
        kb_.kbClass_ = value;
      }
      else if (attribute.equals(S_ATTR_DB)) {
        kb_.dbFile_ = value;
      }
      else if (attribute.equals(S_ATTR_DATA)) {
        kb_.dataDir_ = value;
      }
      else if (attribute.equals(S_ATTR_ONT)) {
        kb_.ontology_ = value;
      }
      else {
        //ignore
      }
    }
  }

  /**
   * Processor of end of file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  void handleEndOfFile() throws Exception {
    if (kb_ != null) {
      endPrevSection();
    }
  }

  /**
   * Wraps up the previous section.
   * @throws java.lang.Exception when a supposed section is incomplete in content.
   */
  void endPrevSection() throws Exception {
    if (kb_ != null) {
      if (null == kb_.kbClass_) {
        error("Missing repository factory class information!");
      }
      if (!isMemory_ && null == kb_.dbFile_) {
        error("Missing database information!");
      }
      if (null == kb_.dataDir_) {
        error("Missing test data directory information!");
      }
      if (null == kb_.ontology_) {
        error("Missing ontology information!");
      }
      kbList_.add(kb_);
    }
  }
}