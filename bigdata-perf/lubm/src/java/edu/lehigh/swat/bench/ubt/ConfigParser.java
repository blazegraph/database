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

abstract public class ConfigParser {
  /** comment prefix */
  public static final String COMMENT_PREFIX = "#";
  /** beginning character of a section id */
  public static final char C_SEC_BEGIN = '[';
  /** closing character of a section id */
  public static final char C_SEC_END = ']';

  /** file reader */
  private LineNumberReader reader_;
  /** file name */
  private String fileName_;

  /**
   * constructor
   */
  public ConfigParser() {
  }

  /**
   * Parses the specified file line by line.
   * @param fileName The config file.
       * @throws java.lang.Exception if there are IO errors or syntax/content errors.
   */
  void parse(String fileName) throws Exception {
    fileName_ = fileName;
    reader_ = new LineNumberReader(new FileReader(fileName));

    String line;
    handleBeginOfFile();
    while ( (line = readLine()) != null) {
      handleLine(line);
    }
    handleEndOfFile();
  }

  /**
   * Line processor.
   * @param line A line retrieved from the file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  abstract void handleLine(String line) throws Exception;

  /**
   * Processor of begin of file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  void handleBeginOfFile() throws Exception {
    //do nothing
  }

  /**
   * Processor of end of file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  void handleEndOfFile() throws Exception {
    //do nothing
  }

  /**
   * Reads the next line. Empty line will be skipped.
   * @return The next line.
   * @throws java.lang.Exception if an IO error occurs.
   */
  private String readLine() throws Exception {
    String line = null;

    while ( (line = reader_.readLine()) != null) {
      line.trim();
      if (line.length() > 0)
        break;
    }

    return line;
  }

  /**
       * Throws an exception when a syntax/content error is encountered in the file.
   * @param error The error message.
   * @throws java.lang.Exception when a syntax/content error is encountered in the file.
   */
  void error(String error) throws Exception {
    String err = fileName_ + ": Line " + reader_.getLineNumber() + ": " + error;
    throw new Exception(err);
  }

  /**
   * Check if the specified line is a section id line.
   * @param line Line string.
   * @return True if the line is a section id line, otherwise false.
   * @throws java.lang.Exception upon a syntax error
   */
  boolean isStartOfSection(String line) throws Exception {
    if (line.charAt(0) != C_SEC_BEGIN) {
      return false;
    }
    else {
      if (line.charAt(line.length() - 1) != C_SEC_END) {
        error("Syntax error!");
        return false;
      }
      return true;
    }
  }
}