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

public class QueryConfigParser
    extends ConfigParser {
  /** beginning charater of a constituent atom of a query */
  public final char C_ATOM_BEGIN = '(';
  /** closing charater of a constituent atom of a query */
  public final char C_ATOM_END = ')';
  /** delimiter between arguments in an atom */
  public final char C_ARG_DELIM = ' ';

  /** QuerySpecification object representing the current section */
  private QuerySpecification query_ = null;
  /** list of queries */
  private Vector queryList_;

  /**
   * constructor
   */
  public QueryConfigParser() {
  }

  /**
   * Creates the query list by parsing the specified query config file.
   * @param fileName Name of the query config file.
   * @return The created query list.
       * @throws java.lang.Exception if there are IO errors or syntax/content errors.
   */
  public Vector createQueryList(String fileName) throws Exception {
    queryList_ = new Vector();
    parse(fileName);
    return queryList_;
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

    String predicate;
    String[] args;
    int i, j, n;

    if (isStartOfSection(line)) {
      endPrevSection();
      //new section
      query_ = new QuerySpecification();
      query_.id_ = line.substring(1, line.length() - 1);
      if (query_.id_.length() <= 0) {
        error("Empty id!");
      }
    }
    else {
      String q = query_.query_.getString();
      if (q.length() > 0)
        q += "\n";
      query_.query_.setString(q + line);
      if (line.charAt(0) != C_ATOM_BEGIN ||
          line.charAt(line.length() - 1) != C_ATOM_END) {
        //Ignore the syntax error so that other langugages can be used.
        //error("Syntax error");
        return;
      }
      line = line.substring(1, line.length() - 1);
      i = 0;
      for (n = 0; (i = line.indexOf(C_ARG_DELIM, i + 1)) >= 0; n++)
        ;
      if (n < 0) {
        predicate = line;
        args = null;
      }
      else {
        predicate = line.substring(0, i = line.indexOf(C_ARG_DELIM));
        line = line.substring(++i);
        args = new String[n];
        for (j = 0; j < n - 1; j++) {
          args[j] = line.substring(0, i = line.indexOf(C_ARG_DELIM));
          line = line.substring(++i);
        }
        args[j] = line;
      }
      query_.query_.addAtom(predicate, args);
    }
  }

  /**
   * Processor of end of file.
   * @throws java.lang.Exception upon syntax/context errors.
   */
  protected void handleEndOfFile() throws Exception {
    if (query_ != null) {
      endPrevSection();
    }
  }

  /**
   * Wraps up the previous section.
   */
  private void endPrevSection() {
    if (query_ != null) {
      queryList_.add(query_);
    }
  }
}