/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.io.Text;

import java.sql.Date;

public class VectorUDFDateString extends StringUnaryUDF {
  private static final long serialVersionUID = 1L;

  private static final Log LOG = LogFactory.getLog(
      VectorUDFDateString.class.getName());

  public VectorUDFDateString(int colNum, int outputColumn) {
    super(colNum, outputColumn, new StringUnaryUDF.IUDFUnaryString() {
      Text t = new Text();

      @Override
      public Text evaluate(Text s) {
        if (s == null) {
          return null;
        }
        try {
          Date date = Date.valueOf(s.toString());
          t.set(date.toString());
          return t;
        } catch (IllegalArgumentException e) {
          if (LOG.isDebugEnabled()) {
            LOG.info("VectorUDFDateString passed bad string for Date.valueOf '" + s.toString() + "'");
          }
          return null;
        }
      }
    });
  }

  public VectorUDFDateString() {
    super();
  }
}
