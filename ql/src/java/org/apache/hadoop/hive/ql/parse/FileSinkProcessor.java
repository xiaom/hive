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

package org.apache.hadoop.hive.ql.parse;

import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;

/**
 * FileSinkProcessor handles addition of merge, move and stats tasks for filesinks
 *
 */
public class FileSinkProcessor implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(FileSinkProcessor.class.getName());

  @Override
  /*
   * (non-Javadoc)
   * we should ideally not modify the tree we traverse.
   * However, since we need to walk the tree at any time when we modify the
   * operator, we might as well do it here.
   */
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {

    GenTezProcContext context = (GenTezProcContext) procCtx;
    FileSinkOperator fileSink = (FileSinkOperator) nd;

    // just remember it for later processing
    context.fileSinkSet.add(fileSink);
    return true;
  }
}