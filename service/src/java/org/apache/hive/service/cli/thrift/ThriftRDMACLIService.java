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

package org.apache.hive.service.cli.thrift;

import java.net.InetSocketAddress;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.mellanox.jxio.EventName;
import com.mellanox.jxio.EventQueueHandler;
import com.mellanox.jxio.EventReason;
import com.mellanox.jxio.Msg;
import com.mellanox.jxio.MsgPool;
import com.mellanox.jxio.ServerPortal;
import com.mellanox.jxio.ServerSession;
import com.mellanox.jxio.ServerSession.SessionKey;
import com.mellanox.jxio.WorkerCache.Worker;
import com.mellanox.jxio.exceptions.JxioGeneralException;
import com.mellanox.jxio.exceptions.JxioSessionClosedException;

public class ThriftRDMACLIService extends ThriftCLIService {

  private final EventQueueHandler eqh;
  private final ServerPortal listener; // play the role of TServerSocket
  private ArrayList<MsgPool> msgPools = new ArrayList<MsgPool>();

  public ThriftRDMACLIService(CLIService cliService) {
    super(cliService, "ThriftRDMACLIService");
  }
  /**
   * Callbacks for the listener server portal
   */
  public class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.info("got event " + session_event.toString() + "because of " + reason.toString());
      if (session_event == EventName.PORTAL_CLOSED) {
        eqh.breakEventLoop();
      }
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      LOG.info("onSessionNew " + sesKey.getUri());
      SessionServerCallbacks callbacks = new SessionServerCallbacks(sesKey.getUri());
      ServerSession session = new ServerSession(sesKey, callbacks);
      callbacks.setSession(session);
      listener.accept(session);
    }
  }

  // FIXME: copied from tachyon code as skeleton code
  public class SessionServerCallbacks implements ServerSession.Callbacks {
    private final DataServerMessage responseMessage;
    private ServerSession session;
    // XXX: what URI should we look
    // TODO: check the interface of SessionServerCallback
    public SessionServerCallbacks(String uri) {
      responseMessage = DataServerMessage.createBlockResponseMessage(true, blockId, 0, -1);
      responseMessage.setLockId(lockId);
    }

    public void setSession(ServerSession ses) {
      session = ses;
    }

    public void onRequest(Msg m) {
      if (session.getIsClosing()) {
        session.discardRequest(m);
      } else {
        responseMessage.copy(m.getOut());
        try {
          session.sendResponse(m);
        } catch (JxioGeneralException e) {
          LOG.error("Exception accured while sending messgae "+e.toString());
        } catch (JxioSessionClosedException e) {
          LOG.error("session was closed unexpectedly "+e.toString());
        }
      }

      if (!session.getIsClosing() && responseMessage.finishSending()) {
        LOG.info("finished reading, closing session");
        session.close();
      }
    }

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.info("got event " + session_event.toString() + ", the reason is " + reason.toString());
      if (session_event == EventName.SESSION_CLOSED) {
        responseMessage.close();
        mBlocksLocker.unlock(Math.abs(responseMessage.getBlockId()), responseMessage.getLockId());
      }
    }

    public boolean onMsgError(Msg msg, EventReason reason) {
      LOG.error(this.toString() + " onMsgErrorCallback. reason is " + reason);
      return true;
    }
  }
  /**
   * run(): run the server thread
   */
  @Override
  public void run() {
    try {
      hiveAuthFactory = new HiveAuthFactory();
      // XXX: remove TTransport
      // TTransportFactory  transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);

      String portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }

      String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
      if (hiveHost == null) {
        hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
      }

      if (hiveHost != null && !hiveHost.isEmpty()) {
        serverAddress = new InetSocketAddress(hiveHost, portNum);
      } else {
        serverAddress = new  InetSocketAddress(portNum);
      }

      /**
       * XXX: Let us keep it simple, use one thread for now
      minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
      maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
       */
      /** NOTES:
       * TServerSocket is part of the transparition layer created by thrift
       * Its resposibility is to listen on the socket for requests
       */
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname +
              " Not configured for SSL connection");
        }
        serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum,
            keyStorePath, hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD));
      }

      // TODO: create a JXIO server to handle request
      /*
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
      .processorFactory(processorFactory)
      .transportFactory(transportFactory)
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads);

      server = new TThreadPoolServer(sargs);
      */
      LOG.info("ThriftRDMACLIService listening on " + serverAddress);
      // XXX: how to let it listening on serverAddress/Port?
      eqh.runEventLoop(-1, -1);
      server.serve();

    } catch (Throwable t) {
      LOG.error("Error: ", t);
    }

  }
}
