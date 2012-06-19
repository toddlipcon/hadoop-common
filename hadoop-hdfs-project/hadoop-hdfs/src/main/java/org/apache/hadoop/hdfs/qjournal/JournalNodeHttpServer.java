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
package org.apache.hadoop.hdfs.qjournal;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_KRB_HTTPS_USER_NAME_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalNodeHttpServer {
  // TODO: move to DFSConfigKeys
  static final String DFS_JOURNALNODE_HTTP_ADDRESS_KEY = "dfs.journalnode.http-address";
  static final int DEFAULT_PORT = 8480;
  private static final String DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT =
      "0.0.0.0:" + DEFAULT_PORT;


  public static final Log LOG = LogFactory.getLog(
      JournalNodeHttpServer.class);

  public static final String JN_ATTRIBUTE_KEY = "localjournal";

  private HttpServer httpServer;
  private int infoPort;
  private int httpsPort;
  private JournalNode localJournalNode;

  private final Configuration conf;

  JournalNodeHttpServer(Configuration conf, JournalNode jn) {
    this.conf = conf;
    this.localJournalNode = jn;
  }

  void start() throws IOException {
    final InetSocketAddress bindAddr = getAddress(conf);

    // initialize the webserver for uploading/downloading files.
    // Kerberized SSL servers must be run from the host principal...
    UserGroupInformation httpUGI = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(
            conf.get(DFS_JOURNAL_KRB_HTTPS_USER_NAME_KEY),
            bindAddr.getHostName()), conf.get(DFS_JOURNAL_KEYTAB_FILE_KEY));
    try {
      httpServer = httpUGI.doAs(new PrivilegedExceptionAction<HttpServer>() {
        @Override
        public HttpServer run() throws IOException, InterruptedException {
          LOG.info("Starting web server as: "
              + UserGroupInformation.getCurrentUser().getUserName());

          int tmpInfoPort = bindAddr.getPort();
          httpServer = new HttpServer("journal", bindAddr.getHostName(),
              tmpInfoPort, tmpInfoPort == 0, conf, new AccessControlList(conf
                  .get(DFS_ADMIN, " ")));

          if (UserGroupInformation.isSecurityEnabled()) {
            // TODO: implementation 
          }
          httpServer.setAttribute(JN_ATTRIBUTE_KEY, localJournalNode);
          httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          // use "/getimage" because GetJournalEditServlet uses some
          // GetImageServlet methods.
          // TODO: change getimage to getedit
          httpServer.addInternalServlet("getimage", "/getimage",
              GetJournalEditServlet.class, true);
          httpServer.start();
          return httpServer;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = httpServer.getPort();
    if (!UserGroupInformation.isSecurityEnabled()) {
      httpsPort = infoPort;
    }

    LOG.info("Journal Web-server up at: " + bindAddr + ":" + infoPort
        + " and https port is: " + httpsPort);
  }

  void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
  
  /**
   * Return the actual address bound to by the running server.
   */
  public InetSocketAddress getAddress() {
    InetSocketAddress addr = httpServer.getListenerAddress();
    assert addr.getPort() != 0;
    return addr;
  }

  private static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
        DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr, DEFAULT_PORT,
        DFS_JOURNALNODE_HTTP_ADDRESS_KEY);
  }

  public static Journal getJournalFromContext(ServletContext context, String jid) {
    JournalNode jn = (JournalNode)context.getAttribute(JN_ATTRIBUTE_KEY);
    // TODO: we probably don't want to create in this case!
    return jn.getOrCreateJournal(jid);
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }
}
