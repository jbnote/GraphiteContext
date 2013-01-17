/*
 * GangliaContext.java
 *
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

package org.apache.hadoop.metrics.graphite;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

import java.net.Socket;
import java.net.SocketException;

/**
 * Metrics context for writing metrics to Graphite.<p/>
 *
 * This class is configured by setting ContextFactory attributes which in turn
 * are usually configured through a properties file.  All the attributes are
 * prefixed by the contextName. For example, the properties file might contain:
 * <pre>
 * mapred.class=org.apache.hadoop.metrics.graphite.GraphiteContext
 * mapred.period=60
 * mapred.serverName=graphite.foo.bar
 * mapred.port=2013
 * </pre>
 */
public class GraphiteContext extends AbstractMetricsContext {

    /* Configuration attribute names */
    protected static final String SERVER_NAME_PROPERTY = "serverName";
    protected static final String PERIOD_PROPERTY = "period";
    protected static final String PORT = "port";
    protected static final String PATH = "path";
    private String pathName = null;
    private Writer toServer = null;

    private static final String separator = " ";
    private static final String gsep = ".";

    /** Creates a new instance of GraphiteContext */
    public GraphiteContext() {}

    private void init_socket() throws IOException {
        if (toServer != null)
            return;

        String serverName = getAttribute(SERVER_NAME_PROPERTY);
        int port = Integer.parseInt(getAttribute(PORT));

        Socket server_socket = new Socket(serverName, port);
        toServer = new OutputStreamWriter(server_socket.getOutputStream());
    }

    @Override
    public void init(String contextName, ContextFactory factory) {
        super.init(contextName, factory);
        pathName = getAttribute(PATH);
        if (pathName == null) {
            pathName = "Platform.Hadoop";
        }
        parseAndSetPeriod(PERIOD_PROPERTY);
    }

    synchronized private void emitMetric(String metric) throws IOException {
        /* Not rate-limited, yet */
        if (toServer == null)
            init_socket();

        try {
            toServer.write(metric);
            toServer.flush();
        } catch (IOException problem) {
            toServer = null;
        }
    }

    private String escapeForGraphite(String i) {
        /* Seems like the standard way to cope with dots:
           https://answers.launchpad.net/graphite/+question/191343 */
        return i.replaceAll("\\W", "_");
    }

    private String endpath() {
        long tm = System.currentTimeMillis() / 1000; // Graphite doesn't handle milliseconds
        String endpath =  separator + tm + "\n";
        return endpath;
    }

    /**
     * Emits a metrics record to Graphite.
     */
    @Override
    public void emitRecord(String contextName, String recordName, OutputRecord outRec) throws IOException {
        String basepath = pathName + gsep + contextName + gsep + recordName + gsep;
        StringBuilder tagpath = new StringBuilder();
        for (String tagname : outRec.getTagNames()) {
            String tagval = outRec.getTag(tagname).toString();
            if (tagval.length() == 0)
                continue;
            /* Skip empty tags, which do occur */
            tagpath.append(escapeForGraphite(outRec.getTag(tagname).toString()));
            tagpath.append(gsep);
        }

        String startpath = basepath + tagpath.toString();
        String endpath = endpath();

        for (String metricName : outRec.getMetricNames()) {
            StringBuilder sb = new StringBuilder();
            sb.append(startpath);
            sb.append(escapeForGraphite(metricName));
            sb.append(separator);
            sb.append(outRec.getMetric(metricName));
            sb.append(endpath);
            emitMetric(sb.toString());
        }
    }
}
