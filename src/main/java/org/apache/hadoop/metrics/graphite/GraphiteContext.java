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
    private String serverName = null;
    private String pathName = null;
    private int port = 0;

    private static final String separator = " ";
    private static final String gsep = ".";

    /** Creates a new instance of GraphiteContext */
    public GraphiteContext() {}

    public void init(String contextName, ContextFactory factory) {
        super.init(contextName, factory);

        serverName = getAttribute(SERVER_NAME_PROPERTY);
        port = Integer.parseInt(getAttribute(PORT));

        pathName = getAttribute(PATH);
        if (pathName == null) {
            pathName = "Platform.Hadoop";
        }

        parseAndSetPeriod(PERIOD_PROPERTY);

    }

    private String escapeForGraphite(String i) {
        return i.replace('.', '_');
    }

    private String endpath() {
        long tm = System.currentTimeMillis() / 1000; // Graphite doesn't handle milliseconds
        String endpath =  separator + tm + "\n";
        return endpath;
    }

    /**
     * Emits a metrics record to Graphite.
     */
    public void emitRecord(String contextName, String recordName, OutputRecord outRec) throws IOException {
        String basepath = pathName + gsep + contextName + gsep + recordName + gsep;
        StringBuilder tagpath = new StringBuilder();
        for (String tagname : outRec.getTagNames()) {
            tagpath.append(escapeForGraphite(outRec.getTag(tagname).toString()));
            tagpath.append(gsep);
        }

        String startpath = basepath + tagpath.toString();
        String endpath = endpath();

        for (String metricName : outRec.getMetricNames()) {
            StringBuilder sb = new StringBuilder();
            /* should be merged */
            sb.append(startpath);
            sb.append(metricName);
            sb.append(separator);
            sb.append(outRec.getMetric(metricName));
            sb.append(endpath);
            emitMetric(sb.toString());
        }
    }

    protected void emitMetric(String metric) throws IOException {
        Socket socket = new Socket(serverName, port);
        try {
            Writer writer = new OutputStreamWriter(socket.getOutputStream());
            writer.write(metric);
            writer.flush();
            writer.close();
        } finally {
            socket.close();
        }
    }
}
