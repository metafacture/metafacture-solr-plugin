/*
 * Copyright 2018 Deutsche Nationalbibliothek
 *
 * Licensed under the Apache License, Version 2.0 the "License";
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.metafacture.contrib.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jcsp.lang.Channel;
import org.jcsp.lang.ChannelOutput;
import org.jcsp.lang.One2AnyChannel;
import org.jcsp.lang.Parallel;
import org.jcsp.util.Buffer;
import org.metafacture.contrib.framework.SolrDocumentReceiver;
import org.metafacture.contrib.framework.helpers.DefaultSolrDocumentReceiver;
import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;

@Description("Adds documents to a Solr core.")
@In(SolrDocumentReceiver.class)
@Out(Void.class)
@FluxCommand("to-solr")
public class SolrWriter extends DefaultSolrDocumentReceiver {

    /** Solr Server URL */
    private String url;
    private String core;

    private SolrClient client;
    /** Number of document per commit */
    private int batchSize;
    /** Time range in which a commit will happen. */
    private int commitWithinMs;

    private int maxRetries;
    private int waitMs;

    /** Number of threads to run in parallel */
    int threads;
    One2AnyChannel<SolrInputDocument> documentChannel;
    ChannelOutput<SolrInputDocument> documentChannelOutput;

    private Thread workerThread;

    /** Flag for a hook that acts before the first processing occurs. */
    private boolean onStartup;

    public SolrWriter(String url) {
        this.url = url;
        this.core = "default";
        this.threads = 1;
        this.batchSize = 1;
        this.commitWithinMs = -1;
        this.onStartup = true;
        this.maxRetries = 0;
        this.waitMs = 10_000;
    }

    public void setCore(String core) {
        this.core = core;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setCommitWithinMs(int commitWithinMs) {
        this.commitWithinMs = commitWithinMs;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setWaitMs(int waitMs) {
        this.waitMs = waitMs;
    }

    @Override
    public void process(SolrInputDocument document) {
        if (onStartup) {
            HttpSolrClient httpClient = new HttpSolrClient.Builder()
                    .withBaseSolrUrl(url)
                    .allowCompression(true)
                    .build();
            httpClient.setRequestWriter(new BinaryRequestWriter());
            client = httpClient;

            documentChannel = Channel.one2any(new Buffer<>(2 * threads), threads);

            Parallel parallel = new Parallel();
            for (int i = 0; i < threads; i++) {
                SolrCommitProcess process = new SolrCommitProcess(documentChannel.in(), client, core);
                process.setBatchSize(batchSize);
                process.setCommitWithinMs(commitWithinMs);
                process.setMaxRetries(maxRetries);
                process.setWaitMs(waitMs);
                parallel.addProcess(process);
            }

            documentChannelOutput = documentChannel.out();

            onStartup = false;

            workerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    parallel.run();
                }
            });
            workerThread.start();
        }

        documentChannelOutput.write(document);
    }

    @Override
    public void resetStream() {
        onStartup = true;
        documentChannelOutput.poison(threads);
    }

    @Override
    public void closeStream() {
        documentChannelOutput.poison(threads);
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
