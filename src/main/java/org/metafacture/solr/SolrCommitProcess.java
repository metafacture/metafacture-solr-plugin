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
package org.metafacture.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.jcsp.lang.CSProcess;
import org.jcsp.lang.ChannelInput;
import org.jcsp.lang.PoisonException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SolrCommitProcess implements CSProcess {
    private ChannelInput<SolrInputDocument> in;
    private SolrClient client;
    private String collection;
    private int batchSize;
    private int commitWithinMs;

    public SolrCommitProcess(ChannelInput<SolrInputDocument> in,
                             SolrClient client,
                             String collection)
    {
        this.in = in;
        this.client = client;
        this.collection = collection;
        this.batchSize = 1;
        this.commitWithinMs = -1;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setCommitWithinMs(int commitWithinMs) {
        this.commitWithinMs = commitWithinMs;
    }

    @Override
    public void run() {
        List<SolrInputDocument> batch = new ArrayList<>(batchSize);

        while (true) {
            SolrInputDocument document;
            try {
                document = receive();
                batch.add(document);
            } catch (PoisonException e) {
                if (!batch.isEmpty()) commit(batch);
                break;
            }

            if (batch.size() == batchSize) {
                commit(batch);
                batch = new ArrayList<>(batchSize);
            }
        }
    }

    private boolean commit(List<SolrInputDocument> documents) {
        try {
            UpdateResponse response;
            if (commitWithinMs > 0)
                response = client.add(collection, documents, commitWithinMs);
            else
                response = client.add(collection, documents);

            if (response.getStatus() != 0) {
                return false;
            }
        } catch (Exception e) {
            System.err.println("Could not commit batch, due to " + e.getMessage());
            return false;
        }
        return true;
    }

    private SolrInputDocument receive() {
        return in.read();
    }
}