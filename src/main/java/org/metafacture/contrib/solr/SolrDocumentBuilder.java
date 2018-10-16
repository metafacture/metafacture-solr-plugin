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

import org.apache.solr.common.SolrInputDocument;
import org.metafacture.contrib.framework.SolrDocumentReceiver;
import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description("Builds a Solr Document from metafacture stream events.")
@In(StreamReceiver.class)
@Out(SolrDocumentReceiver.class)
@FluxCommand("build-solr-doc")
public class SolrDocumentBuilder extends DefaultStreamPipe<ObjectReceiver<SolrInputDocument>> {

    private SolrInputDocument document;
    private String updateMethod;
    private String updateFieldName;
    private List<String> updateFieldValues;

    public SolrDocumentBuilder() {
        updateMethod = "";
        updateFieldValues = new ArrayList<>();
    }

    @Override
    public void startRecord(String identifier) {
        document = new SolrInputDocument();
    }

    @Override
    public void endRecord() {
        getReceiver().process(document);
    }

    @Override
    public void startEntity(String name) {
        updateMethod = name;
    }

    @Override
    public void endEntity() {
        if (!updateMethod.isEmpty()) {
            Map<String,Object> atomicUpdateAction = new HashMap<>();
            atomicUpdateAction.put(updateMethod, new ArrayList<>(updateFieldValues));
            document.addField(updateFieldName, atomicUpdateAction);
        }
        updateMethod = "";
    }

    @Override
    public void literal(String name, String value) {
        if (updateMethod.isEmpty()) {
            document.addField(name, value);
        } else {
            updateFieldName = name;
            updateFieldValues.add(value);
        }
    }

    @Override
    public void onResetStream() {
        updateFieldValues.clear();
    }

    @Override
    public void onCloseStream() {
        updateFieldValues.clear();
    }
}
