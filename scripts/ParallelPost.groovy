#!/usr/bin/env groovy
/*
 * Copyright (c) 2018 Deutsche Nationalbibliothek
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Groovy: Version 2.5.0
 */
@GrabResolver(name='jitpack', root='https://jitpack.io')
@Grab('org.metafacture:metafacture-xml:5.0.0')
@Grab('com.github.eberhardtj:metafacture-solr-plugin:v0.1.1')
@Grab('org.slf4j:slf4j-api:1.7.25')
@Grab('org.slf4j:slf4j-simple:1.7.25')
import groovy.cli.picocli.CliBuilder
import org.metafacture.solr.SolrWriter
import org.metafacture.solr.SolrXmlHandler
import org.metafacture.xml.XmlDecoder

def summary = '\n' + 'Posts Apache Solr index updates (in XML) to a Solr Server.' +
        '\n' +
        '\n' +
        'Example: groovy ParallelPost.groovy -u "http://localhost:8983/solr/" -c "demo"' +
        '\n'

def cli = new CliBuilder(usage:'ParallelPost [-ibdth] -u URL -c CORE', header: '\nOptions:', footer: summary)
cli.with {
    i argName: 'file', longOpt: 'xml-input', 'Input (Default: stdin)', type: String.class, defaultValue: '-'
    u argName: 'url', longOpt: 'url', 'Solr Host URL', type: String.class, required: true
    c argName: 'core', longOpt: 'core', 'Core name', type: String.class, required: true
    b argName: 'num', longOpt: 'batch-size', 'Batch size', type: Integer.class, defaultValue: '10000'
    d argName: 'num', longOpt: 'delay', 'Delay before a commit happens (in ms)', type: Integer.class, defaultValue: '60000'
    t argName: 'num', longOpt: 'threads', 'Number of concurrent threads', type: Integer.class, defaultValue: '1'
    h longOpt: 'help', 'Show usage information'
}

def options = cli.parse(args)

if (!options) {
    return
}

if (options.h) {
    cli.usage()
    return
}

def useStdin = ((String) options.i) == '-'
String url = (String) options.u

InputStream inputStream = useStdin ? System.in : new FileInputStream((String) options.i)

def utf8 = 'UTF-8'

inputStream.withReader(utf8, { reader ->
    XmlDecoder decoder = new XmlDecoder()
    SolrXmlHandler handler = new SolrXmlHandler()

    SolrWriter writer = new SolrWriter(url)
    writer.setCore((String) options.c)
    writer.setBatchSize((Integer) options.b)
    writer.setCommitWithMs((Integer) options.d)
    writer.setThreads((Integer) options.t)

    decoder.setReceiver(handler)
    handler.setReceiver(writer)
    decoder.process(reader)
    decoder.closeStream()
})
