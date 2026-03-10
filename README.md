# metafacture-solr-plugin

## About

A plugin for [metafacture](https://github.com/metafacture/metafacture-core) that extends the standard XML module.

## Build

<figure>
<img src="https://jitpack.io/v/metafacture/metafacture-solr-plugin.svg" alt="https://jitpack.io/v/metafacture/metafacture-solr-plugin.svg" />
</figure>

`sh gradlew test fatJar`

Produces `metafacture-solr-VERSION-plugin.jar` in `build/libs` .

Place the build JAR inside the `plugins` directory of your
`metafacture-core` distribution.

## Command Reference

| Command | In | Out |
|--------|--------|--------|
| build-solr-doc | StreamReceiver | SolrDocumentReceiver |
| handle-solr-xml | XmlReceiver | SolrDocumentReceiver |
| to-solr | SolrDocumentReceiver | Void |

### build-solr-doc

#### Description

Builds a Solr Input Document from metafacture stream events.

The following metafacture events:

```
startRecord("ignored")
literal("id", "1")
literal("key", "value")
endRecord()
```

would create the following Solr Document

`{"id": "1", "key": "value" }`

Atomic index updates are handled by the `entity` event.

```
startRecord("ignored")
literal("id", "1")
entity("add")
literal("name", "alice")
literal("name", "bob")
endEntity()
endRecord()
```

creates the following Solr Document

`{"id": "1", "name": {"add": ["alice", "bob"]}}`

See also [Updating Parts of Documents 7.1](https://lucene.apache.org/solr/guide/7_1/updating-parts-of-documents.html) or [9.1](https://solr.apache.org/guide/solr/latest/indexing-guide/partial-document-updates.html).

#### Syntax

`build-solr-doc`

#### Example

Flux:

`... | build-solr-doc | to-solr(...);`

### handle-solr-xml

#### Description

A XML handler for Solr Index Updates.

#### Syntax

`handle-solr-xml`

#### Example

Flux:

`... | decode-xml | handle-solr-xml | ...`

### to-solr

#### Description

A sink that commits Solr Input Documents to a Apache Solr instance.

#### Syntax

`to-solr([url])`
Required parameter `url` sets the URL to Solr Server.

#### Options

- `core`: Solr Core (Default: default)

- `batchSize`: Number of documents per commit (Default: 1).

- `commitWithinMs`: Max time (in ms) before a commit will happen (Default: -1 (Disabled), See also: [Solr Ref Guide - commitWithin](https://lucene.apache.org/solr/guide/7_4/updatehandlers-in-solrconfig.html#UpdateHandlersinSolrConfig-commitWithin)).

- `threads`: Number of threads for concurrent batch processing (Default: 1).

- `maxRetries`: Number of max retries for non-successful commits (Default: 0).

- `waitMs`: Delay (in ms) before a retry will be triggered (Default: 10000).

#### Example

Minimal case:

`... | to-solr("https://example.com/solr/", core="test");`

`... | to-solr("https://example.com/solr/", core="test", batchSize="2", commitWithinMs="1000", threads="2");`
