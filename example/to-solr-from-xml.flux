"records.xml"
| open-file
| decode-xml
| handle-solr-xml
| to-solr("http://localhost:1111/solr/", core="test6", commitWithinMs="1000");