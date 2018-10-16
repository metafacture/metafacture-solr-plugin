"records.xml"
| open-file
| decode-xml
| handle-solr-xml
| print;