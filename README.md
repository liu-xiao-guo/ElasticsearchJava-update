This is a very simply app showing how to update Elasticsearch documents.

Firstly, you need to create documents in Kibana as follows:

PUT employees/_doc/1
{
  "id": "1",
  "sex": "male",
  "age": 28,
  "name": "Mark"
}


PUT employees/_doc/2
{
  "id": "2",
  "sex": "female",
  "age": 22,
  "name": "Grace"
}
