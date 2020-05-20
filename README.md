# elasticsearch-playground

#### Prerequisiti
* Tramite Kibana, aggiungere i dati di test, utilizzare l'indice **kibana_sample_data_ecommerce**.
* Per eseguire la query Nested, far girare i comandi:

        PUT nested_test
        {
          "mappings": {
            "properties": {
              "user": {
                "type": "nested"
              }
            }
          }
        }

        PUT nested_test/_doc/1
        {
          "group" : "fans",
          "user" : [
            {
              "first" : "John",
              "last" :  "Smith"
            },
            {
              "first" : "Alice",
              "last" :  "White"
            }
          ]
        }
        
#### Bootstrap (ElasticSearch + Kibana)
* make start

#### Test
* mvn test
