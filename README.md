# IMDB Web Scrapper

main->merge->transform->elasticinsert

GET movies\_search

{
    "size":0,

    "query":{

        "match":{

            "Genres":{

                "query":"Romance"

            }

        }

    },

    "aggs":{

        "quantity":{

            "terms":{

                "field":"Top Cast.keyword",

                "size":50

            }

        }

    }

}
