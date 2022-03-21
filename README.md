# LinksInLOD

To analyse the links in a KG (through its HDT file), follow the steps as below:

- Download HDT file and upload into _'HDTfiles'_ directory, e.g., from https://www.rdfhdt.org/datasets/
- Add authoritative namespace URIs (e.g., 'http://dbpedia.org/resource/') of the HDT dataset to 'authoritative_all.csv' file. They can be learned as of the algorithm implemented in [1]
- Run _'analyse_link_types'_. For Wikidata (and its non-RDFS/OWL semantics) run 'analyse_link_types_wikidata', which implements a mapping of the Wikidata semantics to RDFS/OWL as of [2]
- Statistics are written into several CSV files in the 'statistics' folder

An analysis of the link types of the LOD Cloud and Wikidata are published in [1] and [2], respectively

[1] Armin Haller, Javier D. Fernández, Maulik R. Kamdar, and Axel Polleres:
_What Are Links in Linked Open Data? A Characterization and Evaluation of Links between Knowledge Graphs on the Web_. ACM Journal of Data Information Quality 12(2): 9:1-9:34 (2020)

[2] Armin Haller, Axel Polleres, Daniil Dobriy, Nicolas Ferranti, and  Sergio José Rodríguez Méndez: _An Analysis of Links in Wikidata_. In Proceedings of the 19th International Conference on The Semantic Web, ESWC. (2022)