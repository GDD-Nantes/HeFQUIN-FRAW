# HeFQUIN WITH FEDUP

1) git clone git@github.com:GDD-Nantes/HeFQUIN-FRAW.git
2) git clone https://github.com/GDD-Nantes/fedup
3) cd fedup
4) sed -i '' -e 's/<jena.version>5.0.0<\\\/jena.version>/<jena.version>4.10.0<\\\/jena.version>/g' pom.xml
5) sed -i '' -e 's/.enableCors(true, "")/.enableCors(true)/g' src/main/java/fr/gdd/fedup/cli/FedUPServer.java
6) mvn clean package install -Dmaven.test.skip -f pom.xml
7) cd ..
8) cd HeFQUIN-FRAW
9) git checkout hefquin_with_fedup
10) mvn clean package install -Dmaven.test.skip -f pom.xml
11) wget https://zenodo.org/records/11933972/files/fedshop20-h0.zip
12) wget https://zenodo.org/records/11933972/files/fedshop200-h0.zip
13) unzip fedshop20-h0.zip -d summaries
14) unzip fedshop200-h0.zip -d summaries
15) rm fedshop20*-h0.zip
16) sed -i '' -e 's/ENDPOINT_URL_PLACEHOLDER/[YOUR_ENDPOINT_URL]/g' fedshop200.ttl
17) ./bin/hefquin --federationDescription fedshop200.ttl --confDescr DefaultEngineWithFedupConfForFedshop20.ttl --file query.sparql

Notes : 
- IMPORTANT : N'oubliez pas de lancer Virtuoso ou votre endpoint de choix avant d'éxecuter une requêtes avec HeFQUIN. Sinon, ça marche moins bien.
- Un exemple de fédération déjà entièrement déclarée est présente dans fedshop200bg.ttl (conçue pour une utilisation de fedup et d'endpoints virtuels blazegraph).
- Pour utiliser fedshop200 au lieu de fedshop20, il faut changer la configuration de hefquin utilisée ("DefaultEngineWithFedupConfForFedshop20") dans la commande ci-dessus.
- Pour utiliser d'autres configurations de fedshop (40, 60, 80, etc.) il faut générer / télécharger les summaries correspondant et les mettre dans le dossier "summaries", et créer le fichier de configuration de hefquin correspondant.





# HeFQUIN
HeFQUIN is a query federation engine for heterogeneous federations of graph data sources (e.g, federated knowledge graphs) that is currently under development by [the Semantic Web research group at Linköping University](https://www.ida.liu.se/research/semanticweb/).

### Features of HeFQUIN
* Support for all features of SPARQL 1.1 (where basic graph patterns, group graph patterns (AND), union graph patterns, optional patterns, and filters are supported natively within the HeFQUIN engine, and the other features of SPARQL are supported through integration of the HeFQUIN engine into Apache Jena)
* So far, support for SPARQL endpoints, TPF, and brTPF
  * [work on openCypher Property Graphs ongoing](https://github.com/LiUSemWeb/HeFQUIN/tree/main/src/main/java/se/liu/ida/hefquin/engine/wrappers/lpgwrapper)
  * [work on GraphQL APIs ongoing](https://github.com/LiUSemWeb/HeFQUIN/tree/main/src/main/java/se/liu/ida/hefquin/engine/wrappers/graphqlwrapper)
* Initial support for vocabulary mappings
* [Heuristics-based logical query optimizer](https://github.com/LiUSemWeb/HeFQUIN/wiki/Heuristics-Based-Logical-Query-Optimizer)
* Several different [cost-based physical optimizers](https://github.com/LiUSemWeb/HeFQUIN/wiki/Cost-Based-Physical-Query-Optimizers) (greedy, dynamic programming, simulated annealing, randomized iterative improvement)
* Relevant [physical operators](https://github.com/LiUSemWeb/HeFQUIN/wiki/Physical-Operators); e.g., hash join, symmetric hash join (SHJ), request-based nested-loops join (NLJ), several variations of bind joins (brTPF-based, UNION-based, FILTER-based, VALUES-based)
* Two execution models (push-based and pull-based)
* Features for getting an understanding of the internals of the engine
  * printing of logical and physical plans
  * programmatic access to execution statistics on the level of individual operators and data structures, as well as printing of these statistics from the CLI
* 380+ unit tests

### Current Limitations
* HeFQUIN does not yet have a source selection component. All subpatterns of the queries given to HeFQUIN need to be wrapped in SERVICE clauses.

### Publications related to HeFQUIN
* Sijin Cheng and Olaf Hartig: **[FedQPL: A Language for Logical Query Plans over Heterogeneous Federations of RDF Data Sources](https://olafhartig.de/files/ChengHartig_FedQPL_iiWAS2020_Extended.pdf)**. In _Proceedings of the 22nd International Conference on Information Integration and Web-based Applications & Services (iiWAS)_, 2020.
* Sijin Cheng and Olaf Hartig: **[Source Selection for SPARQL Endpoints: Fit for Heterogeneous Federations of RDF Data Sources?](https://olafhartig.de/files/ChengHartig_QuWeDa2022.pdf)**. In _Proceedings of the 6th Workshop on Storing, Querying and Benchmarking Knowledge Graphs (QuWeDa)_, 2022.
* Sijin Cheng and Olaf Hartig: **[A Cost Model to Optimize Queries over Heterogeneous Federations of RDF Data Sources](https://olafhartig.de/files/ChengHartig_CostModel_DMKG2023.pdf)**. In _Proceedings of the 1st International Workshop on Data Management for Knowledge Graphs (DMKG)_, 2023.
  * repo related to the experiments in this paper: [LiUSemWeb/HeFQUIN-DMKG2023-Experiments](https://github.com/LiUSemWeb/HeFQUIN-DMKG2023-Experiments)
* Sijin Cheng, Sebastian Ferrada, and Olaf Hartig: **[Considering Vocabulary Mappings in Query Plans for Federations of RDF Data Sources](https://olafhartig.de/files/ChengEtAL_VocabMappings_CoopIS2023.pdf)**. In _Proceedings of the 29th International Conference on Cooperative Information Systems (CoopIS)_, 2023.
  * repo related to the experiments in this paper: [LiUSemWeb/HeFQUIN-VocabMappingsExperiments](https://github.com/LiUSemWeb/HeFQUIN-VocabMappingsExperiments)
