# HeFQUIN-FRAW
HeFQUIN-FRAW allows to execute random walks across federations of SPARQL endpoints that also support random walks,
like endpoints running [RAW x Passage](https://github.com/passage-org/passage).

HeFQUIN-FRAW is an extension of HeFQUIN.
This repository started as a clone of the [HeFQUIN github repository](https://github.com/LiUSemWeb/HeFQUIN).
Below, you can find information regarding the HeFQUIN base engine.

# HeFQUIN
HeFQUIN is a **query federation engine for heterogeneous federations of graph data sources (e.g, federated knowledge graphs)** that is currently under development by [the Semantic Web research group at Linköping University](https://www.ida.liu.se/research/semanticweb/).

For detailed information about HeFQUIN, refer to the **Website at [https://liusemweb.github.io/](https://liusemweb.github.io/)**, where you can find
* a list of the [features of HeFQUIN](https://liusemweb.github.io/HeFQUIN/doc/features.html),
* a detailed [user documentation](https://liusemweb.github.io/HeFQUIN/doc/index.html),
* a list of [related research publications](https://liusemweb.github.io/HeFQUIN/research),
* information [for contributors](https://liusemweb.github.io/HeFQUIN/devdoc),
  and more.

## Quick Guide
This version of HeFQUIN-FRAW relies on [FedUP](https://github.com/GDD-Nantes/fedup) for the source selection step.
You need to install FedUP beforehand.
***
### Using HeFQUIN-FRAW as a Service
* **_Starting the service using the command line program_**
  * Clone the hefquin-fraw repository and checkout the "provenance" branch.
    ```bash
      git clone git@github.com:GDD-Nantes/HeFQUIN-FRAW.git
      cd HeFQUIN-FRAW
      git pull
    ```

  * Generate the JAR file
    ```bash
      mvn install -Dmaven.test.skip -f pom.xml
    ```

  * Start the server using the hefquin-server binary.
    ```bash
      ./bin/hefquin-server \
        --port=8080 \
        --path="" \
        --federationDescription fedshop200.ttl \
        --confDescr ExampleEngineConf.ttl \
        --frawConfDescr f200_UOJ.ttl
    ```
    where
    * the `--port` argument specifies the port at which the service shall listen and
    * the `--path` argument specifies the custom URL path serving the different endpoints
    * the `--federationDescription` argument refers to a file (e.g., `fedshop200.ttl`) that contains an [RDF-based description of your federation](https://liusemweb.github.io/HeFQUIN/doc/federation_description.html).
    * the `--confDescr` argument refers to a file (e.g., `f200_eval.ttl`) that contains an RDF-based description of the engine used to process queries sent to the SPARQL endpoint.
    * the `--frawConfDescr` argument refers to a file (e.g., `f200_UOJ.ttl`) that contains an RDF-based description of the engine used to process queries sent to the sampling endpoint.

    This configuration assumes a federation of 200 endpoints as described in [FedShop](https://github.com/GDD-Nantes/FedShop) that all offer to process queries with random walks.
    The easiest way to set up such a federation is using [RAW x Passage](https://github.com/passage-org/passage).

* **_Interacting with the HeFQUIN-FRAW Service_**
  * After starting up the HeFQUIN-FRAW service, you can first test it test by opening [`http://localhost:8080/`](http://localhost:8080/) in a Web browser (assuming that you have started the service at port 8080 and without specifying a custom path).
  * You can interact with the service like with a SPARQL endpoint (the endpoint should be exposed at `http://localhost:8080/sparql`). For instance, by using the command-line tool [`curl`](https://curl.se/), you may execute the following command to issue the query in a file called `ExampleQuery.rq`.
    ```bash
    curl -X POST http://localhost:8080/sparql --data-binary @query.sparql -H 'Content-Type: application/sparql-query'
    ```
  * Our [documentation page about interacting with a HeFQUIN service](https://liusemweb.github.io/HeFQUIN/doc/hefquin_service.html) provides more details.
  * Moreover, you can read more about the [queries and query features that you can use](https://liusemweb.github.io/HeFQUIN/doc/queries.html).