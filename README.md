A Simple Blackboard Architecture
================================

This repository contains a simple, REST-based blackboard architecture
implementation. This implementation provides some basic plumbing for a
blackboard and an example knowledge source that interacts with it. 

```
./activator run
```

If all goes well, then try the following:

```
curl --data-ascii @asthma.json -H 'Content-Type:application/json' http://localhost:9000/blackboard/api
```

where ```asthma.json``` is a file containing the following payload:

```
{
    "type": "query",
    "name": "A simple blackboard example",
    "term": "asthma"
}
```

Now try

```
curl http://localhost:9000/blackboard/api
```

There should be a new knowledge graph created with a single query node. To
update this knowledge graph using the [Pharos](https://pharos.nih.gov)
knowledge source we can use the following call:

```
curl -X PUT http://localhost:9000/blackboard/api/1/ks.pharos
```

If all goes well, now rerun

```
curl http://localhost:9000/blackboard/api
```

to see that the knowledge graph has been updated with new nodes contributed by
the [Pharos](https://pharos.nih.gov) knowledge source.
