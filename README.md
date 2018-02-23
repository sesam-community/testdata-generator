# testdata-generator
Simple tool to generate testdata from an existing Sesam node and transfer the config to a target node

# Configuration

This should be put into the master node, not the target node as it will be overwritten if "FULL_SYNC" is set to
"true". Also note that "memory" might have to be increased if there is a large number of pipes and/or big entities
as the MS gathers the whole config in memory while running:

::

    {
      "_id": "testdata-microservice",
      "type": "system:microservice",
      "name": "Create testdata",
      "docker": {
        "environment": {
          "FULL_SYNC": "false",
          "MASTER_NODE": {
            "_id": "m1",
            "endpoint": "https://master-node.sesam.cloud",
            "jwt_token": "eyJ0eXAiOiJKV1QiLCJhbGci.."
          },
          "TARGET_NODE": {
            "_id": "t1",
            "endpoint": "https://target-node.sesam.cloud",
            "jwt_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
          }
        },
        "image": "sesam/testdata-generator:latest",
        "memory": 4096,
        "port": 5000,
      }
    }


The MS may take a long time to finish and update the target node.
