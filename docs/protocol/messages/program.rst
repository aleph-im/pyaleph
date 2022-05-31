Programs
========

PROGRAM messages create a new application that can then be run on Aleph VMs.

Content format
--------------

The `content` field of a PROGRAM message must contain the following fields:

.. code-block:: json

    "code": {
      "encoding": "plain | zip | tar.gzip",
      "entrypoint": "application",
      "ref": "str",
      "ref": "str",
      "use_latest": true,
    },
    "on": {
      "http": true,
      "cron": "5 4 * * *",
      "aleph": [
        {"type": "POST", "channel": "FOUNDATION", "content": {"type": "calculation"}}
      ]
    },
    "environment":{
      "reproducible": true,
      "internet": false,
      "aleph_api": false
    },
    "resources": {
      "vcpus": 1,
      "memory": 128,
      "seconds": 1
    },
    "runtime": {
      "address": "0x4cB66fDf10971De5c7598072024FFd33482907a5",
      "comment": "Aleph Alpine Linux with Python 3.8"
    },
    "data": {
      "encoding": "tar.gzip",
      "mount": "/mnt",
      "address": "0xED9d5B040386F394B9ABd34fD59152756b126710"
    },
    "export": {
      "encoding": "tar.gzip",
      "mount": "/mnt"
    }
