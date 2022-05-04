*****************
Garbage collector
*****************

Core Channel Nodes dispose of unneeded files through a process called garbage collection.
Two kinds of garbage collection are in place, one for the local storage system and
one for the IPFS service.

Local storage
=============

CCNs have a dedicated process to dispose of files, the garbage collector.
This process monitors the files on the local storage and deletes them once
they are scheduled for deletion.

Files can be scheduled for deletion for a number of reasons:
- They were temporary files that ended up being unused by the user that pushed them
- The user decided to delete them
- The payment plan of the user no longer covered for them.

In any of these situations, a date and time of deletion is assigned to the file.
The garbage collector runs periodically and simply deletes the files for which
this date and time is passed.

By default, the garbage collector runs once every hour. Temporary files uploaded
using the /storage/add_[json|file] endpoints are given a lifetime of one hour
before deletion.

IPFS
====

The IPFS daemon has its own garbage collector process. You can read more about it
in their `official documentation <https://docs.ipfs.io/concepts/persistence/#garbage-collection>`_.
