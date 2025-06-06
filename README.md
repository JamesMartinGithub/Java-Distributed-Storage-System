# Java-Distributed-Storage-System
(2023) A distributed storage system written in java consisting of a client, controller, and dstores

The client communicates with the controller, which then directs the client to a chosen dstore for file uploading/retrieval.
The controller stores an index of files stored - for each file this includes:
- filename
- filesize (bytes)
- state
- which dstores contain it

Each dstore creates a folder in its directory for file storage using the name provided when starting the dstore

# Documentation:
- For how to run the system see `How_To_Run.txt`
- The message protocols can be found in `Protocol.java`

# Testing:
- When run with multiple unseen tests that involve multiple clients, failing dstores, and concurrent client requests, this system passed all tests

## Note:
- `ClientMain.java` and `client.jar` were provided for development testing and were not written by myself
