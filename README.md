# Canister

An Erlang Session Management system

Work-in-progress, with the following goals:

* Be be the standard session management system for [Nitrogen](https://nitrogenproject.com)
* Data Persistence (currently using mnesia)
* Cluster data replication
* Automatic self-healing in the event of split brain.  The initial method will be naive: whichever session record was most recently updated will be replicated across nodes.
* Expired sessions automatically deleted accordingly
