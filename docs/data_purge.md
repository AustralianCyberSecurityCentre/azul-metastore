# Data Purge / Deletion

To delete data from Azul, use the `purge` subcommand.

Please wait until processing for the binary and children of the binary has completed before initiating purge command. If you don't wait, some events flowing through the system may be missed.

The purge subcommand also supports purging a list of sha256 hashes from a newline delimited file `azul-metastore purge binaries list.txt`.

Example deletion of a single binary file (including all descendants).

```
azul@purge-5fcdb8ff5b-bdv6k:/purge$ azul-metastore purge binary 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry - starting prometheus metrics server started on port 8900
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - Searching for 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.assemblyline already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.incidents already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.reporting already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.samples already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.tasking already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.testing already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.virustotal already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary.vthunts already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - Alias azul.dev01.binary2.watch already exists in opensearch and is not being updated.
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - azul.dev01.binary template up to date
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - azul.dev01.status template up to date
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - azul.dev01.plugin template up to date
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - azul.dev01.annotation template up to date
INFO    2023-10-02 23:02:16+0000 - azul_metastore.common.wrapper - azul.dev01.cache template up to date
Found 3 docs
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - Gathering dispatcher information from documents
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - potential new hash to purge from datastore 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - sending 3 deletion events to dispatcher
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - deleting 3 docs from metastore
WARNING 2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - hashes to be checked for purging are located in /purge/purge-2023-10-02.txt
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8 unreferenced and will be deleted
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry_purge - 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8 deleted by dispatcher
INFO    2023-10-02 23:02:16+0000 - azul_metastore.entry - command finished
azul@purge-5fcdb8ff5b-bdv6k:/purge$ azul-metastore purge binary 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8
INFO    2023-10-02 23:03:34+0000 - azul_metastore.entry - starting prometheus metrics server started on port 8900
INFO    2023-10-02 23:03:34+0000 - azul_metastore.entry_purge - Searching for 6230f679e5434881d645b3dacb68db68ce886fd5a2a224e9aff4d66d9e177ac8
...
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 97adaa30c816b6362468fc621d258b83007bfa7e0c967a4186ce40678462ac81 unreferenced and will be deleted
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 97adaa30c816b6362468fc621d258b83007bfa7e0c967a4186ce40678462ac81 NOT found in dispatcher.
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 1a07370d8f527c1ff1478ef3681dc455e7c5ad2529c8ce7e99e159332eaeda53 unreferenced and will be deleted
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 1a07370d8f527c1ff1478ef3681dc455e7c5ad2529c8ce7e99e159332eaeda53 NOT found in dispatcher.
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 60e53a5f8dd2b447948de68f34d087d23ae446fe334e2bdf9aabbd989037d0d4 unreferenced and will be deleted
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry_purge - 60e53a5f8dd2b447948de68f34d087d23ae446fe334e2bdf9aabbd989037d0d4 NOT found in dispatcher.
INFO    2023-10-02 23:03:37+0000 - azul_metastore.entry - command finished
```
