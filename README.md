# columnstore_transfer_with_spider
Transfer massive Columnstore tables from one instance to another using Spider engine.
Scripts by Edward Stoever for MariaDB Support. Ref: CS0514319

Spider engine allows you to define a table on one database as a query on a remote database. With this, you can copy data from one instance (source) to another (target). Columnstore tables frequently contain millions or billions of rows. Use these scripts to help you to slice a very large table into smaller chunks, allowing you to keep an eye on the progress of the process and prevent failure.

Originally written for use with SkySQL instances. However, can also be used for on-premise instances. Can also be used for tables of any supported MariaDB engine.

