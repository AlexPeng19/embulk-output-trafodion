# Trafodion output plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 3306)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **table**: destination table name (string, required)

## Example

```yaml
out:
type: trafodion
host: localhost 
user: my_user 
password: my_passwd
database: my_database
schema: schema_name
table: table_name
mode: insert
```


## Build

```
$ ./gradlew gem 
```
