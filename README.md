# Trafodion output plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
out:
type: trafodion
host: 10.10.10.8
user: trafodion
password: traf123
database: trafodion
schema: alex
table: EMBULK_TEST
mode: insert_direct
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
