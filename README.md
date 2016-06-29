# WebSocket plugin for SensorBee

The WebSocket plugin for SensorBee allows to receive data from remote SensorBee instances.

SensorBee will probably support remote query in the future, but it isn't provided at the moment.
This plugin helps to build distributed SensorBee topologies until the feature gets ready.

## `websocket` Source

### Required parameters

#### `topology`

`topology` is the name of the topology which have the target stream.

#### `stream`

`stream` is the name of the stream to issue queries.

### Optional parameters

#### `host`

`host` is the name of the host which runs the target SensorBee. The default
value is `localhost`.

#### `port`

`port` is the port number of the target SensorBee. The default value is 15601.

#### `buffer_size`

`buffer_size` is the integervalue passed to the `BUFFER SIZE` clause in BQL.
The default value is 1024.

#### `drop_mode`

`drop_mode` specifies the value to control the behavior of the buffer when it's full.
It has following three values:

* `"wait"`: `WAIT IF FULL` will be specified in BQL
* `"newest"`: `DROP NEWEST IF FULL`
* `"oldest"`: `DROP OLDEST IF FULL`

These values are case-sensitive. The default value is "wait".

### Example

```
CREATE SOURCE remote TYPE websocket WITH
  topology = "my_topology",
  stream = "my_stream",
  host = "anotherhost",
  port = 15601,
  buffer_size = 1024,
  drop_mode = "wait";
```

This source issues a query equivalent to the following command:

```
$ bql --uri http://anotherhost:15601/ -t my_topology
my_topology> SELECT RSTREAM * FROM my_stream [RANGE 1 TUPLES, BUFFER SIZE 1024, WAIT IF FULL];

```

## Registering plugin

Add `gopkg.in/sensorbee/websocket.v1/plugin` to build.yaml of the `build_sensorbee` command.

```yaml
plugins:
  - gopkg.in/sensorbee/websocket.v1/plugin
```
