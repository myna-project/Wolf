# Wolf
Wolf is a lightweight and modular IoT gateway written in Python.
Wolf receives or retrieves data, depending on the protocol used, from various industrial electronic devices and then stores it into InfluxDB and/or sends it to other Information Systems through the cloud plugins.
Wolf uses Redis to implement caching and data persistence. Wolf also exposes REST services to get, set or update the configuration and to retrieve the measures data from InfluxDB, these REST services are used by [WolfUI](https://github.com/myna-project/WolfUI) to provide the web based graphical interface. 
Wolf design is lightweight to run low performance environments such as embedded systems.
### Supported plugins
Field plugins:
* AMQP JSON, XML
* EnOcean
* IO-Link IFM IoT JSON, Modbus
* Modbus RTU, TCP
* MQTT – JSON, XML or raw data format
* TP-Link Smart Plugs
Cloud plugins:
* AMQP
* InfluxDB
* [Togo HTTP/REST](https://github.com/myna-project/Togo) MQTT
* [Togo MQTT](https://github.com/myna-project/Togo) REST

### Installation requirements
Wolf is compatible with **python3** (developed and tested with **3.7**). With **2.7** it "might works".
Wolf requires the following python modules:
* bitstring
* bottle
* configparser
* dateutil
* influxdb
* jsonpath_rw
* lxml
* msgpack
* paho
* pika
* pyHS100
* pymodbus
* pytz
* pkg_resources
* redis
* requests
* schedule
* serial
* setuptools
* uritools
* wsgiproxy

For installation on Debian and derivative distros (eg. Ubuntu) you can use apt:

```
apt-get install redis-server
apt-get install python3-dateutil python3-msgpack python3-pkg-resources python3-redis python3-requests python3-schedule python3-serial python3-jsonpath-rw python3-pika python3-setuptools python3-bottle python3-lxml python3-tz python3-influxdb python3-paho-mqtt python3-bitstring python3-uritools python3-wsgiproxy
```

Otherwise you can use pip:

```
pip install dateutil lxml pyinotify requests schedule tzlocal
```

NB: Some modules aren't available as distro packages, so they can be installed from Github pakages. The autoupdate system integrated in Wolf will directly download/update requested packages (ala Golang). Anyway not all python modules are required to be installed at once: only modules used by enabled plugins are required (eg. if you don't use AMQP protocol you are not required to install pika).
Wolf relies on [systemd](https://github.com/systemd/systemd) to behave like a daemon and restart in case of failures. The systemd unit for Jackal is provided. Our reference distro is **Debian**, anyway the same results can be achieved with other service managers such as [supervisor](https://github.com/Supervisor/supervisor).

### Plugins development
Wolf architecture is modular and through for rapid development of new plugins. Wolf searches and loads plugins in the plugins directory and use them if they are configured in the ini configuration file.
There are field plugins that read field devices, cloud plugins that store/send data anywhere han been configured. There may be multiple instances of field plugins (eg. modbus_tcp.1, modbus_tcp.2, etc.).
Field plugins are classes with a **run()** method to launch one times tasks (eg. listening threads), **poll()** to periodically poll data from devices, **stop()** to stop any background thread started with run(), **write()** to write to field devices.
Cloud plugins are classes with a **post()** method to send data, **post_config()** to send configuration (ontology of configured field devices).
