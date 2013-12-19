# meshy

## TCP multiplexing gearwheels

`meshy` is a java library for creating clustered services over
multiplexed tcp connections.  [Netty](http://netty.io/) is used to
handle all the gritty networking bits and UDP broadcast for service
discovery.

Services are typically hierarchical -- think file systems or URLs --
and file services and RPC are the two most common use cases.

Nodes form a mesh once they have discovered the local network
topology.  Each mesh node is capable of handling any request that the
mesh can handle by proxying to an appropriate node.

A typical use case is for each physical node to have one mesh node,
and for local processes to talk to their local mesh node.  Multiple
meshes can be overlayed on the same physical hosts by using unique
ports or "secret" keys.

## Building

`meshy` uses [Apache Maven](http://maven.apache.org/) which it is beyond
the scope to detail.  The super simple quick start is:

`mvn test`

## Use

```xml
<dependency>
  <groupId>com.addthis</groupId>
  <artifactId>meshy</artifactId>
  <version>latest-and-greatest</version>
</dependency>
```

You can either install locally, or releases will eventually make their
way to maven central.



## Administrative

### Versioning

It's x.y.z where:

 * x: something major happened
 * y: next release
 * z: bug fix only

### License

meshy is released under the Apache License Version 2.0.  See
[Apache](http://www.apache.org/licenses/LICENSE-2.0) or the LICENSE
for details.