# Kopper
A toolkit for dealing with data on the wire.

## Why
Dealing with schema evolution is a pain. Kopper aims to make it easier by
providing a set of tools to help you manage your schema evolution and decouple
how data is used from how it is stored.

## Kopper Core
Kopper core introduces a new way of working with data in your application.
Instead of generating out code, it takes the approach of writing an interface
and seamlessly link it to the data underneath.

Kopper Core currently has a big focus on Avro, since that's what we are using
at KOR. Generating out SpecificRecord classes causes compatibility issues that
aren't easy to solve. But GenericRecord is a pain to work with because it simply
is too generic. We actually want to have a strong-typed interface to our data.

Dynamic Proxies allow us to define an interface and define what needs to happen
when a method is called. This is exactly what we want; the strong typed interface
translating to and from a GenericRecord.

Serializers and Deserializers are available to hook these dynamic records into
your kafka streaming logic.

## Kopper Cast
Kopper Cast takes schema evolution to the next level by defining what needs to
happen to evolve from one schema version to another. These so-called 'casters'
can then be used to read and write data in a compatible way, without the restrictions
resulting from using compatibility modes.

Kopper cast allows for schemas evolving in an incompatible way, having the casters
handle the incompatibility.

## Disclaimer
Kopper is not necessarily production-ready code (yet). It lays out ways to implement
conceptual patterns around schema evolution. Kopper is open to community
contributions and interactions, and can over time become hardened enough to
qualify for production use cases.
