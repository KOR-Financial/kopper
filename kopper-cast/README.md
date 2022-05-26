# Kopper Cast
Kopper cast provides generic upcasting capabilities for data read from any
transport. It has specific extensions for reading data from kafka topics and
casting them, allowing incompatible schema changes.

## Why
Through schema registry we are able to check if a new schema is compatible
with a previously defined one. Depending on the compatibility level, there
are constraints on what kind of changes may be done evolving from one
schema version to the next. For example, if you want to be able to keep
reading messages since the beginning of time, the only way to do so is to make
your schema more tolerant over time. Instead of relying on a single data type
for a field, you instead would allow multiple incompatible data types (using
a union in avro for example). At the same time, you are only allowed to add
optional fields to your schema, not required ones since that would break with
regard to previous data records that do not have that field.

Most of our strictly typed programming languages are not capable of dealing
with multiple data types for a single field. Therefor the most 'loose' data
type (Object) is chosen and the responsibility is left to the developer to
deal with it. So while we are able to _read_ the data, we deal with the
complexity while _processing_ since we need to make sure our code shares the
same compatibility level as the one enforced onto the data.

Instead, we actually want to be able to use the latest schema only, and have
our code rely on the data being compatible with that latest schema only. We
also want to be able to make breaking changes in schema evolution in order to
keep our schemas lean.

# How

> Originally a concept of object-oriented programming, where "a subclass gets
> cast to its superclass automatically when needed" - AxonIQ

Upcasters take an input data fragment and transform it into a different data
fragment. This resulting data fragment might look totally different from the
original one.

Kopper Cast has been written with several layers in mind. It provides a
*low-level* api, not bound to anything except plain java to allow you to use
the idea of upcasters in any setting. More specific api's for writing
upcasters can be found in the *high-level* api, allowing for upcasters to be
created in a declarative manner as well as providing an upcaster registry
for upcasters to be registered and retrieved from.

## Low-Level API
The `Upcaster` class will come to no surprise. The upcaster's focus is on
ensuring the input data is transformed into output data conform to the target
version.

A simple upcaster for upcasting `java.util.Properties` might look like this:

```java
class PropertiesUpcasterV1 implements Upcaster<Properties, Integer> {
    @Override
    public Integer getTargetVersion() { return 1; }

    @Override
    public Properties upcast(Properties input, Integer inputVersion) {
        Properties output = input;

        // your logic here

        return output;
    }
}
```

An upcaster will rarely be used on its own, but takes part in a chain of
upcasters, each to be invoked in sequence. Each upcaster deals with lifting
the incoming data one version up, eventually leading to the data being
compatible with the latest version. The `UpcasterChain` will keep track of
the root `UpcasterChainNode` of the chain; the first upcaster in the sequence
of upcasters. Each `UpcasterChainNode` references the next upcaster in line.

The following snippet creates a chain and registers a few upcasters to it:

```java
public class MyApp {
    // ...

    protected UpcasterChain<Properties, Integer> createChain() {
        return UpcasterChain.<Properties, Integer>builder()
                .register(new PropertiesUpcasterV1())
                .register(new PropertiesUpcasterV2())
                // ...
                .register(new PropertiesUpcasterV99())
                .build();
    }

    // ...
}
```

Once the chain has been constructed, we can call its `doUpcast()` method
providing our input data:

```java
public class MyApp {
    // ...

    public void run() {
        Properties data = new Properties();
        data.setProperty("id", "ID");

        VersionedItem<Properties, Integer> result = chain.doUpcast(new VersionedItem<>(data, 1));

        // do something with the result
    }

    // ...
}
```

Given the chain defined above, the version of the result will now
probably be `99` given `PropertiesUpcasterV99` being the last upcaster to be
invoked.

## High-Level API
The high-level api provides upcasters for specific wire formats. It also allows
for declarative upcasters to be defined. This breed of upcasters opens the door
to upcasters being registered in a central registry and retrieved, much like
schemas in schema registry.

### Declarative Upcasters
The whole point of an upcaster is to contain logic to take input from one
version to the next. The upcaster has specific logic for doing so, but this
also means we need to redeploy our application each time an upcaster is added
to the chain.

Declarative Upcasters use an `Evaluator` for performing that logic. It defines
the expression to execute depending on the field in the target version. During
execution, the declarative upcaster will run through all target fields,
checking if there is an expression to be evaluated to reach to the target
field's value. If not, it will try to use the input field's data instead.

It is possible to write your own `Evaluator` and wire it in. Right now, the
Spring Expression Language (SpEL) is being used. More information on how to
write expressions for SpEL can be found in the [SpEL reference guide](https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions)

### Avro
Avro specific upcasters are centered around a `GenericRecord` since we need
to be able to read in any kind of data that adheres to the schema.

#### Explicit Avro Upcaster
Writing an explicit Avro upcaster requires us to deal with `GenericRecord`
directly, reading field values from the input, casting them, and writing
them to the `GenericRecordBuilder`:

```java
public class MyClass {
    private AvroUpcaster<Integer> upcasterV4() {
        return new AvroUpcaster<>(Schemas.SCHEMA_V4, 4) {
            @Override
            public GenericRecordBuilder upcast(GenericRecordBuilder builder, GenericRecord input) {
                // -- check if the name was set
                if (input.hasField("name")) {
                    builder.set("firstname", "");
                    builder.set("lastname", "");

                    Object originalValue = input.get("name");
                    if (originalValue instanceof String s) {
                        String[] parts = s.split("\\s", 2);

                        if (parts.length >= 1) {
                            builder.set("firstname", parts[0]);
                        }

                        if (parts.length >= 2) {
                            builder.set("lastname", parts[1]);
                        }
                    }
                }

                return builder;
            }
        };
    }
}
```

#### Declarative Avro Upcaster
The declarative Avro upcaster requires us to define the expressions to be
executed. An expression can be defined for each field on the **target** schema.

The following variables are available when evaluating expressions:
- `#input`: the input `GenericRecord`
- `#schema`: the target Schema

```java
class MyClass {
    private DeclarativeAvroUpcaster<Integer> upcasterV4() {
        return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V4, 4)
                .withExpression("firstname", "#input.name?.split('\\s', 2)[0] ?: ''")
                .withExpression("lastname", "#input.name?.split('\\s', 2)[1] ?: ''")
                .build();
    }
}
```

The expressions are just that - expressions. It is not a full programming
language, which might be limiting for some upcasting cases. Functions can be
passed into the expression engine to support additional functionality:

```java
class MyClass {
    private DeclarativeAvroUpcaster<Integer> upcasterV3() throws NoSuchMethodException {
        return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V3, 3)
                .withExpression("age", "#input.age != null ? #parseInt(#input.age) : null")
                .withVariable("parseInt", Integer.class.getDeclaredMethod("parseInt", String.class)).build();
    }

    private DeclarativeAvroUpcaster<Integer> upcasterV5() throws NoSuchMethodException {
        return DeclarativeAvroUpcaster.builder(Schemas.SCHEMA_V5, 5).withExpression("name",
                        "#asRecord(#schema.getField('name').schema(), {firstname: #input.firstname, lastname: #input.lastname})")
                .withVariable("asRecord", AvroHelpers.class.getDeclaredMethod("asRecord", Schema.class, Map.class))
                .build();
    }
}
```

The first upcaster (`upcasterV3`) registers `Integer.parseInt()` as the
`#parseInt` function within the expression language. This allows us to use
`#parseInt(...)` to cast a string into an integer.

The second upcaster (`upcasterV5`) registers `AvroHelpers.asRecord()` as the
`#asRecord()` function to create new GenericRecords.

#### Serde
The `UpcastingDeserializer` allows for upcasting to happen as part of the
deserialization process of consumers. Using `CastSerdes.upcastingSerde()`, a
`Serde<GenericRecord>` can be retrieve that will serialize generic records and
deserialize generic records using upcasting. Pass in the `UpcasterRegistry`
for the deserializer to be able to find the right chain.

The `UpcastingDeserializer` also integrates with SchemaRegistry, allowing
schema versions to be retrieved directly from a SchemaRegistry instance.

## Upcaster Registry
Similar to the Kafka's schema registry, the upcaster registry allows chains to
be registered and retrieved.

### In Memory Upcaster Registry
The `InMemoryUpcasterRegistry` is the most straightforward implementation of
an `UpcasterRegistry` internally using a simple `Map` to collect the
`UpcasterChain`s. This is particularly interesting during testing.
