# Bravo

**Bravo** [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.king.bravo/bravo/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.king.bravo/bravo)

```
<dependency>
  <groupId>com.king.bravo</groupId>
  <artifactId>bravo</artifactId>
  <version>LATEST</version>
</dependency>
```

**Bravo Test Utils** [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.king.bravo/bravo-test-utils/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.king.bravo/bravo-test-utils)

```
<dependency>
  <groupId>com.king.bravo</groupId>
  <artifactId>bravo-test-utils</artifactId>
  <version>LATEST</version>
  <scope>test</scope>
</dependency>
```

*Note: The Flink dependencies of Bravo are declared compileOnly (provided in Maven world) so that it wont conflict with your own Flink version. This means that we assume that you already have that dependencies in your project*

## Introduction

Bravo is a convenient state reader and writer library leveraging the Flink’s
batch processing capabilities. It supports processing and writing Flink streaming snapshots.
At the moment it only supports processing RocksDB snapshots but this can be extended in the future for other state backends.

Our goal is to cover a few basic features:
 - Converting keyed states to Flink DataSets for processing and analytics
 - Reading/Writing non-keyed operators states
 - Bootstrap keyed states from Flink DataSets and create new valid savepoints
 - Transform existing savepoints by replacing/changing/creating states

Some example use-cases:
 - Point-in-time state analytics across all operators and keys
 - Bootstrap state of a streaming job from external resources such as reading from database/filesystem
 - Validate and potentially repair corrupted state of a streaming job
 - Change max parallelism of a job

## Disclaimer

This is more of a proof of concept implementation, not necessarily something production ready.

Who am I to tell you what code to run in prod, I have to agree, but please double check the code you are planning to use :)

## Building Bravo

In order to build Bravo locally you only need to use the gradle wrapper already included in the project

```bash
cd bravo

# Build project and run all tests
./gradlew clean build

# Build project and publish to local maven repo
./gradlew clean install
```


## Reading states

### Reading and processing states

The `OperatorStateReader` provides DataSet input format that understands RocksDB savepoints and checkpoints and can extract keyed state rows from it. The input format creates input splits by operator subtask of the savepoint at the moment but we can change this to split by keygroups directly.

The reader can also be used to provide in-memory [access to non-keyed states](#accessing-non-keyed-states).

For example, this code snippet shows how to read keys & values of a _keyed value state_:

```java
// First we start by taking a savepoint/checkpoint of our running job...
// Now it's time to load the metadata
Savepoint savepoint = StateMetadataUtils.loadSavepoint(savepointPath);

ExecutionEnvironment env = ExecutionEnvironment.getEnvironment();

// We create a KeyedStateReader for accessing the state of the operator with the UID "CountPerKey"
OperatorStateReader reader = new OperatorStateReader(env, savepoint, "CountPerKey");

// The reader now has access to all keyed states of the "CountPerKey" operator
// We are going to read one specific value state named "Count"
// The DataSet contains the key-value tuples from our state
DataSet<Tuple2<Integer, Integer>> countState = reader.readKeyedStates(
		KeyedStateReader.forValueStateKVPairs("Count", new TypeHint<Tuple2<Integer, Integer>>() {}));

// We can now work with the countState dataset and analyze it however we want :)
```

The `KeyedStateReader` class provides a set of methods for creating readers for different types of keyed states.

Some examples:

```
KeyedStateReader.forValueStateKVPairs(...)
KeyedStateReader.forValueStateValues(...)
KeyedStateReader.forMapStateEntries(...)
KeyedStateReader.forListStates(...)
KeyedStateReader.forMapStateValues(...)
KeyedStateReader.forReducerStateValues(...)
```

For more complete code examples on the usage of the specific readers please look at some of the test cases, they are actually quite nice:

https://github.com/king/bravo/blob/master/bravo/src/test/java/com/king/bravo/

#### Accessing non-keyed states

The reader assumes that the machine that we run the code has enough memory to restore the non-keyed states locally. (This is mostly a safe assumption with the current operator state design)

```java

// We restore the OperatorStateBackend in memory
OperatorStateBackend stateBackend = reader.createOperatorStateBackendFromSnapshot(0);

// Now we can access the state just like from the function
stateBackend.getListState(...)
stateBackend.getBroadcastState(...)

```
## Creating new savepoints

### OperatorStateWriter

As the name suggests the `OperatorStateWriter` class provides utilities to change (replace/transform) the state for a single operator. Once we have a new valid operator state we will use some utility methods to create a new Savepoint.

Let's continue our reading example by modifying the state of the all the users, then creating a new valid savepoint.
```java
DataSet<Tuple2<Integer, Integer>> countState = //see above example

// We want to change our state based on some external data...
DataSet<Tuple2<Integer, Integer>> countsToAdd = environment.fromElements(
        Tuple2.of(0, 100), Tuple2.of(3, 1000),
        Tuple2.of(1, 100), Tuple2.of(2, 1000));

// These are the new count states we want to put back in the state
DataSet<Tuple2<Integer, Integer>> newCounts = countState
        .join(countsToAdd)
        .where(0)
        .equalTo(0)
		.map(new SumValues());

// We create a statetransformer that will store new checkpoint state under the newCheckpointDir base directory
OperatorStateWriter writer = new OperatorStateWriter(savepoint, "CountPerKey",  newCheckpointDir);

writer.addValueState("Count", newCounts);

// Once we are happy with all the modifications it's time to write the states to the persistent store
OperatorState newOpState = writer.writeAll();

// Last thing we do is create a new meta file that points to a valid savepoint
StateMetadataUtils.writeSavepointMetadata(newCheckpointDir, StateMetadataUtils.createNewSavepoint(savepoint, newOpState));
```

We can also use the StateTransformer to transform and replace non-keyed states:

```java
writer.transformNonKeyedState(BiConsumer<Integer, OperatorStateBackend> transformer);
```

## Contact us!

The original King authors of this code:
 - David Artiga (david.artiga@king.com)
 - Gyula Fora (gyula.fora@king.com)

With any questions or suggestions feel free to reach out to us in email anytime!
