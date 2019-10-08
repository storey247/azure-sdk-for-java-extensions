# azure-sdk-for-java-extensions

Using the Java SDK for Azure? This library consists of handy extensions and helpers that I've built up along the way based on my experience with the codebase.

# Dependencies

## Maven

```xml
<dependency>
  <groupId>com.github.storey247/azure-sdk-for-java-extensions</groupId>
  <artifactId>com.storey247.azure-sdk-for-java-extensions</artifactId>
  <version>0.1.0-snaphot</version>
</dependency>
```

# Getting Started

## ServiceBus

- AutoRenewMessageReceiver

### AutoRenewMessageReceiver
Wrapper class over MessageReceiver that automatically handles message lock renewal for you

```java
    ConnectionStringBuilder builder = new ConnectionStringBuilder(namespaceConnectionString, endpoint);
    IMessageReceiver receiver = new AutoRenewMessageReceiver(builder);
    IMessage message = receiver.receiveAsync();

    // do work and don't worry about the lock timer, we got this!
```
