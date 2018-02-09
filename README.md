# bravo
Extension of avro's ReflectData to provide customizable overrides for field accessors.

It gives you the ability to use any functions that you provide as getters/setters to use during avro encoding and/or decoding.
So, for example I can have a class like the below
```
Class TwoInts {
  int a;
  int b;
}
```
Lets now say that I then have an avro schema X that specifies that the TwoInts record holds three integer fields, 'a', 'b' and 'total'.
The BravoData class (extends avro's ReflectData) can then be instantiated and given a function that can get the value for the 'total' field given a TwoInts instance. I can then use this BravoData to create avro DatumWriter to perform avro encoding on TwoInts for my schema X.
A full example of this is at [RecordEncoding_WithFieldOverrides.java](src/main/java/powerdancer/bravo/example/RecordEncoding_WithFieldOverrides.java)

Field overrides can be provided as getters (for avro encoding) and setters (for avro decoding), and if you only care for encoding, you do not need to provide setters. Even fields which are already available in your POJO can be overridden as well if you like to do so.

More examples at [/src/main/java/powerdancer/bravo/example/](src/main/java/powerdancer/bravo/example/)
