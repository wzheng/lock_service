javac -sourcepath src -cp lib/jsonrpc2.jar:lib/json-smart.jar -d bin/ src/TPCTest.java
java -cp bin:lib/jsonrpc2.jar:lib/json-smart.jar TPCTest