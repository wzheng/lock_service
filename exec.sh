javac -sourcepath src -cp lib/jsonrpc2.jar:lib/json-smart.jar -d bin/ src/main/$1.java
java -cp bin:lib/jsonrpc2.jar:lib/json-smart.jar main.$1