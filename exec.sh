javac -sourcepath src -cp lib/jsonrpc2.jar:lib/json-smart.jar -d bin/ src/$1.java
java -cp bin:lib/jsonrpc2.jar:lib/json-smart.jar $1