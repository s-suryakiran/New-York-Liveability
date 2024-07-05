javac -classpath `hadoop classpath` *.java
jar cvf aggregateZips.jar AggregateZips.class AggregateZipsMapper.class AggregateZipsReducer.class

