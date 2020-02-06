# fpasperd
spark-submit --master local --driver-java-options '-Dlog4j.configuration=file:src\main\resources\log4j.properties' \
  --class FpasperdMain .\target\lgd-java-2.0.jar