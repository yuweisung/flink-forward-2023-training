package com.rockthejvm.playground;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Playground {
  /**
   * A test application. If this code compiles and runs, then you're good to go.
   * Feel free to experiment and play with Flink features as we progress with the training.
   * Enjoy!
   */
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Integer> simpleNumbers = env.fromElements(1,2,3,4);
    simpleNumbers.print();
    env.execute();
  }
}
