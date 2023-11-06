package com.rockthejvm.part1streams;

import com.rockthejvm.gaming.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;


public class WindowFunctions {

  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  static Player alice = new Player(UUID.randomUUID(), "Alice");
  static Player bob = new Player(UUID.randomUUID(), "Bob");
  static Player charlie = new Player(UUID.randomUUID(), "Charlie");
  static Player diana = new Player(UUID.randomUUID(), "Diana");
  static Player emily = new Player(UUID.randomUUID(), "Emily");
  static Player fred = new Player(UUID.randomUUID(), "Fred");

  static Instant serverStartTime = Instant.parse("2023-11-06T00:00:00.000Z");

  static List<ServerEvent> events = List.of(
    bob.register(serverStartTime, Duration.ofSeconds(2)),
    bob.online(serverStartTime, Duration.ofSeconds(2)),
    diana.register(serverStartTime, Duration.ofSeconds(3)),
    diana.online(serverStartTime, Duration.ofSeconds(4)),
    emily.register(serverStartTime, Duration.ofSeconds(4)),
    alice.register(serverStartTime, Duration.ofSeconds(4)),
    fred.register(serverStartTime, Duration.ofSeconds(6)),
    fred.online(serverStartTime, Duration.ofSeconds(6)),
    charlie.register(serverStartTime, Duration.ofSeconds(8)),
    alice.online(serverStartTime, Duration.ofSeconds(10)),
    emily.online(serverStartTime, Duration.ofSeconds(10)),
    charlie.online(serverStartTime, Duration.ofSeconds(10))
  );

  // how many players were REGISTERED every 3 seconds?
  // [3s, 6s] - 3 players registered
  static DataStream<ServerEvent> eventStream = env
    .fromCollection(
      events,
      TypeInformation.of(ServerEvent.class)
    )
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .<ServerEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(
          new SerializableTimestampAssigner<ServerEvent>() {
            @Override
            public long extractTimestamp(ServerEvent serverEvent, long l) {
              return serverEvent.getEventTime().toEpochMilli();
            }
          }
        )
    );

  static void demoCountByWindow() throws Exception {
    AllWindowedStream<ServerEvent, TimeWindow> threeSecondWindow = eventStream.windowAll(
      TumblingEventTimeWindows.of(Time.seconds(3))
    );

    DataStream<String> registrationsEvery3s = threeSecondWindow.apply(
      new AllWindowFunction<ServerEvent, String, TimeWindow>() {
        //                  ^ input      ^ output  ^ window type
        @Override
        public void apply(TimeWindow window, Iterable<ServerEvent> values, Collector<String> out) throws Exception {
          int eventCount = 0;
          for (ServerEvent e : values) if (e instanceof PlayerRegistered)
            eventCount += 1;

          out.collect(
            "[" + window.getStart() + " - " + window.getEnd() + "] - " + eventCount + " players registered"
          );
        }
      }
    );

    registrationsEvery3s.print();
    env.execute();
  }

  public static void main(String[] args) throws Exception {
    demoCountByWindow();
  }
}
