package com.rockthejvm.part1streams;

import com.rockthejvm.gaming.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
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
    bob.offline(serverStartTime, Duration.ofSeconds(6)),
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

  static class CountByWindowAll implements AllWindowFunction<ServerEvent, String, TimeWindow> {
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
  static void demoCountByWindow() throws Exception {
    AllWindowedStream<ServerEvent, TimeWindow> threeSecondWindow = eventStream.windowAll(
      TumblingEventTimeWindows.of(Time.seconds(3))
    );

    DataStream<String> registrationsEvery3s = threeSecondWindow.apply(
      new CountByWindowAll()
    );

    registrationsEvery3s.print();
    env.execute();
  }


  static void demoCountByWindow_v2() {
    AllWindowedStream<ServerEvent, TimeWindow> threeSecondWindow = eventStream.windowAll(
      TumblingEventTimeWindows.of(Time.seconds(3))
    );

    DataStream<String> registrationsEvery3s = threeSecondWindow.process(
      new ProcessAllWindowFunction<ServerEvent, String, TimeWindow>() {
        @Override
        public void process(
          ProcessAllWindowFunction<ServerEvent, String, TimeWindow>.Context context,
          Iterable<ServerEvent> values,
          Collector<String> out
        ) throws Exception {
          // process function gives you access to more than just the window
          // important: can use state (TBD)
          TimeWindow window = context.window();
          int eventCount = 0;
          for (ServerEvent e : values) if (e instanceof PlayerRegistered)
            eventCount += 1;

          out.collect(
            "[" + window.getStart() + " - " + window.getEnd() + "] - " + eventCount + " players registered"
          );
        }
      }
    );
  }

  static void demoCountByWindow_v3() throws Exception {
    AllWindowedStream<ServerEvent, TimeWindow> threeSecondWindow = eventStream.windowAll(
      TumblingEventTimeWindows.of(Time.seconds(3))
    );

    // downside: I can't access the window
    DataStream<Long> registrationsEvery3s = threeSecondWindow.aggregate(
      new AggregateFunction<ServerEvent, Long, Long>() {

        @Override
        public Long createAccumulator() {
          return 0L;
        }

        @Override
        public Long add(ServerEvent event, Long currentAcc) {
          // the logic stays here
          if (event instanceof PlayerRegistered)
            return currentAcc + 1;
          return currentAcc;
        }

        @Override
        public Long getResult(Long accumulator) {
          return accumulator;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
          return acc1 + acc2;
        }
      }
    );

    // print, execute
    registrationsEvery3s.print();
    env.execute();
  }

  // keyed streams and window functions
  static KeyedStream<ServerEvent, String> eventsByType =
    eventStream.keyBy(e -> e.getClass().getSimpleName());

  // for every key => new window allocation
  static WindowedStream<ServerEvent, String, TimeWindow> threeSecondWindowByType =
    eventsByType.window(TumblingEventTimeWindows.of(Time.seconds(3)));

  static class CountByWindow implements WindowFunction<ServerEvent, String, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<ServerEvent> values, Collector<String> out) throws Exception {
      int eventCount = 0;
      for (ServerEvent e : values) if (e instanceof PlayerRegistered)
        eventCount += 1;

      out.collect(
        "[" + window.getStart() + " - " + window.getEnd() + "] - " + eventCount + " events of type " + key
      );
    }
  }

  static void demoCountByTypeByWindow() throws Exception {
    DataStream<String> result = threeSecondWindowByType.apply(
      new CountByWindow()
    );
    result.print();
    env.execute();
  }

  // sliding windows
  // count how many players registered every 3s, updated every 1s?

  static void demoSlidingWindows() throws Exception {
    Time windowSize = Time.seconds(3);
    Time slidingDuration = Time.seconds(1);

    AllWindowedStream<ServerEvent, TimeWindow> slidingWindowAll =
      eventStream.windowAll(
        SlidingEventTimeWindows.of(windowSize, slidingDuration)
      );

    // processing
    DataStream<String> result = slidingWindowAll.apply(
      new CountByWindowAll()
    );

    result.print();
    env.execute();
  }

  // session windows
  // how many players registered NO MORE THAN 1s APART?

  static void demoSessionWindows() throws Exception {
    AllWindowedStream<ServerEvent, TimeWindow> groupedBySession =
      eventStream.windowAll(
        EventTimeSessionWindows.withGap(Time.seconds(1))
      );

    // processing
    DataStream<String> result = groupedBySession.apply(
      new CountByWindowAll()
    );

    result.print();
    env.execute();
  }

  // global window
  // how many registrations we have every 10 events

  static class CountByGlobalWindow implements AllWindowFunction<ServerEvent, String, GlobalWindow> {
    //                                                            ^ input      ^ output  ^ window type
    @Override
    public void apply(GlobalWindow window, Iterable<ServerEvent> values, Collector<String> out) throws Exception {
      int eventCount = 0;
      for (ServerEvent e : values) if (e instanceof PlayerRegistered)
        eventCount += 1;

      out.collect(
        eventCount + " players registered"
      );
    }
  }

  static void demoGlobalWindow() throws Exception {
    DataStream<String> result = env
      .fromCollection(
        events,
        TypeInformation.of(ServerEvent.class)
      )
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of(2))
      .apply(new CountByGlobalWindow());

    result.print();
    env.execute();
  }

  /**
   * Add Player Offline events in your list.
   * 1. Retrieve the number of NET new players (online - offline)
   *  - every 2 seconds
   *  - every 2 seconds, updated every 1s
   * 2. Find the window (sliding windows 2s, every 1s) that had THE MOST net registrations
   */


  public static void main(String[] args) throws Exception {
    demoGlobalWindow();
  }
}
