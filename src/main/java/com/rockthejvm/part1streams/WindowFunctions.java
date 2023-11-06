package com.rockthejvm.part1streams;

import com.rockthejvm.gaming.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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


  public static void main(String[] args) {

  }
}
