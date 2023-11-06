package com.rockthejvm.gaming;

import java.time.Instant;
import java.util.UUID;

public interface ServerEvent {
  Instant getEventTime();
  String getId();
}



