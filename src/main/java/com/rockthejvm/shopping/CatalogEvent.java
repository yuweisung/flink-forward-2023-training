package com.rockthejvm.shopping;

import java.time.Instant;

public interface CatalogEvent {
    String getUserId();
    Instant getTime();
}
