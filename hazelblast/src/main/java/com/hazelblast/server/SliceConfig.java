package com.hazelblast.server;

import static com.hazelblast.utils.Arguments.notNull;

public class SliceConfig {

    public final String name;

    public SliceConfig() {
        this(Slice.DEFAULT_NAME);
    }

    public SliceConfig(String name) {
        this.name = notNull("name", name);
    }
}
