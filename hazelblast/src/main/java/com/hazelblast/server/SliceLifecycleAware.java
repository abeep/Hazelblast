package com.hazelblast.server;

public interface SliceLifecycleAware {

    void onStart();

    void onStop();
}
