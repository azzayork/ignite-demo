package com.azzayork.ignite.lifecycle;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.lifecycle.LifecycleBean;

@Slf4j
public class MyLifecycleBean implements LifecycleBean {

    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        log.debug("*** Lifecycle: {} {}", evt.toString(), "***");
    }
}
