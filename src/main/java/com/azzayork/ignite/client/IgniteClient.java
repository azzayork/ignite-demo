package com.azzayork.ignite.client;

import com.azzayork.ignite.lifecycle.MyLifecycleBean;
import lombok.Getter;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

@Getter
public class IgniteClient {

    Ignite ignite;

    public IgniteClient() {

        // Create new com.azzayork.ignite.configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setLifecycleBeans(new MyLifecycleBean());
        cfg.setClientMode(true);
        cfg.setPeerClassLoadingEnabled(true);

        // Start Ignite node with given com.azzayork.ignite.configuration.
        ignite = Ignition.start(cfg);
    }

}
