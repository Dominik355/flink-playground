package com.bilik.common;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Both beforeAll() and afterAll() just calls methods of MiniClusterWithClientResource, which are
 * overriden from rule of Junit4. We just insrted our classRule form Junit4 version into beforeAll
 * method and assigned it to class variable. So we basically just reuse that class, just adds
 * these calls as extension and do not register them as Rules.
 */
public class FlinkMiniClusterExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterExtension.class);

    private static final int PARALLELISM = 2;
    private static MiniClusterWithClientResource flinkCluster;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        flinkCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(PARALLELISM)
                                .setNumberTaskManagers(1)
                                .build());

        flinkCluster.before();

        LOG.info("Web UI is available at {}", flinkCluster.getRestAddres());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        flinkCluster.after();
    }
}