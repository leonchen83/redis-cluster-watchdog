/*
 * Copyright 2016 leon chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.cluster.watchdog;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.rdb.RdbListener;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class SimpleReplicationListener implements ReplicationListener {
    private static final Log logger = LogFactory.getLog(SimpleReplicationListener.class);
    private volatile Replicator replicator;

    @Override
    public void onSetReplication(String ip, int port) {
        new Thread(() -> {
            try {
                if (replicator != null) {
                    replicator.close();
                    replicator = null;
                }
                replicator = new RedisReplicator(ip, port, Configuration.defaultSetting());
                replicator.addRdbListener(new RdbListener.Adaptor() {
                    @Override
                    public void handle(Replicator replicator, KeyValuePair<?> kv) {
                        logger.info(kv);
                    }
                });
                replicator.addCommandListener((r, c) -> logger.info(c));
                replicator.addCloseListener(r -> logger.info("replication closed [" + ip + ":" + port + "]"));
                replicator.open();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();
    }

    @Override
    public void onUnsetReplication() {
        try {
            Replicator replicator = this.replicator;
            if (replicator != null) replicator.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long onGetSlaveOffset() {
        Replicator replicator = this.replicator;
        if (replicator == null) return 0L;
        return replicator.getConfiguration().getReplOffset();
    }
}
