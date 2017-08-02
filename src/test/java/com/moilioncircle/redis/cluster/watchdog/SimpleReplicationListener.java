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

import com.moilioncircle.redis.cluster.watchdog.storage.StorageEngine;
import com.moilioncircle.redis.cluster.watchdog.util.Tuples;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple4;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandListener;
import com.moilioncircle.redis.replicator.rdb.RdbListener;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

import java.io.IOException;

import static com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType.NONE;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class SimpleReplicationListener implements ReplicationListener {

    private volatile Replicator replicator;

    @Override
    public void onSetReplication(String ip, int port, StorageEngine engine) {
        new Thread(() -> {
            try {
                if (replicator != null) { replicator.close(); replicator = null; }
                //
                replicator = new RedisReplicator(ip, port, Configuration.defaultSetting());
                replicator.addRdbListener(new RdbListener.Adaptor() {
                    @Override
                    public void preFullSync(Replicator replicator) {
                        engine.clear();
                    }

                    @Override
                    public void handle(Replicator replicator, KeyValuePair<?> kv) {
                        long expire = kv.getExpiredType() == NONE ? 0 : kv.getExpiredMs();
                        engine.save(kv.getRawKey(), kv.getValue(), expire, true);
                    }
                });
                replicator.addCommandListener(new CommandListener() {
                    @Override
                    public void handle(Replicator replicator, Command command) {
                        Tuple4<byte[], Object, Long, Boolean> kv = extract(command);
                        engine.save(kv.getV1(), kv.getV2(), kv.getV3(), kv.getV4());
                    }
                });
                replicator.open();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();
    }

    private Tuple4<byte[], Object, Long, Boolean> extract(Command command) {
        // extract command to key value pair.
        return Tuples.of(new byte[0], null, 0L, true);
    }

    @Override
    public void onUnsetReplication(StorageEngine engine) {
        try { Replicator r = replicator; if (r != null) r.close(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    @Override
    public long onGetSlaveOffset() {
        Replicator r = this.replicator;
        if (r == null) return 0L; return r.getConfiguration().getReplOffset();
    }
}
