/*
 * Copyright 2016-2018 Leon Chen
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

import com.moilioncircle.redis.cluster.watchdog.command.CommandHandler;
import com.moilioncircle.redis.cluster.watchdog.storage.RedisStorageEngine;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ReplicationTest {
    
    @Test
    public void test() {
        ClusterConfiguration configuration = ClusterConfiguration.defaultSetting();
        configuration.setFailover(true).setVersion(Version.PROTOCOL_V0).setClusterAnnouncePort(20000);
        
        ClusterWatchdog watchdog = new RedisClusterWatchdog(configuration);
        watchdog.setStorageEngine(new RedisStorageEngine());
        watchdog.addCommandHandler("set", new CommandHandler.Adaptor() {
            @Override
            public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
                getStorageEngine().save(rawMessage[1], rawMessage[2], 0, true);
                reply(t, "OK");
            }
        });
        watchdog.addCommandHandler("get", new CommandHandler.Adaptor() {
            @Override
            public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
                byte[] value = (byte[]) getStorageEngine().load(rawMessage[1]);
                replyBulk(t, value);
            }
        });
        watchdog.setClusterConfigListener(System.out::println);
        watchdog.setReplicationListener(new SimpleReplicationListener());
        watchdog.start();
        
        Jedis jedis = new Jedis("127.0.0.1", 20000);
        assertEquals("OK", jedis.set("key", "value"));
        assertEquals(1L, jedis.dbSize().longValue());
        assertEquals("value", jedis.get("key"));
        jedis.close();
        watchdog.stop(5, TimeUnit.SECONDS);
    }
}