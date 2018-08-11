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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.ClusterConfiguration;
import com.moilioncircle.redis.cluster.watchdog.storage.StorageEngine;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface CommandHandler {
    StorageEngine getStorageEngine();
    
    void setStorageEngine(StorageEngine engine);
    
    ClusterConfiguration getConfiguration();
    
    void setConfiguration(ClusterConfiguration configuration);
    
    void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage);
    
    abstract class Adaptor implements CommandHandler {
        protected StorageEngine storageEngine;
        protected ClusterConfiguration configuration;
        
        @Override
        public StorageEngine getStorageEngine() {
            return storageEngine;
        }
        
        @Override
        public void setStorageEngine(StorageEngine storageEngine) {
            this.storageEngine = storageEngine;
        }
        
        @Override
        public ClusterConfiguration getConfiguration() {
            return configuration;
        }
        
        @Override
        public void setConfiguration(ClusterConfiguration configuration) {
            this.configuration = configuration;
        }
        
        /**
         *
         */
        protected void reply(Transport<byte[][]> t, String message) {
            reply(t, message.getBytes());
        }
        
        protected void reply(Transport<byte[][]> t, byte[] message) {
            t.write("+".getBytes(), false);
            t.write(message, false);
            t.write("\r\n".getBytes(), true);
        }
        
        protected void replyNumber(Transport<byte[][]> t, long number) {
            t.write((":" + number + "\r\n").getBytes(), true);
        }
        
        protected void replyBulk(Transport<byte[][]> t, String message) {
            replyBulk(t, message.getBytes());
        }
        
        protected void replyBulk(Transport<byte[][]> t, byte[] message) {
            t.write(("$" + message.length + "\r\n").getBytes(), false);
            t.write(message, false);
            t.write("\r\n".getBytes(), true);
        }
        
        protected void replyError(Transport<byte[][]> t, String message) {
            replyError(t, message.getBytes());
        }
        
        protected void replyError(Transport<byte[][]> t, byte[] message) {
            t.write("-".getBytes(), false);
            t.write(message, false);
            t.write("\r\n".getBytes(), true);
        }
    }
}
