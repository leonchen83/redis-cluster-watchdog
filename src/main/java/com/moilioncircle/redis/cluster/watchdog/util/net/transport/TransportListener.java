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

package com.moilioncircle.redis.cluster.watchdog.util.net.transport;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public interface TransportListener<T> {
    
    void onConnected(Transport<T> transport);
    
    void onMessage(Transport<T> transport, T message);
    
    void onException(Transport<T> transport, Throwable cause);
    
    void onDisconnected(Transport<T> transport, Throwable cause);
    
    abstract class Adaptor<T> implements TransportListener<T> {
        
        public void onConnected(Transport<T> transport) {
        }
        
        public void onMessage(Transport<T> transport, T message) {
        }
        
        public void onException(Transport<T> transport, Throwable cause) {
        }
        
        public void onDisconnected(Transport<T> transport, Throwable cause) {
        }
        
    }
}
