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

package com.moilioncircle.redis.cluster.watchdog.storage;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DefaultStorageEngine implements StorageEngine {
    private static Iterator<byte[]> EMPTY = new Iterator<byte[]>() {
        @Override
        public boolean hasNext() {
            return false;
        }
        
        @Override
        public byte[] next() {
            return new byte[0];
        }
    };
    
    @Override
    public long size() {
        return 0L;
    }
    
    @Override
    public long clear() {
        return 0L;
    }
    
    @Override
    public void persist() {
    }
    
    @Override
    public boolean readonly() {
        return false;
    }
    
    @Override
    public long size(int slot) {
        return 0L;
    }
    
    @Override
    public long clear(int slot) {
        return 0L;
    }
    
    @Override
    public void readonly(boolean r) {
    }
    
    @Override
    public Iterator<byte[]> keys() {
        return EMPTY;
    }
    
    @Override
    public Iterator<byte[]> keys(int slot) {
        return EMPTY;
    }
    
    @Override
    public long ttl(byte[] key) {
        return 0L;
    }
    
    @Override
    public boolean delete(byte[] key) {
        return false;
    }
    
    @Override
    public Object load(byte[] key) {
        return null;
    }
    
    @Override
    public boolean exist(byte[] key) {
        return false;
    }
    
    @Override
    public Class<?> type(byte[] key) {
        return null;
    }
    
    @Override
    public boolean save(byte[] key, Object value, long expire, boolean force) {
        return false;
    }
    
    @Override
    public byte[] dump(byte[] key) {
        return null;
    }
    
    @Override
    public boolean restore(byte[] key, byte[] serialized, long expire, boolean force) {
        return false;
    }
    
    @Override
    public void start() {
    }
    
    @Override
    public void stop() {
        stop(0, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void stop(long timeout, TimeUnit unit) {
    }
}
