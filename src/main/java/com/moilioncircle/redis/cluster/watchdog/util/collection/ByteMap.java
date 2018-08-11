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

package com.moilioncircle.redis.cluster.watchdog.util.collection;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Arrays.fill;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public class ByteMap<V> implements Map<Byte, V> {
    //
    private static final int MASK = 0x000000FF;
    private static final byte[] INT2BYTE = new byte[256];
    
    static {
        INT2BYTE[Byte.MAX_VALUE & MASK] = Byte.MAX_VALUE;
        for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
            INT2BYTE[b & MASK] = b;
        }
    }
    
    private final Object[] table;
    private int size;
    
    public ByteMap() {
        this.size = 0;
        this.table = new Object[256];
    }
    
    @Override
    public int size() {
        return this.size;
    }
    
    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }
    
    @Override
    public void clear() {
        if (this.size <= 0) return;
        fill(table, null);
        size = 0;
    }
    
    @Override
    public Set<Byte> keySet() {
        return new KeySet();
    }
    
    @Override
    public Collection<V> values() {
        return new Values();
    }
    
    @Override
    public V get(final Object key) {
        return (V) this.table[index(key)];
    }
    
    @Override
    public V put(Byte key, V value) {
        final int index = index(key);
        V r = (V) this.table[index];
        if (r == null) this.size++;
        this.table[index] = value;
        return r;
    }
    
    @Override
    public V remove(final Object key) {
        final int index = index(key);
        V r = (V) this.table[index];
        if (r != null) this.size--;
        this.table[index] = null;
        return r;
    }
    
    @Override
    public boolean containsKey(Object key) {
        return this.table[index(key)] != null;
    }
    
    @Override
    public Set<Map.Entry<Byte, V>> entrySet() {
        return new EntrySet();
    }
    
    @Override
    public boolean containsValue(final Object value) {
        for (final Object element : this.table) {
            if (Objects.equals(element, value)) return true;
        }
        return false;
    }
    
    @Override
    public void putAll(Map<? extends Byte, ? extends V> map) {
        if (map == null) return;
        for (Map.Entry<? extends Byte, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }
    
    protected int index(Object key) {
        final Byte index = (Byte) key;
        return index & MASK;
    }
    
    protected Iterator<V> valueIterator() {
        return new ValueIterator();
    }
    
    protected Iterator<Byte> keyIterator() {
        return new KeyIterator();
    }
    
    protected Iterator<Map.Entry<Byte, V>> entryIterator() {
        return new EntryIterator();
    }
    
    private class Values extends AbstractCollection<V> {
        
        @Override
        public int size() {
            return size;
        }
        
        @Override
        public void clear() {
            ByteMap.this.clear();
        }
        
        @Override
        public Iterator<V> iterator() {
            return valueIterator();
        }
        
        @Override
        public boolean contains(Object value) {
            return containsValue(value);
        }
    }
    
    private class KeySet extends AbstractSet<Byte> {
        
        @Override
        public int size() {
            return size;
        }
        
        @Override
        public void clear() {
            ByteMap.this.clear();
        }
        
        @Override
        public Iterator<Byte> iterator() {
            return keyIterator();
        }
        
        @Override
        public boolean contains(final Object key) {
            return key != null && key instanceof Byte && containsKey(key);
        }
    }
    
    private class EntrySet extends AbstractSet<Map.Entry<Byte, V>> {
        
        @Override
        public int size() {
            return size;
        }
        
        @Override
        public void clear() {
            ByteMap.this.clear();
        }
        
        @Override
        public Iterator<Map.Entry<Byte, V>> iterator() {
            return entryIterator();
        }
        
        @Override
        public boolean contains(final Object entry) {
            if (!(entry instanceof Map.Entry)) return false;
            final Map.Entry<Byte, V> e = (Map.Entry<Byte, V>) entry;
            final V candidate = get(e.getKey());
            return candidate != null && candidate.equals(e.getValue());
        }
    }
    
    private abstract class AbstractIterator<T> implements Iterator<T> {
        
        protected int index = prefetch(0);
        protected int prev = index;
        
        @Override
        public final boolean hasNext() {
            return this.index >= 0;
        }
        
        @Override
        public final void remove() {
            ByteMap.this.remove((byte) prev);
        }
        
        protected final int prefetch(int index) {
            for (int i = index, length = table.length; i < length; i++) {
                if (table[i] != null) return i;
            }
            return -1;
        }
    }
    
    private class KeyIterator extends AbstractIterator<Byte> {
        
        @Override
        public Byte next() {
            if (this.index < 0) return null;
            final byte r = INT2BYTE[this.index];
            this.prev = this.index;
            this.index = prefetch(this.index + 1);
            return r;
        }
    }
    
    private class ValueIterator extends AbstractIterator<V> {
        
        @Override
        public V next() {
            if (this.index < 0) return null;
            final V r = (V) table[this.index];
            this.prev = this.index;
            this.index = prefetch(this.index + 1);
            return r;
        }
    }
    
    private class EntryIterator extends AbstractIterator<Map.Entry<Byte, V>> {
        
        @Override
        public Map.Entry<Byte, V> next() {
            if (this.index < 0) return null;
            final Map.Entry<Byte, V> r = new Entry(this.index);
            this.prev = this.index;
            this.index = prefetch(this.index + 1);
            return r;
        }
    }
    
    protected class Entry implements Map.Entry<Byte, V> {
        
        private final int index;
        
        public Entry(int index) {
            this.index = index;
        }
        
        @Override
        public Byte getKey() {
            return (byte) this.index;
        }
        
        @Override
        public V getValue() {
            return (V) table[this.index];
        }
        
        @Override
        public V setValue(V value) {
            final V r = (V) table[this.index];
            table[this.index] = value;
            if (r == null) size++;
            return r;
        }
    }
    
}
