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

import com.moilioncircle.redis.cluster.watchdog.util.Tuples;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;
import com.moilioncircle.redis.replicator.AbstractReplicator;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.DefaultExceptionListener;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.UncheckedIOException;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandListener;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.RdbListener;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueHash;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueList;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueModule;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueZSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static com.moilioncircle.redis.replicator.Constants.MODULE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_LOAD_NONE;
import static com.moilioncircle.redis.replicator.Constants.RDB_MODULE_OPCODE_EOF;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPMAP;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_INTSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STRING;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_ZIPLIST;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class RedisStorageEngine implements StorageEngine {
    
    private volatile boolean readonly;
    private AtomicLong size = new AtomicLong(0);
    private ConcurrentHashMap<Key, Tuple2<Long, Object>>[] slots;
    
    public RedisStorageEngine() {
        this.slots = new ConcurrentHashMap[CLUSTER_SLOTS];
        for (int i = 0; i < CLUSTER_SLOTS; i++) slots[i] = new ConcurrentHashMap<>();
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
        clear();
    }
    
    @Override
    public long size() {
        return size.get();
    }
    
    @Override
    public synchronized long clear() {
        long rs = 0L;
        for (int i = 0; i < CLUSTER_SLOTS; i++) {
            rs += clear(i);
        }
        assert 0 == size.get();
        return rs;
    }
    
    @Override
    public void persist() {
    }
    
    @Override
    public long size(int slot) {
        return slots[slot].size();
    }
    
    @Override
    public synchronized long clear(int slot) {
        int size = slots[slot].size();
        slots[slot].clear();
        this.size.addAndGet(-size);
        return size;
    }
    
    @Override
    public Iterator<byte[]> keys() {
        return new Iter();
    }
    
    @Override
    public Iterator<byte[]> keys(int slot) {
        return new SlotIter(slot);
    }
    
    @Override
    public long ttl(byte[] key) {
        Tuple2<Long, Object> v = slots[StorageEngine.calcSlot(key)].get(new Key(key));
        if (v == null) return -2L;
        long now = System.currentTimeMillis();
        if (v.getV1() != 0 && v.getV1() < now) return -1L;
        else if (v.getV1() == 0) return 0L;
        else return v.getV1() - now;
    }
    
    @Override
    public boolean delete(byte[] key) {
        if (slots[StorageEngine.calcSlot(key)].remove(new Key(key)) != null) {
            size.decrementAndGet();
            return true;
        }
        return false;
    }
    
    @Override
    public Object load(byte[] key) {
        Tuple2<Long, Object> v = slots[StorageEngine.calcSlot(key)].get(new Key(key));
        if (v == null) return null;
        else if (v.getV1() != 0 && v.getV1() < System.currentTimeMillis()) return null; //expired
        else return v.getV2();
    }
    
    @Override
    public boolean exist(byte[] key) {
        return slots[StorageEngine.calcSlot(key)].containsKey(new Key(key));
    }
    
    @Override
    public Class<?> type(byte[] key) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean save(byte[] key, Object value, long expire, boolean force) {
        Tuple2<Long, Object> r = slots[StorageEngine.calcSlot(key)].compute(new Key(key), (k, v) -> {
            if (v == null) {
                size.incrementAndGet();
                return Tuples.of(expire, value);
            } else if (!force) {
                if (v.getV1() != 0L && v.getV1() < System.currentTimeMillis())
                    // mark expired
                    return Tuples.of(expire, value);
                else
                    return v;
            } else {
                return Tuples.of(expire, value);
            }
        });
        return r.getV2() == value;
    }
    
    @Override
    public byte[] dump(byte[] key) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean restore(byte[] key, byte[] serialized, long expire, boolean force) {
        Replicator replicator = new RestoreReplicator(new ByteArrayInputStream(serialized), Configuration.defaultSetting());
        AtomicBoolean rs = new AtomicBoolean(false);
        replicator.addRdbListener(new RdbListener.Adaptor() {
            @Override
            public void handle(Replicator replicator, KeyValuePair<?> kv) {
                rs.set(save(key, kv.getValue(), expire, force));
            }
        });
        try {
            replicator.open();
        } catch (IOException e) {
        }
        return rs.get();
    }
    
    @Override
    public boolean readonly() {
        return this.readonly;
    }
    
    @Override
    public void readonly(boolean r) {
        this.readonly = r;
    }
    
    public static class RestoreReplicator extends AbstractReplicator {
        
        public RestoreReplicator(InputStream in, Configuration configuration) {
            Objects.requireNonNull(in);
            Objects.requireNonNull(configuration);
            this.configuration = configuration;
            this.inputStream = new RedisInputStream(in, this.configuration.getBufferSize());
            this.inputStream.setRawByteListeners(this.rawByteListeners);
            if (configuration.isUseDefaultExceptionListener())
                addExceptionListener(new DefaultExceptionListener());
        }
        
        @Override
        public void open() throws IOException {
            try {
                doOpen();
            } catch (EOFException ignore) {
            } catch (UncheckedIOException e) {
                if (!(e.getCause() instanceof EOFException)) throw e;
            } finally {
                close();
            }
        }
        
        protected void doOpen() throws IOException {
            RestoreRdbVisitor v = new RestoreRdbVisitor(this);
            submitEvent(v.rdbLoadObject(this.inputStream, null, v.applyType(this.inputStream), 8));
            int version = this.inputStream.readInt(2);
            long checksum = this.inputStream.readLong(8);
        }
        
        @Override
        public void close() throws IOException {
            doClose();
        }
        
        @Override
        public void setRdbVisitor(RdbVisitor rdbVisitor) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean addCommandListener(CommandListener listener) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean removeCommandListener(CommandListener listener) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public CommandParser<? extends Command> getCommandParser(CommandName command) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public <T extends Command> void addCommandParser(CommandName command, CommandParser<T> parser) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public CommandParser<? extends Command> removeCommandParser(CommandName command) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void builtInCommandParserRegister() {
            throw new UnsupportedOperationException();
        }
        
        private static class RestoreRdbVisitor extends DefaultRdbVisitor {
            
            public RestoreRdbVisitor(Replicator replicator) {
                super(replicator);
            }
            
            @Override
            public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueString o0 = new KeyStringValueString();
                byte[] val = parser.rdbLoadEncodedStringObject().first();
                o0.setValueRdbType(RDB_TYPE_STRING);
                o0.setValue(new String(val, UTF_8));
                o0.setRawValue(val);
                o0.setDb(db);
                return o0;
            }
            
            @Override
            public Event applyList(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueList o1 = new KeyStringValueList();
                long len = parser.rdbLoadLen().len;
                List<String> list = new ArrayList<>();
                List<byte[]> rawList = new ArrayList<>();
                for (int i = 0; i < len; i++) {
                    byte[] element = parser.rdbLoadEncodedStringObject().first();
                    list.add(new String(element, UTF_8));
                    rawList.add(element);
                }
                o1.setValueRdbType(RDB_TYPE_LIST);
                o1.setValue(list);
                o1.setRawValue(rawList);
                o1.setDb(db);
                return o1;
            }
            
            @Override
            public Event applySet(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueSet o2 = new KeyStringValueSet();
                long len = parser.rdbLoadLen().len;
                Set<String> set = new LinkedHashSet<>();
                Set<byte[]> rawSet = new LinkedHashSet<>();
                for (int i = 0; i < len; i++) {
                    byte[] element = parser.rdbLoadEncodedStringObject().first();
                    set.add(new String(element, UTF_8));
                    rawSet.add(element);
                }
                o2.setValueRdbType(RDB_TYPE_SET);
                o2.setValue(set);
                o2.setRawValue(rawSet);
                o2.setDb(db);
                return o2;
            }
            
            @Override
            public Event applyZSet(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueZSet o3 = new KeyStringValueZSet();
                long len = parser.rdbLoadLen().len;
                Set<ZSetEntry> zset = new LinkedHashSet<>();
                while (len > 0) {
                    byte[] element = parser.rdbLoadEncodedStringObject().first();
                    double score = parser.rdbLoadDoubleValue();
                    zset.add(new ZSetEntry(new String(element, UTF_8), score, element));
                    len--;
                }
                o3.setValueRdbType(RDB_TYPE_ZSET);
                o3.setValue(zset);
                o3.setDb(db);
                return o3;
            }
            
            @Override
            public Event applyZSet2(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueZSet o5 = new KeyStringValueZSet();
                long len = parser.rdbLoadLen().len;
                Set<ZSetEntry> zset = new LinkedHashSet<>();
                while (len > 0) {
                    byte[] element = parser.rdbLoadEncodedStringObject().first();
                    double score = parser.rdbLoadBinaryDoubleValue();
                    zset.add(new ZSetEntry(new String(element, UTF_8), score, element));
                    len--;
                }
                o5.setValueRdbType(RDB_TYPE_ZSET_2);
                o5.setValue(zset);
                o5.setDb(db);
                return o5;
            }
            
            @Override
            public Event applyHash(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueHash o4 = new KeyStringValueHash();
                long len = parser.rdbLoadLen().len;
                Map<String, String> map = new LinkedHashMap<>();
                ByteArrayMap<byte[]> rawMap = new ByteArrayMap<>();
                while (len > 0) {
                    byte[] field = parser.rdbLoadEncodedStringObject().first();
                    byte[] value = parser.rdbLoadEncodedStringObject().first();
                    map.put(new String(field, UTF_8), new String(value, UTF_8));
                    rawMap.put(field, value);
                    len--;
                }
                o4.setValueRdbType(RDB_TYPE_HASH);
                o4.setValue(map);
                o4.setRawValue(rawMap);
                o4.setDb(db);
                return o4;
            }
            
            @Override
            public Event applyHashZipMap(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueHash o9 = new KeyStringValueHash();
                ByteArray aux = parser.rdbLoadPlainStringObject();
                RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(aux));
                Map<String, String> map = new LinkedHashMap<>();
                ByteArrayMap<byte[]> rawMap = new ByteArrayMap<>();
                BaseRdbParser.LenHelper.zmlen(stream); // zmlen
                while (true) {
                    int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
                    if (zmEleLen == 255) {
                        o9.setValueRdbType(RDB_TYPE_HASH_ZIPMAP);
                        o9.setValue(map);
                        o9.setRawValue(rawMap);
                        o9.setDb(db);
                        return o9;
                    }
                    byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
                    zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
                    if (zmEleLen == 255) {
                        //value is null
                        map.put(new String(field, UTF_8), null);
                        rawMap.put(field, null);
                        o9.setValueRdbType(RDB_TYPE_HASH_ZIPMAP);
                        o9.setValue(map);
                        o9.setRawValue(rawMap);
                        o9.setDb(db);
                        return o9;
                    }
                    int free = BaseRdbParser.LenHelper.free(stream);
                    byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
                    BaseRdbParser.StringHelper.skip(stream, free);
                    map.put(new String(field, UTF_8), new String(value, UTF_8));
                    rawMap.put(field, value);
                }
            }
            
            @Override
            public Event applyListZipList(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueList o10 = new KeyStringValueList();
                ByteArray aux = parser.rdbLoadPlainStringObject();
                RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(aux));
                
                List<String> list = new ArrayList<>();
                List<byte[]> rawList = new ArrayList<>();
                BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                BaseRdbParser.LenHelper.zltail(stream); // zltail
                int zllen = BaseRdbParser.LenHelper.zllen(stream);
                for (int i = 0; i < zllen; i++) {
                    byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                    list.add(new String(e, UTF_8));
                    rawList.add(e);
                }
                int zlend = BaseRdbParser.LenHelper.zlend(stream);
                if (zlend != 255) {
                    throw new AssertionError("zlend expect 255 but " + zlend);
                }
                o10.setValueRdbType(RDB_TYPE_LIST_ZIPLIST);
                o10.setValue(list);
                o10.setRawValue(rawList);
                o10.setDb(db);
                return o10;
            }
            
            @Override
            public Event applySetIntSet(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueSet o11 = new KeyStringValueSet();
                ByteArray aux = parser.rdbLoadPlainStringObject();
                RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(aux));
                
                Set<String> set = new LinkedHashSet<>();
                Set<byte[]> rawSet = new LinkedHashSet<>();
                int encoding = BaseRdbParser.LenHelper.encoding(stream);
                long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
                for (long i = 0; i < lenOfContent; i++) {
                    switch (encoding) {
                        case 2:
                            String element = String.valueOf(stream.readInt(2));
                            set.add(element);
                            rawSet.add(element.getBytes());
                            break;
                        case 4:
                            element = String.valueOf(stream.readInt(4));
                            set.add(element);
                            rawSet.add(element.getBytes());
                            break;
                        case 8:
                            element = String.valueOf(stream.readLong(8));
                            set.add(element);
                            rawSet.add(element.getBytes());
                            break;
                        default:
                            throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
                    }
                }
                o11.setValueRdbType(RDB_TYPE_SET_INTSET);
                o11.setValue(set);
                o11.setRawValue(rawSet);
                o11.setDb(db);
                return o11;
            }
            
            @Override
            public Event applyZSetZipList(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueZSet o12 = new KeyStringValueZSet();
                ByteArray aux = parser.rdbLoadPlainStringObject();
                RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(aux));
                
                Set<ZSetEntry> zset = new LinkedHashSet<>();
                BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                BaseRdbParser.LenHelper.zltail(stream); // zltail
                int zllen = BaseRdbParser.LenHelper.zllen(stream);
                while (zllen > 0) {
                    byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
                    zllen--;
                    double score = Double.valueOf(new String(BaseRdbParser.StringHelper.zipListEntry(stream), UTF_8));
                    zllen--;
                    zset.add(new ZSetEntry(new String(element, UTF_8), score, element));
                }
                int zlend = BaseRdbParser.LenHelper.zlend(stream);
                if (zlend != 255) {
                    throw new AssertionError("zlend expect 255 but " + zlend);
                }
                o12.setValueRdbType(RDB_TYPE_ZSET_ZIPLIST);
                o12.setValue(zset);
                o12.setDb(db);
                return o12;
            }
            
            @Override
            public Event applyHashZipList(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueHash o13 = new KeyStringValueHash();
                ByteArray aux = parser.rdbLoadPlainStringObject();
                RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(aux));
                
                Map<String, String> map = new LinkedHashMap<>();
                ByteArrayMap<byte[]> rawMap = new ByteArrayMap<>();
                BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                BaseRdbParser.LenHelper.zltail(stream); // zltail
                int zllen = BaseRdbParser.LenHelper.zllen(stream);
                while (zllen > 0) {
                    byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
                    zllen--;
                    byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
                    zllen--;
                    map.put(new String(field, UTF_8), new String(value, UTF_8));
                    rawMap.put(field, value);
                }
                int zlend = BaseRdbParser.LenHelper.zlend(stream);
                if (zlend != 255) {
                    throw new AssertionError("zlend expect 255 but " + zlend);
                }
                o13.setValueRdbType(RDB_TYPE_HASH_ZIPLIST);
                o13.setValue(map);
                o13.setRawValue(rawMap);
                o13.setDb(db);
                return o13;
            }
            
            @Override
            public Event applyListQuickList(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueList o14 = new KeyStringValueList();
                long len = parser.rdbLoadLen().len;
                List<String> list = new ArrayList<>();
                List<byte[]> rawList = new ArrayList<>();
                for (int i = 0; i < len; i++) {
                    ByteArray element = parser.rdbGenericLoadStringObject(RDB_LOAD_NONE);
                    RedisInputStream stream = new RedisInputStream(new com.moilioncircle.redis.replicator.io.ByteArrayInputStream(element));
                    
                    BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
                    BaseRdbParser.LenHelper.zltail(stream); // zltail
                    int zllen = BaseRdbParser.LenHelper.zllen(stream);
                    for (int j = 0; j < zllen; j++) {
                        byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                        list.add(new String(e, UTF_8));
                        rawList.add(e);
                    }
                    int zlend = BaseRdbParser.LenHelper.zlend(stream);
                    if (zlend != 255) {
                        throw new AssertionError("zlend expect 255 but " + zlend);
                    }
                }
                o14.setValueRdbType(RDB_TYPE_LIST_QUICKLIST);
                o14.setValue(list);
                o14.setRawValue(rawList);
                o14.setDb(db);
                return o14;
            }
            
            @Override
            public Event applyModule(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueModule o6 = new KeyStringValueModule();
                char[] c = new char[9];
                long moduleid = parser.rdbLoadLen().len;
                for (int i = 0; i < c.length; i++) {
                    c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
                }
                String moduleName = new String(c);
                int moduleVersion = (int) (moduleid & 1023);
                ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
                if (moduleParser == null) {
                    throw new NoSuchElementException("module[" + moduleName + "," + moduleVersion + "] not exist.");
                }
                o6.setValueRdbType(RDB_TYPE_MODULE);
                o6.setValue(moduleParser.parse(in, 1));
                o6.setDb(db);
                return o6;
            }
            
            @Override
            public Event applyModule2(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueModule o7 = new KeyStringValueModule();
                char[] c = new char[9];
                long moduleid = parser.rdbLoadLen().len;
                for (int i = 0; i < c.length; i++) {
                    c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
                }
                String moduleName = new String(c);
                int moduleVersion = (int) (moduleid & 1023);
                ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
                if (moduleParser == null) {
                    throw new NoSuchElementException("module[" + moduleName + "," + moduleVersion + "] not exist.");
                }
                o7.setValueRdbType(RDB_TYPE_MODULE_2);
                o7.setValue(moduleParser.parse(in, 2));
                o7.setDb(db);
                
                long eof = parser.rdbLoadLen().len;
                if (eof != RDB_MODULE_OPCODE_EOF) {
                    throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
                }
                return o7;
            }
            
            public KeyValuePair<?> rdbLoadObject(RedisInputStream in, DB db, int valueType, int version) throws IOException {
                return super.rdbLoadObject(in, db, valueType, version);
            }
        }
    }
    
    private class Key {
        private final byte[] key;
        
        private Key(final byte[] key) {
            this.key = key;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key1 = (Key) o;
            return Arrays.equals(key, key1.key);
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }
    }
    
    private class SlotIter implements Iterator<byte[]> {
        private Iterator<Key> curr;
        
        private SlotIter(int slot) {
            this.curr = slots[slot].keySet().iterator();
        }
        
        @Override
        public boolean hasNext() {
            return curr.hasNext();
        }
        
        @Override
        public byte[] next() {
            return curr.next().key;
        }
    }
    
    private class Iter implements Iterator<byte[]> {
        private int idx = 0;
        private Iterator<Key> curr;
        
        private Iter() {
            this.curr = slots[idx].keySet().iterator();
        }
        
        @Override
        public boolean hasNext() {
            while (true) {
                if (curr.hasNext()) return true;
                else if (idx + 1 < CLUSTER_SLOTS) {
                    idx++;
                    curr = slots[idx].keySet().iterator();
                    continue;
                } else {
                    return false;
                }
            }
        }
        
        @Override
        public byte[] next() {
            return curr.next().key;
        }
    }
}