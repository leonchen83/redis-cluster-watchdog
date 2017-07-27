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

package com.moilioncircle.redis.cluster.watchdog.command;

import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;
import com.moilioncircle.redis.replicator.*;
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
import com.moilioncircle.redis.replicator.rdb.datatype.*;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.moilioncircle.redis.replicator.Constants.*;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class RestoreCommandHandler extends AbstractCommandHandler {

    public RestoreCommandHandler(ClusterManagers managers) {
        super(managers);
    }

    @Override
    public void handle(Transport<Object> t, String[] message, byte[][] rawMessage) {

        if (!managers.configuration.isAsMaster()) {
            replyError(t, "Unsupported COMMAND");
            return;
        }

        if (rawMessage.length != 4 && rawMessage.length != 5) {
            replyError(t, "wrong number of arguments for 'restore' command");
        }

        byte[] key = rawMessage[1];
        if (key == null) {
            replyError(t, "Invalid key: null");
        }

        long ttl = 0L;
        try {
            ttl = parseLong(message[2]);
        } catch (Exception e) {
            replyError(t, "Invalid ttl: " + message[2]);
        }

        if (ttl < 0) {
            replyError(t, "Invalid ttl: " + ttl);
        }

        byte[] serialized = rawMessage[3];

        if (serialized == null) {
            replyError(t, "Invalid serialized-value: null");
        }

        boolean replace = false;
        if (rawMessage.length == 5) {
            if (message[4] != null && message[4].equalsIgnoreCase("replace")) {
                replace = true;
            } else {
                replyError(t, "wrong number of arguments for 'restore' command");
            }
        }

        final long keyTTL = ttl;
        final boolean keyReplace = replace;
        Replicator replicator = new RestoreReplicator(new ByteArrayInputStream(serialized), Configuration.defaultSetting());
        replicator.addRdbListener(new RdbListener.Adaptor() {
            @Override
            public void handle(Replicator replicator, KeyValuePair<?> kv) {
                kv.setKey(new String(key, UTF_8));
                kv.setRawKey(key);
                if (keyTTL != 0L) {
                    kv.setExpiredType(ExpiredType.MS);
                    kv.setExpiredValue(System.currentTimeMillis() + keyTTL);
                }
                managers.notifyRestoreCommand(kv, keyReplace);
            }
        });
        try {
            replicator.open();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        reply(t, "OK");
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
            this.inputStream.skip(8);
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

        public static class RestoreRdbVisitor extends DefaultRdbVisitor {

            @Override
            public Event applyString(RedisInputStream in, DB db, int version) throws IOException {
                BaseRdbParser parser = new BaseRdbParser(in);
                KeyStringValueString o0 = new KeyStringValueString();
                byte[] val = parser.rdbLoadEncodedStringObject().first();
                o0.setValueRdbType(RDB_TYPE_STRING);
                o0.setValue(new String(val, CHARSET));
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
                    list.add(new String(element, CHARSET));
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
                    set.add(new String(element, CHARSET));
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
                    zset.add(new ZSetEntry(new String(element, CHARSET), score, element));
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
                    zset.add(new ZSetEntry(new String(element, CHARSET), score, element));
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
                    map.put(new String(field, CHARSET), new String(value, CHARSET));
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
                        map.put(new String(field, CHARSET), null);
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
                    map.put(new String(field, CHARSET), new String(value, CHARSET));
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
                    list.add(new String(e, CHARSET));
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
                    double score = Double.valueOf(new String(BaseRdbParser.StringHelper.zipListEntry(stream), CHARSET));
                    zllen--;
                    zset.add(new ZSetEntry(new String(element, CHARSET), score, element));
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
                    map.put(new String(field, CHARSET), new String(value, CHARSET));
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
                        list.add(new String(e, CHARSET));
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

            /**
             * @param in      input stream
             * @param db      redis db
             * @param version rdb version
             * @return module object
             * @throws IOException IOException
             * @since 2.3.0
             */
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

            public RestoreRdbVisitor(Replicator replicator) {
                super(replicator);
            }

            public KeyValuePair<?> rdbLoadObject(RedisInputStream in, DB db, int valueType, int version) throws IOException {
                return super.rdbLoadObject(in, db, valueType, version);
            }
        }
    }
}
