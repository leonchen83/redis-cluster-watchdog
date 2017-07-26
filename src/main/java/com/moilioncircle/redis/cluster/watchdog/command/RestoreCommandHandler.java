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
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.RdbListener;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static java.lang.Long.parseLong;

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
                managers.notifyRestoreCommand(key, keyTTL == 0L ? keyTTL : System.currentTimeMillis() + keyTTL, kv, keyReplace);
            }
        });
        try {
            replicator.open();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static class RestoreReplicator extends AbstractReplicator {

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
            int version = this.inputStream.readInt(2);
            this.inputStream.skip(8);
            RestoreRdbVisitor v = new RestoreRdbVisitor(this);
            v.rdbLoadObject(this.inputStream, null, v.applyType(this.inputStream), version);
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

            public KeyValuePair<?> rdbLoadObject(RedisInputStream in, DB db, int valueType, int version) throws IOException {
                return super.rdbLoadObject(in, db, valueType, version);
            }
        }
    }
}
