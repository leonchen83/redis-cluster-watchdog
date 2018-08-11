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

package com.moilioncircle.redis.cluster.watchdog.command.cluster;

import com.moilioncircle.redis.cluster.watchdog.command.AbstractCommandHandler;
import com.moilioncircle.redis.cluster.watchdog.manager.ClusterManagers;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.util.Iterator;

import static com.moilioncircle.redis.cluster.watchdog.ClusterConstants.CLUSTER_SLOTS;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class ClusterGetKeysInSlotCommandHandler extends AbstractCommandHandler {
    
    public ClusterGetKeysInSlotCommandHandler(ClusterManagers managers) {
        super(managers);
    }
    
    @Override
    public void handle(Transport<byte[][]> t, String[] message, byte[][] rawMessage) {
        if (message.length != 4) {
            replyError(t, "ERR Wrong CLUSTER subcommand or number of arguments");
            return;
        }
        
        int slot;
        try {
            slot = parseInt(message[2]);
        } catch (Exception e) {
            replyError(t, "ERR Invalid slot:" + message[2]);
            return;
        }
        if (slot < 0 || slot > CLUSTER_SLOTS) {
            replyError(t, "ERR Invalid slot:" + slot);
            return;
        }
        
        long max;
        try {
            max = parseLong(message[3]);
        } catch (Exception e) {
            replyError(t, "ERR Invalid number of keys:" + message[3]);
            return;
        }
        if (max < 0) {
            replyError(t, "ERR Invalid number of keys:" + max);
            return;
        }
        
        max = Math.min(managers.slots.countKeysInSlot(slot), max);
        Iterator<byte[]> it = managers.slots.getKeysInSlot(slot);
        if (!it.hasNext()) {
            t.write("*0\r\n".getBytes(), true);
            return;
        } else
            t.write(("*" + max + "\r\n").getBytes(), false);
        int idx = 0;
        while (it.hasNext() && idx++ < max) {
            byte[] key = it.next();
            t.write(("$" + key.length + "\r\n").getBytes(), false);
            t.write(key, false);
            if (it.hasNext())
                t.write("\r\n".getBytes(), false);
            else
                t.write("\r\n".getBytes(), true);
        }
    }
}
