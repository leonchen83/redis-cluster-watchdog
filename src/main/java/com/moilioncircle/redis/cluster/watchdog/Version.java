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

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public enum Version {
    PROTOCOL_V0(0), PROTOCOL_V1(1);

    private int version;
    public int getVersion() { return version; }
    Version(int version) { this.version = version; }

    public static Version valueOf(int version) {
        if (version == 0) return PROTOCOL_V0;
        else if (version == 1) return PROTOCOL_V1;
        else throw new UnsupportedOperationException("version: " + version);
    }
}
