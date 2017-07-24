/*
 * Copyright 2016-2017 Leon Chen
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
public enum ClusterState {
    CLUSTER_OK((byte) 0), CLUSTER_FAIL((byte) 1);

    private byte state;

    ClusterState(byte state) {
        this.state = state;
    }

    public static ClusterState valueOf(byte state) {
        if (state == 0) return CLUSTER_OK;
        else if (state == 1) return CLUSTER_FAIL;
        else throw new UnsupportedOperationException(String.valueOf(state));
    }
}
