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

package com.moilioncircle.replicator.cluster.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static java.util.Arrays.stream;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class Reflections {

    public static <T> Class<T> getActualType(Class<?> clazz) {
        return getActualType(clazz, 0);
    }

    public static <T> Class<T> getActualType(Class<?> clazz, int index) {
        return (Class<T>) getActualTypes(clazz)[index];
    }

    public static Class<?>[] getActualTypes(Class<?> clazz) {
        Type type = clazz.getGenericSuperclass();
        if (!(type instanceof ParameterizedType)) return null;
        ParameterizedType parameterizedType = (ParameterizedType) type;
        return stream(parameterizedType.getActualTypeArguments()).map(e -> (Class<?>) e).toArray(Class<?>[]::new);
    }

}
