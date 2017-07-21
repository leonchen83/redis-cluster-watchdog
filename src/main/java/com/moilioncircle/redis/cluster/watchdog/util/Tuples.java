package com.moilioncircle.redis.cluster.watchdog.util;

import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple2;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple3;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple4;
import com.moilioncircle.redis.cluster.watchdog.util.type.Tuple5;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Tuples {

    public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }

    public static <T1, T2, T3> Tuple3<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
        return new Tuple3<>(t1, t2, t3);
    }

    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 t1, T2 t2, T3 t3, T4 t4) {
        return new Tuple4<>(t1, t2, t3, t4);
    }

    public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
        return new Tuple5<>(t1, t2, t3, t4, t5);
    }
}
