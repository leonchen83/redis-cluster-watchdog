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

package com.moilioncircle.replicator.cluster.config;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * @author Leon Chen
 * @since 2.1.0
 */
public class ConfigFileParser {

    public static List<String> parseLine(String line) {
        char[] ary = line.toCharArray();
        List<String> list = new ArrayList<>();
        StringBuilder s = new StringBuilder();
        boolean inq = false, insq = false;
        for (int i = 0; i < ary.length; i++) {
            char c = ary[i];
            switch (c) {
                case ' ':
                    if (inq || insq) s.append(' ');
                    else if (s.length() > 0) {
                        list.add(s.toString());
                        s.setLength(0);
                    }
                    break;
                case '"':
                    if (!inq && !insq) inq = true;
                    else if (insq) {
                        s.append('"');
                    } else if (inq) {
                        list.add(s.toString());
                        s.setLength(0);
                        inq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse file error.");
                    }
                    break;
                case '\'':
                    if (!inq && !insq) insq = true;
                    else if (inq) {
                        s.append('\'');
                    } else if (insq) {
                        list.add(s.toString());
                        s.setLength(0);
                        insq = false;
                        if (i + 1 < ary.length && ary[i + 1] != ' ')
                            throw new UnsupportedOperationException("parse file error.");
                    }
                    break;
                case '\\':
                    if (!inq) s.append('\\');
                    else {
                        i++;
                        if (i < ary.length) {
                            switch (ary[i]) {
                                case 'n':
                                    s.append('\n');
                                    break;
                                case 'r':
                                    s.append('\r');
                                    break;
                                case 't':
                                    s.append('\t');
                                    break;
                                case 'b':
                                    s.append('\b');
                                    break;
                                case 'f':
                                    s.append('\f');
                                    break;
                                case 'x':
                                    if (i + 2 >= ary.length) s.append("\\x");
                                    else {
                                        char high = ary[++i];
                                        char low = ary[++i];
                                        try {
                                            s.append(parseInt(new String(new char[]{high, low}), 16));
                                        } catch (Exception e) {
                                            s.append("\\x");
                                            s.append(high);
                                            s.append(low);
                                        }
                                    }
                                    break;
                                default:
                                    s.append(ary[i]);
                                    break;
                            }
                        }
                    }
                    break;
                default:
                    s.append(c);
                    break;

            }
        }
        if (inq || insq) throw new UnsupportedOperationException("parse line[" + line + "] error.");
        if (s.length() > 0) list.add(s.toString());
        return list;
    }
}
