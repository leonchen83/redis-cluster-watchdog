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
                                        if (hex(high) && hex(low)) {
                                            s.append((char) hexToInt(high, low));
                                        } else {
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
        if (inq || insq) throw new UnsupportedOperationException("parse file error.");
        if (s.length() > 0) list.add(s.toString());
        return list;
    }

    private static boolean hex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private static int hexToInt(char high, char low) {
        return hexToInt(high) * 16 + hexToInt(low);
    }

    private static int hexToInt(char c) {
        switch (c) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
            case 'A':
                return 10;
            case 'b':
            case 'B':
                return 11;
            case 'c':
            case 'C':
                return 12;
            case 'd':
            case 'D':
                return 13;
            case 'e':
            case 'E':
                return 14;
            case 'f':
            case 'F':
                return 15;
            default:
                return 0;
        }
    }

    public static void main(String[] args) {
        System.out.println(parseLine("foo bar \"newline are supported\\n\" and \"\\xff\\x00otherstuff\" test   \"''\""));
    }

}
