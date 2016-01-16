package brickhouse.hbase;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in c¬ompliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import java.util.HashMap;
import java.util.Map;

/**
 * Represents column types pertaining to column qualifier in hbase
 *
 * Created by Marcin Michalski on 1/11/16.
 */

public enum ColumnType {
    DOUBLE("d"), INT("i"), LONG("l"), STRING("s");

    private final String columnType;

    private static final Map<String, ColumnType> TYPES = new HashMap<String, ColumnType>();

    static {
        TYPES.put(DOUBLE.columnType, DOUBLE);
        TYPES.put(INT.columnType, INT);
        TYPES.put(LONG.columnType, LONG);
        TYPES.put(STRING.columnType, STRING);
    }

    private ColumnType(String type) {
        this.columnType = type;
    }

    public String getColunType() {
        return this.columnType;
    }

    public static ColumnType fromColumnType(String type) {
        return TYPES.get(type);
    }
}
