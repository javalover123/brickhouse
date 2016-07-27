package brickhouse.udf.collect;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
 *
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * 
 * Creates a tagged session id for a user, and user agent and a time stamp. Default length between sessions is 30 minute = 1800000 milliseconds
 * This UDF will create a new session when:
 *  1. user id changes between current event and previous one
 *  2. session timeout is reached between current event and the previous one
 *  3. When user agent between current and last event have changed and no previous session exist for current user agent
 *   
 *   The input to this function is assumed to be sorted by a user_key, by time and user agent, and is useful for creating sessions from
 *     user activity data. (i.e. web logs ).  The timestamp is assumed to be bigint, representing the number of milliseconds from
 *     the beginning of the epoch.
 *     
 *    The input needs to be sorted and partitioned using Hive 'SORT' and 'DISTRIBUTE' clauses.  Example usage would be :
 *      
 *    SELECT user_id,
 *          session_id,
 *          max(user_agent) as user_agent
 *          collect( event_type) as events,
 *          min( tstamp) as session_start,
 *          max( tstamp ) as session_end
 *    FROM
 *      (SELECT user_id,
 *         if(session_info like '%:%', split(session_info, ":")[0], session_info) as session_id,
 *         if(session_info like '%:%', split(session_info, ":")[1], null) as user_agent,
 *         tstamp,
 *         event_type
 *    FROM 
 *      ( SELECT user_id,
 *               sessionize( user_key, user_agent, timestamp, session_timeout),
 *               event_type,
 *          FROM weblogs
 *       DISTRIBUTE BY user_id
 *       SORT BY user_id, tsamp
 *     ) sz
 *   ) ua
 *   GROUP BY
 *     user_id, session_id;
 *     
 *  To avoid memory overruns, one can also specify the maximum number of values to be considered in a session, after which a new session id is calculated
 *
 * @author mmichalski
 */
@Description(
		name="tagged_sessionize",
		value="_FUNC_(long, string, timestamp) - Returns a session id for the given id, ua and ts(long). Optional fourth parameter to specify time betweeen sessions in milliseconds. Optional fourth parameter specifying maximum session size.",
		extended="SELECT _FUNC_(uid, ua, ts), uid, ua, ts, event_type from foo;")

public class TaggedSessionizeUDF extends UDF {

    private static class TaggedSessionInfo {
        private final long userId;
        private final String assignedSessionId;

        private String userAgent;
        private long lastEventTs;


        public TaggedSessionInfo(long userId) {
            this.userId = userId;
            this.assignedSessionId = UUID.randomUUID().toString();

        }

        public long getUserId() {
            return userId;
        }

        public String getUserAgent() {
            return userAgent;
        }

        public void setUserAgent(String userAgent) {
            this.userAgent = userAgent;
        }

        public long getLastEventTs() {
            return lastEventTs;
        }

        public void setLastEventTs(long lastEventTs) {
            this.lastEventTs = lastEventTs;
        }

        public String getAssignedSessionId() {
            return assignedSessionId;
        }

        public String getSessionIdWithUA() {
            if(userAgent != null)
                return assignedSessionId.concat(":").concat(userAgent);
            else
                return assignedSessionId;
        }
    }


    private List<TaggedSessionInfo> userSessions;

    private long lastUid = Long.MIN_VALUE;
    private long lastTS = 0;


    public String evaluate(long uid, String ua, long ts) {
        return evaluate(uid, ua, ts, 1800000);
    }

    /**
     *
     * Create a new session when:
     *  1. user id break occurs
     *  2. user agent break occurs and no previous sessions exists for the current ua
     *  3. timeout between last event and current event is greater than tolerance
     *
     * @param uid
     * @param ua
     * @param ts
     * @param tolerance
     * @return
     */
    public String evaluate(long uid, String ua, long ts, int tolerance) {
        TaggedSessionInfo tsi = null;

        // Reset Tagged sessions since new user break occurred
        if(uid != lastUid) {
            userSessions = new ArrayList<TaggedSessionInfo>();
            tsi = new TaggedSessionInfo(uid);
            if(ua != null) {
                tsi.setUserAgent(ua);
            }
            userSessions.add(tsi);
        } else if(! timeStampCompare(lastTS, ts, tolerance)) {
            userSessions = new ArrayList<TaggedSessionInfo>();
            tsi = new TaggedSessionInfo(uid);
            if(ua != null) {
                tsi.setUserAgent(ua);
            }
            userSessions.add(tsi);
        } else {
            if(ua != null) {
                for(int i=userSessions.size(); i>0; i--) {
                    tsi = userSessions.get(i-1);

                    if(tsi.getUserAgent() == null) {
                        tsi.setUserAgent(ua);
                        break;
                    } else {
                        if(ua.equals(tsi.getUserAgent())) {
                            if(! timeStampCompare(tsi.getLastEventTs(), ts, tolerance)) {
                                tsi = new TaggedSessionInfo(uid);
                                tsi.setUserAgent(ua);
                                userSessions.add(tsi);
                            }
                            break;
                        } else {
                            tsi = null;
                        }
                    }
                }

                // We found no match that means we need a new session
                if(tsi == null) {
                    tsi = new TaggedSessionInfo(uid);
                    tsi.setUserAgent(ua);
                    userSessions.add(tsi);
                }
            } else {
                // Get last created session and use that
                tsi = userSessions.get(userSessions.size()-1);
            }
        }

        lastUid = uid;
        lastTS = ts;
        tsi.setLastEventTs(ts);

        return tsi.getSessionIdWithUA();
    }



    private Boolean timeStampCompare(long lastTS, long ts, int ms) {
        try {
            long difference = ts - lastTS;
            return (Math.abs((int)difference) < ms) ? true : false;
        } catch (ArithmeticException e) {
            return false;
        }
    }

}