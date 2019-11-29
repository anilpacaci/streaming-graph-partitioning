/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
def parse(line, factory) {
    def parts = line.split(/\|/, 3)
    def id = parts[0]
    def label = parts[0].split(/:/)[0]
    def v1 = factory.vertex(id, label)
    v1.property("iid", id)
    if (!parts[1].empty) {
        parts[1].split(/ /).grep { !it.isEmpty() }.each {
            def (eLabel, refId, ts) = it.split(/,/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v1, v2, eLabel)
            if(ts != null) {
                edge.property("creationDate", Long.valueOf(ts))
            }
        }
    }
    
    if (!parts[2].empty) {
        parts[2].split(/ /).grep { !it.isEmpty() }.each {
            def (eLabel, refId, ts) = it.split(/,/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v2, v1, eLabel)
            if(ts != null) {
                edge.property("creationDate", Long.valueOf(ts))
            }
        }
    }
    return v1
}
