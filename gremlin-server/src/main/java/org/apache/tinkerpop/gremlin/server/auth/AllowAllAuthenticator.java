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
package org.apache.tinkerpop.gremlin.server.auth;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AllowAllAuthenticator implements Authenticator {
    private static final SaslNegotiator AUTHENTICATOR_INSTANCE = new Negotiator();

    public boolean requireAuthentication(){
        return false;
    }

    @Override
    public void setup(final Map<String,Object> config) {
    }

    public SaslNegotiator newSaslNegotiator() {
        return AUTHENTICATOR_INSTANCE;
    }

    private static class Negotiator implements SaslNegotiator {

        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
            return null;
        }

        public boolean isComplete() {
            return true;
        }

        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            return AuthenticatedUser.ANONYMOUS_USER;
        }
    }
}
