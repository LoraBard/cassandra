/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.UnauthorizedException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

public class DropUserStatement extends AuthenticationStatement
{
    private final String username;

    public DropUserStatement(String username)
    {
        this.username = username;
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (!isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exists", username));

        AuthenticatedUser user = state.getUser();
        if (user != null && user.getName().equals(username))
            throw new InvalidRequestException("Users aren't allowed to DROP themselves");
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.validateLogin();
        if (!state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perfrom DROP USER queries");
    }

    public CqlResult execute(ClientState state, List<ByteBuffer> variables)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // clean up permissions after the dropped user.
        DatabaseDescriptor.getAuthorizer().revokeAll(username);
        Auth.deleteUser(username);
        DatabaseDescriptor.getAuthenticator().drop(username);
        return null;
    }
}
