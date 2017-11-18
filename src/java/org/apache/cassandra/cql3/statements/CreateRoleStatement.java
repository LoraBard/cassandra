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

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

import io.reactivex.Single;

public class CreateRoleStatement extends AuthenticationStatement
{
    private final RoleResource role;
    private final RoleOptions opts;
    private final boolean ifNotExists;

    public CreateRoleStatement(RoleName name, RoleOptions options, boolean ifNotExists)
    {
        this.role = RoleResource.role(name.getName());
        this.opts = options;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void checkAccess(QueryState state)
    {
        // validate login first to avoid leaking role existence to anonymous users.
        state.checkNotAnonymous();

        if (!ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw invalidRequest("%s already exists", role.getRoleName());

        checkPermission(state, CorePermission.CREATE, RoleResource.root());
        if (opts.getSuperuser().isPresent())
        {
            if (opts.getSuperuser().get() && !state.isSuper())
                throw new UnauthorizedException("Only superusers can create a role with superuser status");
        }
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        opts.validate();

        if (role.getRoleName().isEmpty())
            throw new InvalidRequestException("Role name can't be an empty string");

    }

    public Single<ResultMessage> execute(QueryState state) throws RequestExecutionException, RequestValidationException
    {
        return Single.fromCallable(() -> {
            // not rejected in validate()
            if (ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(role))
                return new ResultMessage.Void();

            DatabaseDescriptor.getRoleManager().createRole(state.getUser(), role, opts);
            grantPermissionsToCreator(state);
            return new ResultMessage.Void();
        });
    }

    /**
     * Grant all applicable permissions on the newly created role to the user performing the request
     * see also: SchemaAlteringStatement#grantPermissionsToCreator and the overridden implementations
     * of it in subclasses CreateKeyspaceStatement & CreateTableStatement.
     */
    private void grantPermissionsToCreator(QueryState state)
    {
        // The creator of a Role automatically gets ALTER/DROP/AUTHORIZE permissions on it if:
        // * the user is not anonymous
        // * the configured IAuthorizer supports granting of permissions (not all do, AllowAllAuthorizer doesn't and
        //   custom external implementations may not)
        if (!state.getUser().isAnonymous())
        {
            try
            {
                IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
                RoleResource executor = RoleResource.role(state.getUser().getName());
                authorizer.grant(AuthenticatedUser.SYSTEM_USER,
                                 authorizer.applicablePermissions(this.role),
                                 this.role,
                                 executor,
                                 GrantMode.GRANT);
            }
            catch (UnsupportedOperationException e)
            {
                // not a problem, grant is an optional method on IAuthorizer
            }
        }
    }
}
