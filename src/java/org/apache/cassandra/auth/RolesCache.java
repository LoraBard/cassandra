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
package org.apache.cassandra.auth;

import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Role> implements RolesCacheMBean
{
    public RolesCache(IRoleManager roleManager)
    {
        super("RolesCache",
              DatabaseDescriptor::setRolesValidity,
              DatabaseDescriptor::getRolesValidity,
              DatabaseDescriptor::setRolesUpdateInterval,
              DatabaseDescriptor::getRolesUpdateInterval,
              DatabaseDescriptor::setRolesCacheMaxEntries,
              DatabaseDescriptor::getRolesCacheMaxEntries,
              roleManager::getRoleData,
              () -> DatabaseDescriptor.getAuthenticator().requireAuthentication());
    }

    /**
     * Retrieve the roles <em>directly</em> assigned to a role.
     */
    public Set<RoleResource> getRoles(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.memberOf;
    }

    /**
     * Retrieve the superuser status of a role.
     */
    public boolean isSuperuser(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.isSuper;
    }

    /**
     * Retrieve the superuser status of a role.
     */
    public boolean canLogin(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.canLogin;
    }

    /**
     * Retrieve the roles assigned to a role.
     */
    public Map<String, String> getCustomOptions(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.options;
    }

    /**
     * Retrieve the credentials for a role.
     */
    String getCredentials(RoleResource role)
    {
        Role ret = getInternal(role);
        return ret.hashedPassword;
    }

    private Role getInternal(RoleResource role)
    {
        return get(role);
    }
}
