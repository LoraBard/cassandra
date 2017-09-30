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

package org.apache.cassandra.auth.jmx;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableMap;
import org.junit.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.*;

public class AuthorizationProxyTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "1");
    }

    JMXResource osBean = JMXResource.mbean("java.lang:type=OperatingSystem");
    JMXResource runtimeBean = JMXResource.mbean("java.lang:type=Runtime");
    JMXResource threadingBean = JMXResource.mbean("java.lang:type=Threading");
    JMXResource javaLangWildcard = JMXResource.mbean("java.lang:type=*");

    JMXResource hintsBean = JMXResource.mbean("org.apache.cassandra.hints:type=HintsService");
    JMXResource batchlogBean = JMXResource.mbean("org.apache.cassandra.db:type=BatchlogManager");
    JMXResource customBean = JMXResource.mbean("org.apache.cassandra:type=CustomBean,property=foo");
    Set<ObjectName> allBeans = objectNames(osBean, runtimeBean, threadingBean, hintsBean, batchlogBean, customBean);

    RoleResource role1 = RoleResource.role("r1");

    @Before
    public void before() throws Throwable
    {
        DatabaseDescriptor.getRoleManager().setup().get();
        DatabaseDescriptor.getRoleManager().createRole(null,
                                                       role1,
                                                       new RoleOptions());
    }

    @Test
    public void roleHasRequiredPermission() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(osBean, permission(CorePermission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleDoesNotHaveRequiredPermission() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(osBean, permission(CorePermission.AUTHORIZE)));

        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "setAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasRequiredPermissionOnRootResource() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(JMXResource.root(), permission(CorePermission.SELECT)));

        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                .isAuthzRequired(() -> true)
                .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasOtherPermissionOnRootResource() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(JMXResource.root(), permission(CorePermission.AUTHORIZE)));

        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                .isAuthzRequired(() -> true)
                .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "invoke",
                                    new Object[]{ objectName(osBean), "bogusMethod" }));
    }

    @Test
    public void roleHasNoPermissions() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, Collections.emptyMap()))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "getAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasNoPermissionsButIsSuperuser() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.createSuperUserRolesAndPermissions(name, roles))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void roleHasNoPermissionsButAuthzNotRequired() throws Throwable
    {
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, Collections.emptyMap()))
                                                     .isAuthzRequired(() -> false)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void authorizeWhenSubjectIsNull() throws Throwable
    {
        // a null subject indicates that the action is being performed by the
        // connector itself, so we always authorize it
        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(null,
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void rejectWhenSubjectNotAuthenticated() throws Throwable
    {
        // Access is denied to a Subject without any associated Principals
        AuthorizationProxy proxy = new ProxyBuilder().isAuthzRequired(() -> true)
                                                     .build();
        assertFalse(proxy.authorize(new Subject(),
                                    "getAttribute",
                                    new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void authorizeWhenWildcardGrantCoversExactTarget() throws Throwable
    {
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(javaLangWildcard, permission(CorePermission.SELECT)));

        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    @Test
    public void rejectWhenWildcardGrantDoesNotCoverExactTarget() throws Throwable
    {
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(javaLangWildcard, permission(CorePermission.SELECT)));

        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "getAttribute",
                                    new Object[]{ objectName(customBean), "arch" }));
    }

    @Test
    public void authorizeOnTargetWildcardWithPermissionOnRoot() throws Throwable
    {
        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
            ImmutableMap.of(role1, Collections.singletonMap(JMXResource.root(), permission(CorePermission.SELECT)));

        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(javaLangWildcard), "arch" }));
    }

    @Test
    public void rejectInvocationOfUnknownMethod() throws Throwable
    {
        // Grant ALL permissions on the root resource, so we know that it's
        // the unknown method that causes the authz rejection. Of course, this
        // isn't foolproof but it's something.
        PermissionSets allPerms =  PermissionSets.builder()
                                                 .addGranted(JMXResource.root()
                                                                        .applicablePermissions())
                                                 .build();
        Map<RoleResource, Map<IResource, PermissionSets>> permissions = ImmutableMap.of(role1, Collections.singletonMap(JMXResource.root(), allPerms));
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                    "unKnownMethod",
                                    new Object[] { ObjectName.getInstance(osBean.getObjectName()) }));
    }

    @Test
    public void rejectInvocationOfBlacklistedMethods() throws Throwable
    {
        String[] methods = { "createMBean",
                             "deserialize",
                             "getClassLoader",
                             "getClassLoaderFor",
                             "instantiate",
                             "registerMBean",
                             "unregisterMBean" };

        // Hardcode the superuser status check to return true, so any allowed method can be invoked.
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.createSuperUserRolesAndPermissions(name, roles))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        for (String method : methods)
            // the arguments array isn't significant, so it can just be empty
            assertFalse(proxy.authorize(subject(role1.getRoleName()), method, new Object[0]));
    }

    @Test
    public void authorizeMethodsWithoutMBeanArgumentIfPermissionsGranted() throws Throwable
    {
        // Certain methods on MBeanServer don't take an ObjectName as their first argument.
        // These methods are characterised by AuthorizationProxy as being concerned with
        // the MBeanServer itself, as opposed to a specific managed bean. Of these methods,
        // only those considered "descriptive" are allowed to be invoked by remote users.
        // These require the DESCRIBE permission on the root JMXResource.
        testNonMbeanMethods(true);
    }

    @Test
    public void rejectMethodsWithoutMBeanArgumentIfPermissionsNotGranted() throws Throwable
    {
        testNonMbeanMethods(false);
    }

    @Test
    public void rejectWhenAuthSetupIsNotComplete() throws Throwable
    {
        // IAuthorizer & IRoleManager should not be considered ready to use until
        // we know that auth setup has completed. So, even though the IAuthorizer
        // would theoretically grant access, the auth proxy should deny it if setup
        // hasn't finished.

        Map<RoleResource, Map<IResource, PermissionSets>> permissions =
        ImmutableMap.of(role1, Collections.singletonMap(osBean, permission(CorePermission.SELECT)));

        // verify that access is granted when setup is complete
        Set<RoleResource> roles = Collections.singleton(role1);
        AuthorizationProxy proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .build();

        assertTrue(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));

        // and denied when it isn't
        proxy = new ProxyBuilder().useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions))
                                                     .isAuthzRequired(() -> true)
                                                     .isAuthSetupComplete(() -> false)
                                                     .build();

        assertFalse(proxy.authorize(subject(role1.getRoleName()),
                                   "getAttribute",
                                   new Object[]{ objectName(osBean), "arch" }));
    }

    private void testNonMbeanMethods(boolean withPermission)
    {
        String[] methods = { "getDefaultDomain",
                             "getDomains",
                             "getMBeanCount",
                             "hashCode",
                             "queryMBeans",
                             "queryNames",
                             "toString" };

        Set<RoleResource> roles = Collections.singleton(role1);
        ProxyBuilder builder = new ProxyBuilder().isAuthzRequired(() -> true);
        if (withPermission)
        {
            Map<RoleResource, Map<IResource, PermissionSets>> permissions =
                ImmutableMap.of(role1, Collections.singletonMap(JMXResource.root(), permission(CorePermission.DESCRIBE)));
            builder.useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, permissions));
        }
        else
        {
            builder.useRolesAndPermissions((name) -> UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, roles, Collections.emptyMap()));
        }
        AuthorizationProxy proxy = builder.build();

        for (String method : methods)
            assertEquals(withPermission, proxy.authorize(subject(role1.getRoleName()), method, new Object[]{ null }));

        // non-whitelisted methods should be rejected regardless.
        // This isn't exactly comprehensive, but it's better than nothing
        String[] notAllowed = { "fooMethod", "barMethod", "bazMethod" };
        for (String method : notAllowed)
            assertFalse(proxy.authorize(subject(role1.getRoleName()), method, new Object[]{ null }));
    }

    private static PermissionSets permission(Permission permission)
    {
        return PermissionSets.builder()
                             .addGranted(permission)
                             .build();
    }

    private static Subject subject(String roleName)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new CassandraPrincipal(roleName));
        return subject;
    }

    private static ObjectName objectName(JMXResource resource) throws MalformedObjectNameException
    {
        return ObjectName.getInstance(resource.getObjectName());
    }

    private static Set<ObjectName> objectNames(JMXResource... resource)
    {
        Set<ObjectName> names = new HashSet<>();
        try
        {
            for (JMXResource r : resource)
                names.add(objectName(r));
        }
        catch (MalformedObjectNameException e)
        {
            fail("JMXResource returned invalid object name: " + e.getMessage());
        }
        return names;
    }

    public static class ProxyBuilder
    {
        Function<String, UserRolesAndPermissions> userRolesAndPermissions;
        Supplier<Boolean> isAuthzRequired;
        Supplier<Boolean> isAuthSetupComplete = () -> true;

        AuthorizationProxy build()
        {
            InjectableAuthProxy proxy = new InjectableAuthProxy();

            if (userRolesAndPermissions != null)
                proxy.setUserRolesAndPermissions(userRolesAndPermissions);

            if (isAuthzRequired != null)
                proxy.setIsAuthzRequired(isAuthzRequired);

            proxy.setIsAuthSetupComplete(isAuthSetupComplete);

            return proxy;
        }

        public ProxyBuilder useRolesAndPermissions(Function<String, UserRolesAndPermissions> f)
        {
            userRolesAndPermissions = f;
            return this;
        }

        ProxyBuilder isAuthzRequired(Supplier<Boolean> s)
        {
            isAuthzRequired = s;
            return this;
        }

        ProxyBuilder isAuthSetupComplete(Supplier<Boolean> s)
        {
            isAuthSetupComplete = s;
            return this;
        }

        private static class InjectableAuthProxy extends AuthorizationProxy
        {
            void setUserRolesAndPermissions(Function<String, UserRolesAndPermissions> f)
            {
                this.userRolesAndPermissions = f;
            }

            void setIsAuthzRequired(Supplier<Boolean> s)
            {
                this.isAuthzRequired = s;
            }

            void setIsAuthSetupComplete(Supplier<Boolean> s)
            {
                this.isAuthSetupComplete = s;
            }
        }
    }
}
