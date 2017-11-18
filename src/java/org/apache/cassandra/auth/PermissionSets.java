/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.auth;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.auth.permission.Permissions;

/**
 * Container for granted permissions, restricted permissions and grantable permissions.
 */
public final class PermissionSets
{
    public static final PermissionSets EMPTY = new PermissionSets(Permissions.immutableSetOf(),
                                                                  Permissions.immutableSetOf(),
                                                                  Permissions.immutableSetOf());

    /**
     * Immutable set of granted permissions.
     */
    public final Set<Permission> granted;
    /**
     * Immutable set of restricted permissions.
     */
    public final Set<Permission> restricted;
    /**
     * Immutable set of permissions grantable to others.
     */
    public final Set<Permission> grantables;

    private PermissionSets(Set<Permission> granted, Set<Permission> restricted, Set<Permission> grantables)
    {
        this.granted = granted;
        this.restricted = restricted;
        this.grantables = grantables;
    }

    public Set<GrantMode> grantModesFor(Permission permission)
    {
        Set<GrantMode> modes = EnumSet.noneOf(GrantMode.class);
        if (granted.contains(permission))
            modes.add(GrantMode.GRANT);
        if (restricted.contains(permission))
            modes.add(GrantMode.RESTRICT);
        if (grantables.contains(permission))
            modes.add(GrantMode.GRANTABLE);
        return modes;
    }

    /**
     * Returns all permissions that are contained in {@link #granted}, {@link #restricted}
     * and {@link #grantables}.
     */
    public Set<Permission> allContainedPermissions()
    {
        Set<Permission> all = Permissions.setOf();
        all.addAll(granted);
        all.addAll(restricted);
        all.addAll(grantables);
        return all;
    }

    public Builder unbuild()
    {
        return new Builder().addGranted(granted)
                            .addRestricted(restricted)
                            .addGrantables(grantables);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PermissionSets that = (PermissionSets) o;

        if (!granted.equals(that.granted)) return false;
        if (!restricted.equals(that.restricted)) return false;
        return grantables.equals(that.grantables);
    }

    public int hashCode()
    {
        int result = granted.hashCode();
        result = 31 * result + restricted.hashCode();
        result = 31 * result + grantables.hashCode();
        return result;
    }

    public boolean isEmpty()
    {
        return granted.isEmpty() && grantables.isEmpty() && restricted.isEmpty();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("granted", granted)
                          .add("restricted", restricted)
                          .add("grantables", grantables)
                          .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Set<Permission> effectivePermissions()
    {
        Set<Permission> result = Permissions.setOf();
        result.addAll(granted);
        result.removeAll(restricted);
        return result;
    }

    public boolean hasEffectivePermission(Permission permission)
    {
        return granted.contains(permission) && !restricted.contains(permission);
    }

    public static final class Builder
    {
        private final Set<Permission> granted = Permissions.setOf();
        private final Set<Permission> restricted = Permissions.setOf();
        private final Set<Permission> grantables = Permissions.setOf();

        private Builder()
        {
        }

        public Builder addGranted(Set<Permission> granted)
        {
            this.granted.addAll(granted);
            return this;
        }

        public Builder addRestricted(Set<Permission> restricted)
        {
            this.restricted.addAll(restricted);
            return this;
        }

        public Builder addGrantables(Set<Permission> grantables)
        {
            this.grantables.addAll(grantables);
            return this;
        }

        public Builder addGranted(Permission granted)
        {
            this.granted.add(granted);
            return this;
        }

        public Builder addRestricted(Permission restricted)
        {
            this.restricted.add(restricted);
            return this;
        }

        public Builder addGrantable(Permission grantable)
        {
            this.grantables.add(grantable);
            return this;
        }

        public Builder removeGranted(Set<Permission> granted)
        {
            this.granted.removeAll(granted);
            return this;
        }

        public Builder removeRestricted(Set<Permission> restricted)
        {
            this.restricted.removeAll(restricted);
            return this;
        }

        public Builder removeGrantables(Set<Permission> grantables)
        {
            this.grantables.removeAll(grantables);
            return this;
        }

        public Builder removeGranted(Permission granted)
        {
            this.granted.remove(granted);
            return this;
        }

        public Builder removeRestricted(Permission restricted)
        {
            this.restricted.remove(restricted);
            return this;
        }

        public Builder removeGrantable(Permission grantable)
        {
            this.grantables.remove(grantable);
            return this;
        }

        public Builder add(PermissionSets permissionSets)
        {
            this.granted.addAll(permissionSets.granted);
            this.restricted.addAll(permissionSets.restricted);
            this.grantables.addAll(permissionSets.grantables);
            return this;
        }

        public PermissionSets build()
        {
            if (granted.isEmpty() && restricted.isEmpty() && grantables.isEmpty())
                return EMPTY;

            return new PermissionSets(Permissions.immutableSetOf(granted),
                                      Permissions.immutableSetOf(restricted),
                                      Permissions.immutableSetOf(grantables));
        }
    }
}
