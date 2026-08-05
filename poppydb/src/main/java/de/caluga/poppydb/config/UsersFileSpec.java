package de.caluga.poppydb.config;

import java.util.List;

/**
 * Parsed and validated content of a {@code users-file}: {@code version} is {@code null} for the
 * bare-array (unversioned) shape and a positive integer for the {@code {"version": N, "users":
 * [...]}} wrapper shape. {@code warnings} carries non-fatal findings from {@link UsersFileLoader}
 * (currently: group/other-readable file permissions) so callers - both PoppyDB startup and
 * {@code ConfigInspector.deepCheck} (--check-config) - can surface them without the loader
 * having to know how each caller reports; see {@link UsersFileLoader} for the split between this
 * (collected warnings, surfaced by the caller) and {@link ConfigException} (fatal, thrown
 * directly). Dumb data holder, no behavior.
 */
public record UsersFileSpec(Long version, List<UserSpec> users, List<String> warnings) {
}
