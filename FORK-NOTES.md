# Personal preservation fork

This branch preserves the locally modified `dsh-connect-workbuddy` 4.12.0 installation as of 2026-10-21.

## What is preserved

- Complete fetched upstream Git history and tags. This branch starts from the original `v4.12.0` tag.
- `preserved/dsh-connect-workbuddy-4.12.0-local.zip`: exact installed package snapshot, including its packaged dependency and license files.
- `preserved/runtime/`: readable copies of the three modified/added runtime modules.
- `preserved/patch-history/`: patch diffs for the three local changes.
- `preserved/snapshot-manifest.json`: provenance and SHA-256 hashes of every archived file.

Local changes: U+200B substitution in WorkBuddy system text; WorkBuddy source-label headers; account-scoped DSH session/turn association with independent per-call message IDs. Existing quota failover is retained.

## Important build distinction

The root `src/` is the original upstream 4.12.0 TypeScript source. Local hotfixes were made to the installed compiled package and are preserved separately. **Running the original source build alone does not reproduce these hotfixes.** Use the preserved runtime snapshot to recover this exact version. Porting the hotfixes into TypeScript and publishing a maintained release is separate work.

## Recovery

1. Keep the Git bundle and this repository outside the current computer as another backup.
2. Run `python preserved/verify_snapshot.py` to verify the archive and every contained file.
3. Extract the ZIP into a new staging directory; do not overwrite the only existing copy.
4. The archive root is the plugin package root. Its metadata, compiled `lib/`, client assets and dependency must stay together. It still requires a compatible DSH host and normal account sign-in/configuration, which are deliberately not archived.
5. To restore an installation, fully exit DSH, back up its existing package directory, and replace it with the verified extracted package. Preserve the profile's own settings and account files separately. Plugin updates/reinstallation may overwrite the local hotfixes.

The changes passed syntax and local loopback integration checks. Official billing aggregation and long-term account-risk effects are not established. This fork is independent preservation, not an official WorkBuddy or DeepSeek release. Original MIT license and notices are retained.
