import { lstat, mkdir, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { createDecipheriv, createHash, randomBytes, randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { dirname, join, resolve, win32 } from "node:path";
import { execFile, execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
//#region src/host-peers.ts
/**
* The two host utilities this plugin uses that must ALSO work in the
* standalone CLI (issue #47).
*
* WHY THIS EXISTS. `@deepseek-ai/dsh-atomic-write` and
* `@deepseek-ai/dsh-home-paths` are declared as **peer** dependencies: the DSH
* host carries them, and the plugin is supposed to borrow the host's copies.
* Inside a booted host that works — `packages/boot/app-boot`'s runtime
* interception layer answers those bare specifiers from the installation. But
* `dsh plugin --profile <p> exec dsh-connect-workbuddy …` is NOT a booted host:
* `plugin-manager`'s `runProfilePnpm` spawns `pnpm exec` in the profile
* directory with the plain environment, so the child resolves with stock Node
* rules and the interception layer is absent (DSH deliberately never sets
* `NODE_PATH`; `packages/boot/app-boot/src/index.ts` scrubs it).
*
* The profile is created with `autoInstallPeers: false`, so peers are never
* installed there either. The result on a clean machine is a load-time crash
* before any command runs:
*
*   Error [ERR_MODULE_NOT_FOUND]: Cannot find package
*   '@deepseek-ai/dsh-atomic-write' imported from
*   …/profiles/<p>/node_modules/dsh-connect-workbuddy/lib/host-heartbeat-*.js
*
* The trigger is CHUNKING, which is why the import looked unrelated to the
* command: tsdown emits `host-heartbeat-*.js` as the shared chunk holding
* `auth`, `at-rest`, `host-heartbeat`, and `pi-ai-runtime`, so `lib/bin.js`
* reaches `auth.ts`'s atomic-write import even for a read-only `status`.
*
* WHY NOT a static import with a fallback: a static `import` of a missing
* package throws at MODULE EVALUATION time, before any fallback can run. The
* import itself has to be deferred, so both peers are loaded through
* `import()` inside the functions that need them.
*
* WHY NOT move them to `dependencies`: the host half genuinely runs against
* the host's copies, and duplicating them into every profile would add two
* packages to a market install to serve a CLI that runs a handful of times.
* More importantly these two are pure, stateless, cordis-free utilities — no
* service, no process-global state — so the host's copy and a local copy are
* interchangeable, which is exactly what makes a fallback safe here. (Contrast
* `@deepseek-ai/cordis` or pi-ai, where a second copy WOULD be wrong: cordis
* carries the service registry, and pi-ai's generation changes context shape.)
*
* WHY THE FALLBACK MIRRORS THE HOST. `writeFileAtomic` and `withFileLock`
* govern a cross-process protocol: the lock is a `wx`-created `<file>.lock`
* sibling and the commit is a same-directory temp file renamed over the
* target. A fallback with different timing or a different lock path would let
* a CLI process and the host race each other on the same file, so the retry
* cadence, the Windows transient-error set, and the `EPERM`-needs-`lstat`
* confirmation rule are transcribed from the host package rather than
* reinvented. `docs/WINDOWS.md` and `CHANGELOG.md` (2.0.x) already record the
* measured budget: delay sequence 20+40+80+160+200×4 ≈ 1100ms, not the
* "600ms" an earlier draft of the docs claimed.
*
* Inside DSH the host's own copy always wins — the dynamic import resolves it
* first, and the fallback is unreachable there. The fallback exists solely so
* the standalone CLI is self-sufficient on a machine that has only the plugin.
*
* @module dsh-connect-workbuddy/host-peers
*/
const ATOMIC_WRITE = "@deepseek-ai/dsh-atomic-write";
/**
* Load one peer, preferring whatever the host provides.
*
* The host's copy is resolved through the same ESM resolver this module is
* loaded by, so the answer matches the import the plugin would otherwise have
* made. Resolution failure is not an error here: it is the documented state of
* a market install probed outside the host, and the caller falls back.
*
* Why ONE `catch` covers both "absent" and "broken": `import()` rejects with
* `ERR_MODULE_NOT_FOUND` for the first and rethrows the module's own error for
* the second, and there is nothing this plugin could do differently about
* either — it cannot repair the host's copy. Falling back is strictly better
* than crashing, and it is not SILENT: `doctor` reports which implementation
* is in use, so a host copy that is present but unloadable shows up as
* `fallback` on a machine where the host should have supplied it. Narrowing
* the catch to `ERR_MODULE_NOT_FOUND` would instead turn a corrupt host
* install into a hard crash inside a read-only `status`, which is exactly the
* failure mode issue #47 is about.
*/
async function loadHostModule(specifier) {
	try {
		return await import(specifier);
	} catch {
		return;
	}
}
let atomicWrite;
/** The host's atomic-write copy, or undefined when only the fallback is available. */
function hostAtomicWrite() {
	atomicWrite ??= loadHostModule(ATOMIC_WRITE);
	return atomicWrite;
}
/** Directory name for the default DeepSeek Harness home under the OS home. */
const DSH_HOME_DIR_NAME = ".dsh";
/** Environment variable that overrides the default DeepSeek Harness home. */
const DSH_HOME_ENV = "DSH_HOME";
/** Expand `~`, `~/`, and `~\` against the operating-system home. */
function expandHomePath(path) {
	if (path === "~") return homedir();
	if (path.startsWith("~/") || path.startsWith("~\\")) return join(homedir(), path.slice(2));
	return path;
}
/**
* Resolve the default harness home without the host package.
*
* Precedence, highest first: an explicit configured path, `$DSH_HOME`, then
* `~/.dsh`. A blank `$DSH_HOME` counts as unset, so an empty override can
* never resolve the home to the current working directory. This is the whole
* of `resolveDshHome`; the host package's other exports are not used here.
*/
function fallbackResolveDshHome(configured, env = process.env) {
	const fromEnv = env[DSH_HOME_ENV];
	const chosen = configured ?? (fromEnv !== void 0 && fromEnv.trim().length > 0 ? fromEnv : join(homedir(), DSH_HOME_DIR_NAME));
	return resolve(expandHomePath(chosen));
}
/** Windows errors that mean temporary interference with an atomic replacement. */
const WINDOWS_TRANSIENT_RENAME_ERRORS = /* @__PURE__ */ new Set([
	"EACCES",
	"EBUSY",
	"EPERM"
]);
const WINDOWS_RENAME_RETRY_INITIAL_MS = 20;
const WINDOWS_RENAME_RETRY_MAX_MS = 200;
const WINDOWS_RENAME_RETRY_LIMIT = 8;
/** Whether Windows reported temporary interference with an atomic replacement. */
function isTransientWindowsRenameError(error) {
	if (process.platform !== "win32") return false;
	return WINDOWS_TRANSIENT_RENAME_ERRORS.has(error?.code ?? "");
}
/** Replace the target after bounded retries for transient Windows interference. */
async function renameAtomicTemp(temp, filename) {
	let delay = WINDOWS_RENAME_RETRY_INITIAL_MS;
	for (let retries = 0;; retries += 1) {
		try {
			await rename(temp, filename);
			return;
		} catch (error) {
			if (!isTransientWindowsRenameError(error)) throw error;
			if (retries >= WINDOWS_RENAME_RETRY_LIMIT) throw error;
		}
		await new Promise((resolve) => setTimeout(resolve, delay));
		delay = Math.min(delay * 2, WINDOWS_RENAME_RETRY_MAX_MS);
	}
}
/**
* Replace `filename` with `content` in one atomic step, creating parents.
*
* The content goes to a random-suffix sibling opened `wx`, so the open refuses
* to follow a symlink planted at the temp path, and the fresh inode carries
* `options.mode` through the rename. Same-directory keeps the rename on one
* filesystem; a failure removes the temp file and rethrows.
*/
async function fallbackWriteFileAtomic(filename, content, options) {
	await mkdir(dirname(filename), {
		recursive: true,
		...options.dirMode === void 0 ? {} : { mode: options.dirMode }
	});
	const temp = `${filename}.${randomBytes(6).toString("hex")}.tmp`;
	try {
		await writeFile(temp, content, {
			mode: options.mode,
			flag: "wx"
		});
		await renameAtomicTemp(temp, filename);
	} catch (error) {
		await rm(temp, { force: true });
		throw error;
	}
}
/** Whether an exclusive create found an existing lock. */
async function isLockContention(error, lockPath) {
	const code = error?.code;
	if (code === "EEXIST") return true;
	if (code !== "EPERM") return false;
	try {
		await lstat(lockPath);
		return true;
	} catch {
		return false;
	}
}
const LOCK_RETRY_INITIAL_MS = 20;
const LOCK_RETRY_MAX_MS = 200;
/** The host's own default: sized for a file-work-only critical section. */
const DEFAULT_LOCK_WAIT_MS = 2e3;
/**
* Hold the cross-process writer lock for `filename` around one operation.
*
* The lock is a `wx`-created `<filename>.lock` sibling; paired with the
* rename-based commit above, readers stay lock-free and only writers contend.
* An `EPERM` counts as contention only when a fresh `lstat` confirms the lock
* exists, which covers exclusive-create behavior on Windows. The contender
* never removes an existing lock: file age cannot prove its owner stopped.
*/
async function fallbackWithFileLock(filename, operation, options) {
	const lockPath = `${filename}.lock`;
	const deadline = Date.now() + (options?.waitMs ?? DEFAULT_LOCK_WAIT_MS);
	let delay = LOCK_RETRY_INITIAL_MS;
	let retriedUnconfirmedPermissionError = false;
	for (;;) {
		try {
			await writeFile(lockPath, `${process.pid}\n`, {
				mode: 384,
				flag: "wx"
			});
			break;
		} catch (error) {
			if (!await isLockContention(error, lockPath)) {
				const code = error?.code;
				if (process.platform !== "win32" || code !== "EPERM" || retriedUnconfirmedPermissionError) throw error;
				retriedUnconfirmedPermissionError = true;
			}
		}
		if (Date.now() >= deadline) throw new Error(`atomic-write: timed out waiting for the writer lock at ${lockPath}`);
		await new Promise((resolve) => setTimeout(resolve, delay));
		delay = Math.min(delay * 2, LOCK_RETRY_MAX_MS);
	}
	try {
		return await operation();
	} finally {
		await rm(lockPath, { force: true });
	}
}
/**
* Resolve the DeepSeek Harness home.
*
* Deliberately SYNCHRONOUS and self-contained rather than a host-first loader.
* The host package's `resolveDshHome` is a pure function of its arguments and
* `process.env` — it performs no I/O — and every call site in this plugin
* passes NO argument. The zero-argument form is `$DSH_HOME` (when non-blank)
* else `~/.dsh`, which is reproduced exactly below, so consulting the host
* copy could not change a single answer while it WOULD have forced the
* synchronous call sites (credential paths, the heartbeat path) to become
* async. The host import is therefore dropped outright.
*
* Cost of being wrong: nothing. Precedence is transcribed, not guessed, and
* the two branches are pinned by `tests/host-peers.spec.ts`.
*/
function resolveDshHome(configured) {
	return fallbackResolveDshHome(configured);
}
/**
* Replace `filename` with `content` atomically, under the sibling write lock.
*
* Async only because the host module is loaded lazily; inside DSH this is the
* host's own implementation.
*/
async function writeFileAtomic(filename, content, options) {
	const host = await hostAtomicWrite();
	if (host === void 0) return fallbackWriteFileAtomic(filename, content, options);
	return host.writeFileAtomic(filename, content, options);
}
/** Hold the cross-process writer lock for `filename` around one operation. */
async function withFileLock(filename, operation, options) {
	const host = await hostAtomicWrite();
	if (host === void 0) return fallbackWithFileLock(filename, operation, options);
	return host.withFileLock(filename, operation, options);
}
/**
* Which atomic-write implementation this process would use.
*
* `doctor` reports it so the "works inside DSH, crashed under `plugin exec`"
* split behind issue #47 is visible on demand instead of being inferred from a
* stack trace. `fallback` is the expected answer outside a booted host.
*/
async function hostPeersInfo() {
	return { atomicWrite: await hostAtomicWrite() === void 0 ? "fallback" : "host" };
}
//#endregion
//#region src/at-rest.ts
/**
* WorkBuddy desktop "at-rest" credential decryption.
*
* From 5.6.0 the WorkBuddy desktop app no longer stores `auth.accessToken` /
* `auth.refreshToken` as plain strings. It writes a field wrapper:
*
*   { "$wbEncrypted": 1, "envelope": "<base64 of a JSON envelope>" }
*
* where the envelope is `{suite, keyId, nonce, authTag, ciphertext}` for
* AES-256-GCM with a 12-byte nonce and a 16-byte tag. The authenticated
* additional data is a length-prefixed transcript over the scheme, suite,
* keyId and framing, so the ciphertext can only be opened for the exact field
* shape it was sealed for.
*
* The field key itself is NOT a user secret: it is a build-time constant
* compiled into the app's own Electron native module
* (`electron_browser_workbuddy_storage`). The app fetches it through
* `loggerGet()` and hashes the returned base64 STRING (not the decoded bytes)
* to obtain the 32-byte key; `keyId` is the first 16 hex characters of that
* key's SHA-256.
*
* This module re-derives the same key by asking the installed app for the same
* payload, and caches it in memory for the process lifetime. Nothing is ever
* written to disk, and the payload is never logged.
*
* 改动：**「macOS 也加密」这一事实**（issue #15 真机取证）。本模块原先假设该
*   policy 是 Windows 先行、macOS 只是「将来可能」，于是 macOS 的可执行文件
*   路径用 App 名拼成 `<bundle>/Contents/MacOS/WorkBuddy`——而两个真实 bundle
*   的 `CFBundleExecutable` 都是 `Electron`，该路径并不存在。结果是 macOS 上
*   加密凭据**永远**取不到密钥，用户却被报成「未登录」。现在二进制名向 bundle
*   自己问（`macosBundleExecutable()`），候选含国际版 `WorkBuddy AI.app`，
*   并允许 App 被归入 applications 目录的子目录——扫到的候选必须先用
*   `CFBundleIdentifier` 确认身份才 `execFile`，因为**每个 Electron 应用的
*   二进制都叫 `Electron`**，只按名字匹配就可能启动另一个产品。
*
* @module dsh-connect-workbuddy/at-rest
*/
/** Envelope framing names, mapped to the single-byte AAD framing code. */
const FRAMING_CODE = {
	file: 1,
	field: 2,
	record: 3,
	stream: 4
};
/** Standard (symmetric) format identifiers, transcripted into the AAD. */
const STANDARD_FORMAT_ID = {
	file: "WBEF1",
	field: "WBEV1",
	record: "WBER1",
	stream: "WBES1"
};
/** Domain separator the AAD transcript starts with. */
const AAD_DOMAIN = Buffer.from("WB-AAD\0", "ascii");
/** Scheme name of the symmetric envelope this module opens. */
const SYMMETRIC_SCHEME = "sym-v1";
/** Env override pointing at the WorkBuddy desktop executable. */
const WORKBUDDY_APP_EXECUTABLE_ENV = "WORKBUDDY_APP_EXECUTABLE";
/** How long the app is given to answer with its key payload. */
const KEY_FETCH_TIMEOUT_MS = 1e4;
/**
* File names the WorkBuddy desktop executable goes by on Windows.
*
* `WorkBuddy.exe` is the domestic build. The international build ships as
* **`WorkBuddyAI.exe` under a `WorkBuddyAI` directory** (issue #68), and the
* probe list below carries no candidate that matched it — so a machine with only
* the international app installed could not find the key payload's source and
* reported the credentials as unreadable, which the UI then described as a
* wrong-region sign-in.
*
* Kept as a LIST rather than a second constant because the same name test is
* used in two places that must agree: the directory joiners below, and the
* registry-value validator (`normalizeRegistryExecutable`), which must accept a
* `DisplayIcon` pointing at either binary. The first entry is the canonical one
* used when a directory is known to hold the domestic build.
*/
const APP_EXECUTABLE_NAMES = ["WorkBuddy.exe", "WorkBuddyAI.exe"];
/** The canonical executable name; the rest of {@link APP_EXECUTABLE_NAMES}. */
const APP_EXECUTABLE_NAME = APP_EXECUTABLE_NAMES[0];
/**
* Windows install directories the desktop app is observed to use, in probe
* order, relative to a base such as `%LOCALAPPDATA%` or `%ProgramFiles%`.
*
* `Programs\WorkBuddy` is the common per-user layout; `WorkBuddy` is the flatter
* one some installers produce. The `WorkBuddyAI` pair is the international
* build's layout (issue #68) — both spellings are carried because the directory
* and the binary were observed to agree, but a future build could pair either
* way, and a miss here is invisible (it surfaces as "unreadable credentials").
*/
const WINDOWS_APP_SUBDIRECTORIES = [
	["Programs", "WorkBuddy"],
	["WorkBuddy"],
	["Programs", "WorkBuddyAI"],
	["WorkBuddyAI"]
];
/**
* macOS bundles the desktop app may be installed as, in probe order.
*
* `WorkBuddy.app` is the domestic build; `WorkBuddy AI.app` is the
* international one, and a machine may carry either or both. The user-level
* `~/Applications` location is included because macOS lets an app live there,
* and installs have been observed under a subdirectory of /Applications too —
* hence {@link findWorkbuddyAppExecutable}'s parent scan, which covers those
* without guessing any particular folder name.
*/
const MACOS_APP_BUNDLE_NAMES = ["WorkBuddy.app", "WorkBuddy AI.app"];
function encodeUint32(value) {
	const bytes = Buffer.allocUnsafe(4);
	bytes.writeUInt32BE(value);
	return bytes;
}
/** Length-prefixed UTF-8 string: uint32 big-endian length followed by the bytes. */
function encodeLengthPrefixed(value) {
	const bytes = Buffer.from(value, "utf8");
	return Buffer.concat([encodeUint32(bytes.length), bytes]);
}
/**
* The authenticated additional data for one `sym-v1` FIELD-framed envelope.
*
* Only the field framing is implemented: it is the shape the desktop app uses
* for credential fields, and it is also the shape that cannot be confused with
* a whole-file envelope, so an unexpected framing is a parse error rather than
* a silently wrong transcript.
*/
function fieldAad(keyId, suite, scheme = SYMMETRIC_SCHEME) {
	if (!/^[0-9a-f]{16}$/u.test(keyId)) throw new Error(`workbuddy: envelope keyId is malformed`);
	return Buffer.concat([
		AAD_DOMAIN,
		Buffer.from([1]),
		encodeLengthPrefixed(STANDARD_FORMAT_ID["field"]),
		encodeLengthPrefixed(scheme),
		encodeUint32(suite),
		encodeLengthPrefixed(keyId),
		Buffer.from([FRAMING_CODE["field"]]),
		Buffer.from([0]),
		Buffer.from([0])
	]);
}
/** Whether a value is the app's encrypted-field wrapper. */
function isEncryptedFieldWrapper(value) {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
	const wrapper = value;
	const keys = Object.keys(wrapper).sort();
	return keys.length === 2 && keys[0] === "$wbEncrypted" && keys[1] === "envelope" && wrapper["$wbEncrypted"] === 1 && typeof wrapper["envelope"] === "string";
}
/**
* The at-rest key id for a derived 32-byte key: the first 16 hex characters of
* its SHA-256. This is what the envelope's `keyId` is checked against, so a
* mismatched key fails loudly instead of returning garbage.
*/
function deriveAtRestKeyId(key) {
	return createHash("sha256").update(key).digest("hex").slice(0, 16);
}
/**
* Derive the 32-byte field key from the app's key payload JSON.
*
* The app hashes the payload's base64 STRING — not its decoded bytes — so the
* same spelling is required here; hashing the decoded secret would produce a
* different key and every field would fail to open.
*/
function deriveAtRestKey(payloadJson) {
	let payload;
	try {
		payload = JSON.parse(payloadJson);
	} catch {
		throw new Error("workbuddy: at-rest key payload is not valid JSON");
	}
	if (typeof payload !== "object" || payload === null || Array.isArray(payload)) throw new Error("workbuddy: at-rest key payload is not an object");
	const secret = payload["atRestSecretKey"];
	if (typeof secret !== "string" || secret === "") throw new Error("workbuddy: at-rest key payload carries no atRestSecretKey");
	return createHash("sha256").update(secret, "utf8").digest();
}
/**
* Open one encrypted field with a derived key and return its plaintext.
*
* Throws when the envelope is malformed, belongs to another key, or fails
* authentication — a GCM tag mismatch is the signal that the transcript or the
* key is wrong, and it must never degrade into a truncated token.
*/
function openEncryptedField(field, key) {
	let envelope;
	try {
		envelope = JSON.parse(Buffer.from(field.envelope, "base64").toString("utf8"));
	} catch {
		throw new Error("workbuddy: encrypted field envelope is not valid JSON");
	}
	if (typeof envelope !== "object" || envelope === null || Array.isArray(envelope)) throw new Error("workbuddy: encrypted field envelope is not an object");
	const record = envelope;
	const suite = record["suite"];
	const keyId = record["keyId"];
	const nonce = record["nonce"];
	const authTag = record["authTag"];
	const ciphertext = record["ciphertext"];
	if (typeof suite !== "number" || typeof keyId !== "string") throw new Error("workbuddy: encrypted field envelope is missing suite or keyId");
	if (typeof nonce !== "string" || typeof authTag !== "string" || typeof ciphertext !== "string") throw new Error("workbuddy: encrypted field envelope is missing nonce, authTag or ciphertext");
	const expectedKeyId = deriveAtRestKeyId(key);
	if (keyId !== expectedKeyId) throw new Error(`workbuddy: encrypted field belongs to key ${keyId}, not the available key ${expectedKeyId}`);
	const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(nonce, "base64"), { authTagLength: 16 });
	decipher.setAAD(fieldAad(keyId, suite));
	decipher.setAuthTag(Buffer.from(authTag, "base64"));
	return Buffer.concat([decipher.update(Buffer.from(ciphertext, "base64")), decipher.final()]).toString("utf8");
}
/**
* The executable inside a macOS app bundle, read from the bundle's own
* `Info.plist`.
*
* The binary is NOT reliably named after the app: the WorkBuddy bundles ship
* with `CFBundleExecutable` set to `Electron`, so a path assembled as
* `<bundle>/Contents/MacOS/WorkBuddy` does not exist and the app looks absent
* even when it is installed in the default location. Because the bundle
* documents the real name, asking it is both correct and robust to a future
* build that renames the binary.
*
* Returns undefined when the plist is absent, unreadable, or carries no usable
* name — never a guessed path, so a caller can keep probing.
*/
function macosBundleExecutable(bundle) {
	let plist;
	try {
		plist = readFileSync(join(bundle, "Contents", "Info.plist"), "utf8");
	} catch {
		return;
	}
	const name = /<key>\s*CFBundleExecutable\s*<\/key>\s*<string>([^<]*)<\/string>/u.exec(plist)?.[1]?.trim();
	if (name === void 0 || name === "" || name.includes("/") || name.includes("\\") || name === "." || name === "..") return;
	return join(bundle, "Contents", "MacOS", name);
}
/**
* The executable path WorkBuddy's own uninstall registration points at, or
* undefined when the app was never registered.
*
* Why this exists: the four directory candidates in
* {@link workbuddyAppExecutableCandidates} encode the DEFAULT install layout.
* An app installed anywhere else — a second drive (the observed case was
* `E:\workbuddy\WorkBuddy.exe`) — is installed and signed in, yet every
* candidate misses, so the encrypted credential file cannot be opened and a
* user who IS signed in is reported as signed out. The README's answer was to
* set `WORKBUDDY_APP_EXECUTABLE` by hand; the installer already recorded the
* answer, so this reads it instead.
*
* The registration is the app's own claim about itself, which is what makes it
* safe to hand the path to the credential probe: a display name alone would be
* a guess, whereas the path here was written by the installer that placed the
* binary. The value is still checked against {@link APP_EXECUTABLE_NAME} and for
* existence before use, and the callers fall through to the documented hint when
* it is absent.
*
* The read is done through ALL THREE uninstall views, because the hive a
* registration lands in depends on how the app was installed: per-user
* (`/currentuser`, the layout WorkBuddy uses) registers under `HKCU`, a
* machine-wide install under `HKLM`, and a 32-bit machine-wide one under
* `HKLM\...\WOW6432Node`. A hive that does not exist or is unreadable is the
* normal case on a machine without the app, not an error: every failure path
* returns undefined so the caller can try the next candidate.
*/
function windowsRegistryAppExecutable(query = queryRegistry) {
	for (const root of [
		"HKCU\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Uninstall",
		"HKLM\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Uninstall",
		"HKLM\\SOFTWARE\\WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall"
	]) {
		const listing = query([
			"query",
			root,
			"/s",
			"/f",
			"WorkBuddy",
			"/t",
			"REG_SZ"
		]);
		if (listing === void 0) continue;
		const executable = registryExecutableFromQuery(listing);
		if (executable !== void 0) return executable;
		const fromDirectory = registryInstallLocationFromQuery(listing);
		if (fromDirectory !== void 0) return fromDirectory;
	}
}
/**
* Parse `reg query` output for the value that names the installed executable.
*
* `reg query` prints keys as `HKEY_CURRENT_USER\...\Uninstall\<id>` followed by
* indented `    ValueName    REG_SZ    Data` lines. Only `DisplayIcon` is
* trusted here: it is the one value the installer writes pointing AT the
* binary. The value is normalised by {@link normalizeRegistryExecutable}, which
* strips the quotes and the icon index and rejects anything that is not the
* desktop executable.
*/
function registryExecutableFromQuery(output) {
	for (const rawLine of output.split(/\r?\n/u)) {
		const match = /^\s*DisplayIcon\s+REG_SZ\s+(.*?)\s*$/u.exec(rawLine);
		if (match === null) continue;
		const executable = normalizeRegistryExecutable(match[1]);
		if (executable !== void 0) return executable;
	}
}
/**
* The `WorkBuddy.exe` under an `InstallLocation`-style registry directory.
*
* Kept separate from {@link registryExecutableFromQuery} because the two values
* mean different things: `DisplayIcon` NAMES a file, whereas the directory has
* to have {@link APP_EXECUTABLE_NAME} joined onto it, and the result must still
* be checked for existence by the caller's probe.
*
* `win32.join` rather than the host's `join`: the input is a Windows registry
* value on EVERY machine this function can be called on. `path.join` is the
* running platform's, so a suite on Linux/macOS would mangle `E:\workbuddy`
* into `E:\workbuddy/WorkBuddy.exe` and the caller's `existsSync` could never
* match it. The separator here is fixed by the DATA, not by the host.
*/
function registryInstallLocationFromQuery(output) {
	for (const rawLine of output.split(/\r?\n/u)) {
		const match = /^\s*InstallLocation\s+REG_SZ\s+(.*?)\s*$/u.exec(rawLine);
		if (match === null) continue;
		const directory = stripRegistryQuotes(match[1]);
		if (directory === void 0 || directory === "") continue;
		return win32.join(directory, APP_EXECUTABLE_NAME);
	}
}
/**
* A registry executable value reduced to a usable path, or undefined when it
* cannot be one.
*
* `DisplayIcon` is stored as `"E:\workbuddy\WorkBuddy.exe",0`. The quotes and
* the icon index are stripped, and a value that names something other than the
* desktop executable — an `.ico`, an uninstaller, another product's binary — is
* rejected rather than launched. A bare path without quotes is accepted too,
* because not every installer writes the indexed form.
*
* The basename test uses `win32.basename` for the reason given on
* {@link registryInstallLocationFromQuery}: the separator belongs to the
* registry value (always Windows), so the host's `basename` — which does not
* treat `\` as a separator — would return the whole string and reject every
* legitimate path.
*/
function normalizeRegistryExecutable(value) {
	const unquoted = stripRegistryQuotes(value);
	if (unquoted === void 0 || unquoted === "") return void 0;
	const withoutIndex = unquoted.replace(/,\s*-?\d+\s*$/u, "");
	if (withoutIndex === "") return void 0;
	const base = win32.basename(withoutIndex).toLowerCase();
	if (!APP_EXECUTABLE_NAMES.some((name) => name.toLowerCase() === base)) return void 0;
	return withoutIndex;
}
/**
* The path inside a `DisplayIcon`/`InstallLocation` value, unquoted.
*
* Registry data may be written either quoted (`"E:\workbuddy\WorkBuddy.exe",0`)
* or bare. Only a leading quote is treated as quoting: a bare `C:\dir\a"b.exe`
* is a real (if unusual) filename and must not be truncated at the quote.
*/
function stripRegistryQuotes(value) {
	const trimmed = value.trim();
	if (trimmed === "") return void 0;
	if (!trimmed.startsWith("\"")) return trimmed;
	const closing = trimmed.indexOf("\"", 1);
	if (closing === -1) return void 0;
	return trimmed.slice(1, closing).trim();
}
/**
* Run `reg query` and return its stdout, or undefined when it cannot be run.
*
* `windowsHide` is REQUIRED, not cosmetic: the DSH Desktop host is an Electron
* GUI process with no console, so spawning a console program without it flashes
* a visible black window on every probe. The same reason is recorded for the
* heartbeat probe in `host-heartbeat.ts` and in `docs/WINDOWS.md` §1-3.
*
* `execFileSync` (not `execFile`) keeps this synchronous so the candidates list
* stays a pure function of its inputs: it is called during `findWorkbuddyApp`
* `Executable`, which `readAtRestKey()` and the `doctor` command both treat as
* a plain lookup. The read is bounded by the registry query's own output and
* happens at most once per discovery.
*/
function queryRegistry(args) {
	try {
		return execFileSync("reg", [...args], REGISTRY_PROBE_OPTIONS);
	} catch {
		return;
	}
}
/**
* Options for the registry probe; `windowsHide` is mandatory (see above), and
* the timeout matches `fetchAtRestKeyPayload()`'s: a hung `reg` must not wedge
* the synchronous discovery path forever.
*/
const REGISTRY_PROBE_OPTIONS = {
	encoding: "utf8",
	windowsHide: true,
	timeout: 1e4
};
/**
* Candidate paths of the WorkBuddy desktop executable, in probe order.
*
* The Windows build is the one that encrypts credentials, so Windows leads;
* the macOS bundles are listed because the same native module ships there and
* the encryption policy is enabled on macOS builds too (observed from 5.6.x),
* and `undefined` entries (an unset env variable) are dropped.
*
* On macOS the executable name comes from each bundle (see
* {@link macosBundleExecutable}) rather than being assembled from the app name.
*
* `readBundleExecutable` is injectable, in the same spirit as `platform`/`home`/
* `env`: the macOS branch consults the real filesystem, so without a seam the
* expected candidates would depend on whether the host machine happens to have
* the app installed — and the test would pass on a developer's Mac while
* failing in CI.
*/
function workbuddyAppExecutableCandidates(platform = process.platform, home = homedir(), env = process.env, readBundleExecutable = macosBundleExecutable) {
	const candidates = [env[WORKBUDDY_APP_EXECUTABLE_ENV]?.trim()];
	if (platform === "win32") {
		const local = env["LOCALAPPDATA"]?.trim();
		const programFiles = env["ProgramFiles"]?.trim();
		const programFilesX86 = env["ProgramFiles(x86)"]?.trim();
		const bases = [
			local === void 0 || local === "" ? void 0 : local,
			programFiles === void 0 || programFiles === "" ? void 0 : programFiles,
			programFilesX86 === void 0 || programFilesX86 === "" ? void 0 : programFilesX86
		];
		for (const base of bases) {
			if (base === void 0) continue;
			for (const subdirectory of WINDOWS_APP_SUBDIRECTORIES) for (const name of APP_EXECUTABLE_NAMES) candidates.push(win32.join(base, ...subdirectory, name));
		}
	} else if (platform === "darwin") for (const name of MACOS_APP_BUNDLE_NAMES) candidates.push(readBundleExecutable(join("/Applications", name)), readBundleExecutable(join(home, "Applications", name)));
	return candidates.filter((candidate) => candidate !== void 0 && candidate !== "");
}
/**
* Bundle identifier PREFIXES the desktop app is signed with — `com.tencent.
* workbuddy` (domestic, observed as `…workbuddy.mac`) and `com.workbuddy`
* (international, observed as `com.workbuddy.workbuddy-ai`).
*
* Used to CONFIRM that a discovered bundle really is WorkBuddy before it is
* launched. This matters because the discovery below scans directories and then
* execs what it finds: every Electron app is built around a binary called
* `Electron`, so a name-only match could pick a different product's bundle and
* run it. The identifier is the app's own claim about itself, so it is the
* check that makes the scan safe.
*/
const APP_BUNDLE_IDENTIFIER_PREFIXES = ["com.tencent.workbuddy", "com.workbuddy"];
/**
* Whether a bundle identifies itself as the WorkBuddy desktop app.
*
* The match is on dot boundaries, so a hypothetical `com.workbuddyish` cannot
* pass as `com.workbuddy`.
*
* An unreadable or identifier-less plist is treated as NOT WorkBuddy: refusing
* a candidate only costs a fallback to another path, whereas accepting the
* wrong one would execute an unrelated application.
*/
function isWorkbuddyBundle(bundle) {
	let plist;
	try {
		plist = readFileSync(join(bundle, "Contents", "Info.plist"), "utf8");
	} catch {
		return false;
	}
	const identifier = /<key>\s*CFBundleIdentifier\s*<\/key>\s*<string>([^<]*)<\/string>/u.exec(plist)?.[1]?.trim().toLowerCase();
	if (identifier === void 0 || identifier === "") return false;
	return APP_BUNDLE_IDENTIFIER_PREFIXES.some((prefix) => identifier === prefix || identifier.startsWith(`${prefix}.`));
}
/**
* Bundles of the desktop app found one level BELOW a macOS applications
* directory.
*
* Users do file apps into subfolders (`/Applications/IDE/WorkBuddy.app`), and
* a hardcoded `/Applications/<name>` then reports the app as missing while it
* is installed and signed in. The scan is deliberately ONE level deep and
* matches the known bundle names only, so it stays predictable and cheap; each
* candidate is then confirmed by {@link isWorkbuddyBundle} before use.
*
* Returns [] when the parent is absent or unreadable — a missing directory is
* the normal case, not an error.
*/
function macosNestedAppBundles(parent) {
	let entries;
	try {
		entries = readdirSync(parent);
	} catch {
		return [];
	}
	const bundles = [];
	for (const entry of entries) {
		const nested = join(parent, entry);
		for (const name of MACOS_APP_BUNDLE_NAMES) {
			const bundle = join(nested, name);
			try {
				if (!statSync(bundle).isDirectory()) continue;
			} catch {
				continue;
			}
			if (isWorkbuddyBundle(bundle)) bundles.push(bundle);
		}
	}
	return bundles;
}
/**
* Locate the desktop executable AND say how it was found.
*
* Kept beside {@link findWorkbuddyAppExecutable} rather than folded into it
* because the two answer different questions: callers on the credential path
* only want the path, whereas `doctor` must explain the discovery. Sharing the
* candidate list keeps them from drifting.
*/
function findWorkbuddyAppExecutableWithSource(platform = process.platform, home = homedir(), env = process.env, readRegistryAppPath = windowsRegistryAppExecutable) {
	const fromEnv = env[WORKBUDDY_APP_EXECUTABLE_ENV]?.trim();
	for (const candidate of workbuddyAppExecutableCandidates(platform, home, env)) {
		try {
			if (!existsSync(candidate)) continue;
		} catch {
			continue;
		}
		return {
			executable: candidate,
			source: fromEnv !== void 0 && fromEnv !== "" && candidate === fromEnv ? "env" : "default-layout"
		};
	}
	if (platform === "win32") {
		const fromRegistry = readRegistryAppPath();
		if (fromRegistry !== void 0) try {
			if (existsSync(fromRegistry)) return {
				executable: fromRegistry,
				source: "registry"
			};
		} catch {}
	}
	const executable = findWorkbuddyAppExecutable(platform, home, env, readRegistryAppPath);
	return executable === void 0 ? void 0 : {
		executable,
		source: "nested-bundle"
	};
}
/**
* The first candidate that exists as a file, or undefined when the desktop app
* is not installed where this platform expects it.
*/
function findWorkbuddyAppExecutable(platform = process.platform, home = homedir(), env = process.env, readRegistryAppPath = windowsRegistryAppExecutable) {
	for (const candidate of workbuddyAppExecutableCandidates(platform, home, env)) try {
		if (existsSync(candidate)) return candidate;
	} catch {}
	if (platform === "win32") {
		const fromRegistry = readRegistryAppPath();
		if (fromRegistry !== void 0) try {
			if (existsSync(fromRegistry)) return fromRegistry;
		} catch {}
	}
	if (platform === "darwin") for (const parent of ["/Applications", join(home, "Applications")]) for (const bundle of macosNestedAppBundles(parent)) {
		const executable = macosBundleExecutable(bundle);
		if (executable === void 0) continue;
		try {
			if (existsSync(executable)) return executable;
		} catch {}
	}
}
/**
* Ask the installed desktop app for its key payload by running its own binary
* as plain Node (`ELECTRON_RUN_AS_NODE`) and calling the native binding.
*
* The binding is the app's own public surface for this value, so the plugin
* never has to carry a copy of a build-specific constant: it asks the very
* build that wrote the file. The child is given no stdin and a hard timeout,
* and its stdout is the only thing read.
*/
function fetchAtRestKeyPayload(executable) {
	return new Promise((resolve, reject) => {
		execFile(executable, ["-e", "try{process.stdout.write(process._linkedBinding('electron_browser_workbuddy_storage').loggerGet())}catch(e){process.exitCode=3;process.stderr.write(String(e&&e.message||e))}"], {
			env: {
				...process.env,
				ELECTRON_RUN_AS_NODE: "1"
			},
			timeout: KEY_FETCH_TIMEOUT_MS,
			windowsHide: true,
			maxBuffer: 1048576
		}, (error, stdout, stderr) => {
			if (error !== null) {
				reject(/* @__PURE__ */ new Error(`workbuddy: the desktop app did not provide its at-rest key (${stderr.trim() || error.message})`));
				return;
			}
			const payload = stdout.trim();
			if (payload === "") {
				reject(/* @__PURE__ */ new Error("workbuddy: the desktop app returned an empty at-rest key payload"));
				return;
			}
			resolve(payload);
		});
	});
}
/** Process-lifetime cache of the derived key; never persisted. */
let cachedKey;
let inflightKey;
/**
* When the last FAILED lookup finished, epoch ms; 0 means "no recent failure".
*
* Issue #77: only a SUCCESS was remembered, so a machine where the key cannot
* be obtained re-ran the whole discovery on every single probe. `scanAll()`
* probes one auth file at a time, and a usage request scans several times, so
* one page open re-ran the discovery dozens of times — each run being three
* SYNCHRONOUS `reg query` child processes on the host's main thread when the
* desktop app is not installed (measured 15.3 s of a 19.1 s request, 135
* `reg.exe` spawns). A failure is therefore remembered for
* {@link AT_REST_KEY_FAILURE_TTL_MS}: the answer cannot change within one page
* open, while a user who installs or launches the app still sees it pick up
* within that window instead of only after a restart.
*/
let failedKeyAtMs = 0;
/** The discovery result behind the current key (or failure); see {@link discoverAppExecutable}. */
let cachedAppExecutable;
/**
* How long a failed key lookup is remembered. Long enough to collapse the
* dozens of probes one request makes, short enough that installing (or
* launching) the desktop app takes effect without a restart.
*/
const AT_REST_KEY_FAILURE_TTL_MS = 6e4;
/**
* How long a NEGATIVE executable discovery is remembered.
*
* The negative answer is the expensive one: every candidate path misses, then
* the Windows fallback shells out to `reg query` once per uninstall hive. The
* positive answer is cached for the process lifetime instead — its path is
* retried with `existsSync` on every call anyway, so an app that was moved or
* removed still falls through to a fresh discovery.
*/
const APP_EXECUTABLE_MISS_TTL_MS = 6e4;
/**
* Locate the desktop executable through {@link findWorkbuddyAppExecutable},
* remembering a MISS for {@link APP_EXECUTABLE_MISS_TTL_MS}.
*
* Only this module's own lookup is cached, and only when it goes through the
* real seams: `findWorkbuddyAppExecutable` stays a pure function of its
* arguments for `doctor` and for the injected-platform tests, which must keep
* seeing the machine as it is right now.
*/
function discoverAppExecutable() {
	const now = Date.now();
	if (cachedAppExecutable !== void 0) {
		if (now - cachedAppExecutable.foundAtMs < APP_EXECUTABLE_MISS_TTL_MS) return cachedAppExecutable.executable;
		cachedAppExecutable = void 0;
	}
	const executable = findWorkbuddyAppExecutable();
	if (executable === void 0) {
		cachedAppExecutable = {
			executable: "",
			foundAtMs: now
		};
		return;
	}
	cachedAppExecutable = {
		executable,
		foundAtMs: Number.POSITIVE_INFINITY
	};
	return executable;
}
/**
* The desktop app's at-rest field key, or undefined when it cannot be obtained
* (app not installed, an older build without the native module, or a future
* build that rotates the payload).
*
* Both outcomes are cached: a success for the process lifetime (the app is
* spawned at most once), a failure for {@link AT_REST_KEY_FAILURE_TTL_MS}. The
* failure TTL is what keeps a machine WITHOUT the app from paying the
* synchronous registry probe on every credential file — see {@link failedKeyAtMs}.
*/
function readAtRestKey() {
	if (cachedKey !== void 0) return Promise.resolve(cachedKey);
	if (inflightKey !== void 0) return inflightKey;
	if (failedKeyAtMs !== 0 && Date.now() - failedKeyAtMs < AT_REST_KEY_FAILURE_TTL_MS) return Promise.resolve(void 0);
	inflightKey = (async () => {
		const executable = discoverAppExecutable();
		if (executable === void 0) return void 0;
		return deriveAtRestKey(await fetchAtRestKeyPayload(executable));
	})().then((key) => {
		cachedKey = key;
		failedKeyAtMs = key === void 0 ? Date.now() : 0;
		return key;
	}, (error) => {
		failedKeyAtMs = Date.now();
		throw error;
	}).finally(() => {
		inflightKey = void 0;
	});
	return inflightKey;
}
/** Drop the cached key; tests and diagnostics only. */
function clearAtRestKeyCache() {
	cachedKey = void 0;
	inflightKey = void 0;
	failedKeyAtMs = 0;
	cachedAppExecutable = void 0;
}
//#endregion
//#region src/off-thinking.ts
/**
* Ids observed to REFUSE `reasoning_effort: "off"` with HTTP 400 despite
* declaring `canDisableThinking: true` (issue #34).
*
* Keyed by exact model id and deliberately NOT by region: `deepseek-v4.1-flash`
* refuses on BOTH gateways, and the split tracks the model family rather than
* the gateway. The cost is asymmetric — offering a level that always fails
* costs the user every request for that model, while hiding one that would have
* worked costs a single option — so the table errs towards hiding, and
* {@link withEffectiveOff} enforces it as a HARD wall rather than a seed
* (issue #83: a tick used to reopen the failing level, and with no
* `reasoningEffort` passed the level became the default for compaction too).
* See {@link offRefused} for why the checkbox must not win.
*
* `deepseek-v4-pro` was measured on the CN gateway only; it is listed because
* its sibling does refuse on both, and the same asymmetry argues for hiding.
*/
const REFUSES_OFF_MODEL_IDS = /* @__PURE__ */ new Set([
	"deepseek-v4.1-flash",
	"deepseek-v4.1-flash-sg",
	"deepseek-v4-pro",
	"primary-model",
	"gpt-6-astra",
	"gpt-5.6-sol",
	"gpt-5.6-terra",
	"gpt-5.6-luna",
	"gemini-3.5-flash"
]);
/**
* Whether `off` should be offered for a model when the user has expressed no
* opinion: the upstream's declaration, minus the ids known to refuse it.
*
* Only an explicit `canDisableThinking: true` enables the level; absence is
* "unknown" and stays unoffered, matching how the rest of the plugin treats an
* undeclared capability.
*/
function offDefaultFor(info) {
	return info.reasoning?.canDisableThinking === true && !REFUSES_OFF_MODEL_IDS.has(info.id);
}
/**
* The model ids the `off` level should be offered for by default (issue #34).
*
* The user's checkbox is a plain set: ticked = offered, unticked = withdrawn.
* This is the seed — what the set is filled with the first time it is read —
* so a profile that has never saved shows the correct default immediately,
* without requiring a save.
*
* Only an explicit `canDisableThinking: true` enables the level; absence is
* "unknown" and stays unoffered, matching how the rest of the plugin treats an
* undeclared capability.
*/
function defaultOffModelIds(models) {
	return models.filter(offDefaultFor).map((model) => model.id);
}
/** How many observations are kept, newest first. Bounded so a refusal storm cannot grow memory. */
const REFUSAL_RING = 20;
/**
* The refusals this process has observed, newest first.
*
* Process-scoped rather than persisted: it is EVIDENCE ("what did the upstream
* actually say"), and a stale record from another install would promote a model
* into the refusal table on the strength of a request the current build never
* sent. The card reads it; nothing sends from it.
*/
const observedRefusals = [];
/** Record one refusal, keeping the newest {@link REFUSAL_RING}. */
function recordRefusal(report) {
	observedRefusals.unshift(report);
	if (observedRefusals.length > REFUSAL_RING) observedRefusals.length = REFUSAL_RING;
}
/** The most recent refusal naming `modelId`, or `undefined` when none did. */
function lastRefusalFor(modelId) {
	return observedRefusals.find((report) => report.modelId === modelId);
}
/**
* Whether a model may offer `off` AT ALL, no matter what the user asked for.
*
* This is the hard wall in front of {@link REFUSES_OFF_MODEL_IDS} (issue #83).
* The table above used to be a seed only: the checkbox wrote the selection
* straight into `canDisableThinking`, so ticking a model known to refuse `off`
* reopened exactly the failure the table exists to prevent — and not once per
* "off" turn either. pi-ai's `openai-completions` reads
* `model.thinkingLevelMap?.off` whenever the caller passes NO
* `reasoningEffort` at all, and a compaction summary is such a caller: it never
* carries an effort. So one tick made every `/compact` for that model answer
* HTTP 400 / code 11133, with the surface showing only "could not produce a
* useful summary".
*
* The asymmetry that writes the table also writes this test: a level that
* always fails costs the user EVERY request for that model, while a level that
* was hidden needlessly costs one option. So the refusal list wins over the
* checkbox, not the other way round.
*
* The table is keyed by EXACT id, so anything that pins a different id at
* request time (aliases such as `primary-model`) is out of its reach; those are
* covered where the request is actually built — see
* {@link normalizePreparedReasoning}.
*/
function offRefused(modelId) {
	return REFUSES_OFF_MODEL_IDS.has(modelId);
}
/**
* Stamp the user's saved `off` selection onto a model list, for the RUNTIME
* catalog.
*
* The runtime model descriptor is built by `workBuddyThinkingLevelMap`, which
* reads only the model's own declaration — so the corrected answer has to be
* written onto the model before it reaches the adapter. Doing it here keeps the
* adapter a pure "declared capability in, level map out" function and keeps the
* table next to the rule it feeds.
*
* This OVERWRITES the declared field rather than adding a sibling, which is
* safe because the declared value has exactly one consumer (that same level
* map) and is never persisted where the card can see it: `toWebModel` drops
* `canDisableThinking`, and the card's `lastCatalog` is written from the raw
* upstream discovery, never from this stamped list. A model that declares no
* reasoning is returned untouched — there is no level to correct.
*
* A refusal-table id is stamped `false` REGARDLESS of the selection
* ({@link offRefused}): the tick stays in the saved set — the card shows it and
* the hint explains it — but it can no longer be turned into a request that
* fails.
*/
function withEffectiveOff(models, selection) {
	return models.map((model) => model.reasoning === void 0 ? model : {
		...model,
		reasoning: {
			...model.reasoning,
			canDisableThinking: selection.has(model.id) && !offRefused(model.id)
		}
	});
}
//#endregion
//#region src/upstream.ts
/**
* WorkBuddy (CodeBuddy / copilot.tencent.com) upstream client: chat streaming,
* token refresh, model catalog, and credit balance.
*
* 参考：corrinehu/dsh-workbuddy-connect（MIT，Copyright (c) 2026 Corrine Hu）
*   — 端点与 wire behavior 由其实现，上游协议本身参照
*     Sliverkiss/workbuddy2api（MIT）。照搬的部分：按 domain 选择
*     CN/global base、强制 stream:true、tool_choice 压平为字符串、
*     CLI 形态请求头、chat 请求绝不携带 refresh token 的安全红线、
*     中英文额度不足标记与错误分类、token 刷新的 X-Refresh-Token 头。
* 改动：原版的 `fetchModels` 只保留 id/name/maxInputTokens/maxOutputTokens，
*   丢弃了上游其余 20 个字段。实测上游每个模型还给出 `credits` 积分倍率、
*   `supportsImages` 多模态、`reasoning.supportedEfforts` 推理档位、
*   `descriptionZh/En` 描述等，这些正是模型管理卡片所需的信息，
*   本实现将其完整解析（解析不出则留空，不虚构）。
*   `reasoning` 另有两种拼写：复数 `supportedEfforts` 形态与单数 `effort`
*   形态（只声明默认档；两网关实测这些模型接受全阶梯，不发参数即不
*   思考），解析层将单数形态折叠为复数（见 parseReasoning，issue #7）。
*   另：积分接口改为按套餐名聚合，实测单个账号下同名「运营裂变包」
*   可达 19 个，逐条渲染会淹没卡片。
*   模型目录路径改为 `/v2/enterprises/personal/models`（原版为
*   `/console/enterprises/personal/models`）：两版官方桌面 App 均调用
*   `/v2` 形态，CN 网关两条路径逐字节同答，而国际版网关（workbuddy.ai）
*   只答 `/v2`、`/console` 形态返回 HTTP 500——统一 `/v2` 即同时覆盖
*   国内版与国际版（WorkBuddy AI）账号，区域由 `domain` 自动路由。
*
* @module dsh-connect-workbuddy/upstream
*/
/**
* The write half of each campaign: the endpoint that actually grants the
* credits.
*
* Kept beside the status paths because they must move together: the pair is
* what the 「套餐与积分」 page itself uses (`xp`/`vp` in its bundle), and a
* campaign whose status is read from one path can only be claimed through its
* matching claim path. Note the same absence of a `/v2` prefix.
*/
const BENEFIT_CLAIM_PATHS = {
	compensation: "/billing/meter/claim-compensation",
	gift: "/billing/meter/claim-gift"
};
const CN_CHAT_BASE = "https://copilot.tencent.com";
const CN_BILLING_BASE = "https://www.codebuddy.cn";
const GLOBAL_BASE = "https://www.workbuddy.ai";
/**
* Legacy model-catalog path, kept only as the CN region's FALLBACK.
*
* This is the document the WorkBuddy *plugin* used to read, and it is NOT the
* document the WorkBuddy *app* reads. The gateway answers it with the CLI
* channel's roster, whose second slot is the paid `hy4-preview`
* (`credits: 'x0.29 credits'`), while the app's own config lists the free
* `hy4-preview-f` (`credits: 'x0.00 credits'`) in that slot under the SAME
* display name "Hy4 preview". Reading this path is therefore what makes the
* plugin's card disagree with the app about a model's price — both are
* correctly displaying a real record, just different ones.
*/
const MODELS_CATALOG_PATH = "/v2/enterprises/personal/models";
/**
* Remote product-config path; the CN region's PRIMARY catalog source and the
* global region's only one.
*
* It is field-compatible with {@link MODELS_CATALOG_PATH} for every key
* {@link parseUpstreamModel} reads, so one parser still serves both. On CN this
* path is what the app itself consumes, so the plugin's roster and the app's
* agree — that parity is the whole point of preferring it.
*
* The CLI user agent deliberately stays {@link CLIENT_UA} here. `/v3/config`
* serves a DIFFERENT roster per client channel: the desktop token yields a
* roster without the free `hy4-preview-f`, and the CLI token yields one with
* it. Measured 2026-10-04 — CN + `/v3/config`: CLI token = 17 models incl.
* `hy4-preview-f`(x0.00); desktop token = 29 models, no `hy4-preview-f` and no
* cheap free tier. That is why the global branch below pairs this path with
* {@link DESKTOP_UA} and the CN branch must NOT.
*/
const GLOBAL_CONFIG_PATH = "/v3/config";
const CLIENT_UA = "CLI/2.63.2 CodeBuddy/2.63.2";
/**
* User agent of the WorkBuddy desktop app.
*
* The config service serves a DIFFERENT product configuration per client
* channel, selected by this product token — the version suffix is ignored
* (`WorkBuddy/5.5.2`, `WorkBuddy/1.0.0` and a bare `WorkBuddy` answer
* identically). On the INTERNATIONAL gateway the split decides which models
* exist at all:
*
*   - CLI channel (`CLI/… CodeBuddy/…`) → 35 models that OMIT
*     `deepseek-v4.1-flash` and `gpt-6-astra`, even though both are perfectly
*     chat-usable (verified: `deepseek-v4.1-flash` streams HTTP 200 and is
*     billed `x0.00`);
*   - desktop channel → the account's real 20-model chat roster including both.
*
* The plugin emulates the CLI channel for CHAT but reads the desktop channel's
* configuration to learn the account's actual model list. The CN gateway needs
* no such switch: its desktop config carries no `cli` agent roster at all, so
* CN keeps reading the shared `/v2/enterprises/personal/models` path.
*/
const DESKTOP_UA = "WorkBuddy/5.5.2";
const JSON_TIMEOUT_MS = 3e4;
const ERROR_BODY_LIMIT = 4096;
/**
* Ceiling for one real-volume probe to get a RESPONSE, which is not a metadata
* call.
*
* Much larger than {@link JSON_TIMEOUT_MS} on purpose: a probe posts ~25k input
* tokens, so prompt processing legitimately takes a while before the first byte
* comes back. It exists only to stop a connection that will NEVER answer — the
* pool's batch is serial, so one hung member would otherwise block all the rest
* with nothing in the UI to say which one it was. Ten seconds is comfortably
* above a working gateway's first byte and far below a user's patience; the
* budget covers the response only, never the one-line body that follows.
*/
const PROBE_TIMEOUT_MS = 1e4;
/**
* Insufficient-credit markers.
*
* ENGLISH IS MATCHED CASE-INSENSITIVELY (see {@link classifyUpstreamError}) so
* each entry is spelled once, lowercase. Chinese has no case, so those entries
* are the upstream's own wording.
*
* `credits exhausted` is listed alongside `credit exhausted` because the
* upstream ships the PLURAL — measured 2026-10-13 on a live `14018`:
*
*   {"error":{"data":{"code":14018,"msg":"Credits exhausted. Please visit the
*    link below to purchase add-on packs and get more credits: …"}}}
*
* The singular-only entry silently missed it (`'credits exhausted.'.includes(
* 'credit exhausted')` is FALSE — the `s` sits between the two words), so a
* failure whose entire point is "go top up" was reported as "wait a bit" and
* benched a healthy account for the 30-minute fallback. Both spellings are kept
* because the gateway has been observed to change its wording; neither is
* assumed to be the only one.
*/
const HARD_CREDIT_MARKERS = [
	"insufficient credit",
	"no credit",
	"credit exhausted",
	"credits exhausted",
	"out of credit",
	"quota exceeded",
	"quota exhaust",
	"payment required",
	"credit not enough",
	"not enough credit",
	"credits not enough",
	"积分不足",
	"额度不足",
	"余额不足",
	"积分用完",
	"额度用尽",
	"没有积分"
];
/**
* Business code of the exhaustion refusal (`14018`).
*
* The CODE, not the prose — see {@link HARD_CREDIT_MARKERS} for the wording bug
* this guards against. Marker tables rot in a way codes do not: the gateway can
* reword its sentence without changing the contract it advertises, and the
* failure is silent (the branch simply stops matching). A code cannot drift like
* that, and matching it in quoted-key form keeps a request id that happens to
* contain the digits from promoting an unrelated body.
*
* ONLY a corroborating signal, never a substitute: the code is checked in
* addition to the markers, so a body that names the code AND reads as a policy
* refusal still resolves by the marker table's order.
*/
const CREDITS_EXHAUSTED_CODE = "\"code\":14018";
/** Session-invalidation markers that mean "sign in again in the WorkBuddy app". */
const SESSION_DEAD_MARKERS = ["Offline user session not found", "12153"];
/**
* Content-policy refusal markers from the chat gateway. The quoted-key form
* avoids substring hits inside unrelated values (request ids are hex and can
* contain `11140` by coincidence).
*/
const POLICY_REJECT_MARKERS = ["request illegal", "\"code\":11140"];
/**
* Business code of the MODEL-scoped throttle (`14003`, "too many requests").
*
* The quoted-key form for the same reason as the policy markers: a request id is
* hexadecimal and can contain the digits `14003` by coincidence, and a bare
* substring search over an arbitrary failure body is how a red herring gets
* promoted into a routing decision.
*/
const MODEL_BUSY_CODE = "\"code\":14003";
/**
* Whether a failure body is the MODEL-scoped throttle rather than an account
* limit.
*
* `14003` and `6004` are different codes from the same gateway and mean
* different things. Measured on one credential at one moment, with requests that
* differed ONLY in `model`, three attempts each: `deepseek-v4.1-flash`
* (multiplier x0.00) answered 1/3 and refused 2/3 with `429 / 14003`, while
* `glm-5.3-flash`, `hy3` and `kimi-k2.6` answered 3/3 — the SAME account, the
* SAME second. A 6004, by contrast, carries a `将在 … UTC+8 重置` sentence and
* is per-account.
*
* So the criterion is the CODE alone, matched in its quoted-key form so a
* request id that happens to contain the digits cannot promote it. Exported so
* the probe and the chat path cannot drift into two different answers about one
* body.
*/
function isModelBusyBody(body) {
	return body.includes(MODEL_BUSY_CODE);
}
/** Classify an upstream failure from its HTTP status and body excerpt. */
function classifyUpstreamError(status, body) {
	if (status === 402) return "hard_credit";
	const lower = body.toLowerCase();
	for (const marker of HARD_CREDIT_MARKERS) if (lower.includes(marker.toLowerCase()) || body.includes(marker)) return "hard_credit";
	if (body.includes(CREDITS_EXHAUSTED_CODE)) return "hard_credit";
	for (const marker of SESSION_DEAD_MARKERS) if (body.includes(marker)) return "session_dead";
	for (const marker of POLICY_REJECT_MARKERS) if (body.includes(marker)) return "policy_reject";
	if (isModelBusyBody(body)) return "model_busy";
	if (status === 429) return "soft_rate";
	if (status === 404) return "not_found";
	if (status >= 500) return "server";
	if (status >= 400) return "client";
	return "client";
}
/**
* Read one of the upstream's localized fields, in either shape it ships: a plain
* string, or a `{ zh, en }` object.
*
* One reader for every such field on purpose. `displayMsg` and `displayTips`
* arrive in the same two shapes from the same envelope, so parsing them with two
* separate hand-rolled branches is how one of them ends up only handling the
* object form (which is exactly what happened to `displayTips`: it was never
* read at all, issue #78).
*/
function localizedText(value) {
	if (typeof value === "string") return value;
	if (typeof value !== "object" || value === null) return void 0;
	const localized = value;
	const pick = localized.zh ?? localized.en;
	return typeof pick === "string" ? pick : void 0;
}
/**
* Parse the known-good fields out of an upstream failure body. Non-JSON
* bodies (HTML error pages, empty strings) yield undefined rather than a guess.
*/
function parseUpstreamErrorDetail(body) {
	let parsed;
	try {
		parsed = JSON.parse(body);
	} catch {
		return;
	}
	if (typeof parsed !== "object" || parsed === null) return void 0;
	const raw = parsed;
	const displayMsg = localizedText(raw.displayMsg) ?? (typeof raw.msg === "string" ? raw.msg : void 0);
	const tips = localizedText(raw.displayTips);
	const detail = {};
	if (typeof raw.code === "number") detail.upstreamCode = raw.code;
	if (typeof raw.requestId === "string") detail.requestId = raw.requestId;
	if (displayMsg !== void 0) detail.displayMsg = displayMsg;
	if (tips !== void 0) detail.tips = tips;
	const ext = raw.extError;
	if (typeof ext === "object" && ext !== null && !Array.isArray(ext)) {
		const extRaw = ext;
		const param = extRaw["param"];
		if (typeof param === "string" && param !== "") detail.rejectedParam = param;
		const value = extRaw["value"];
		if (typeof value === "string" && value !== "") detail.rejectedTier = value;
	}
	return detail;
}
/**
* Region for a login domain; an empty domain means CN (matching upstream tooling).
*
* The international product is reachable under TWO brand domains: the WorkBuddy
* AI desktop app signs in at `workbuddy.ai`, while the CodeBuddy CLI signs the
* same international account in at `codebuddy.ai` (verified against a real
* credential file, issue #4). Both are served by the same gateway stack — a
* read-only probe shows `/v3/config` answering HTTP 200 with the same JSON
* envelope on both hosts — so both classify as `global`. Missing the
* `codebuddy.ai` spelling sent those tokens to the CN gateway, which rejected
* them at the openresty layer with an HTML 401.
*/
function regionOf(domain) {
	const lowered = domain.trim().toLowerCase();
	if (lowered === "workbuddy.ai" || lowered.endsWith(".workbuddy.ai")) return "global";
	if (lowered === "codebuddy.ai" || lowered.endsWith(".codebuddy.ai")) return "global";
	return "cn";
}
/**
* The STRICT counterpart of {@link regionOf}: a region's domain only when it is
* positively recognised, `undefined` for anything else.
*
* `regionOf`'s catch-all is deliberate — the CN product is single-homed, so an
* unrecognised spelling is treated as CN and a real (if oddly written) CN
* credential still resolves. That reasoning holds only for a domain the
* UPSTREAM issued. It does not hold for a value read out of a file someone
* handed the plugin: there "unrecognised" means "cannot be classified", and
* calling it CN would file a foreign or spoofed credential in the CN vault.
* Import gates must use this, not {@link regionOf}.
*
* Both sides are matched on their registrable domain, so a bare host and any
* subdomain qualify while a lookalike such as `codebuddy.cn.evil.com` cannot.
*
* `copilot.tencent.com` is NOT a third product — it is this plugin's OWN CN
* chat gateway (`CN_CHAT_BASE`), and the credential's `domain` field carries it
* verbatim on real records. Refusing it here made the plugin reject its own
* export: the card exported 8 accounts, and re-importing that file skipped the
* three whose `domain` was the gateway. It cannot be spoofed into the wrong
* region by being recognised here — it names the CN gateway, so admitting it is
* the same decision as admitting `workbuddy.cn`.
*/
function knownRegionOf(domain) {
	const lowered = domain.trim().toLowerCase();
	if (lowered === "") return void 0;
	if (lowered === "workbuddy.ai" || lowered.endsWith(".workbuddy.ai")) return "global";
	if (lowered === "codebuddy.ai" || lowered.endsWith(".codebuddy.ai")) return "global";
	if (lowered === "workbuddy.cn" || lowered.endsWith(".workbuddy.cn")) return "cn";
	if (lowered === "codebuddy.cn" || lowered.endsWith(".codebuddy.cn")) return "cn";
	if (lowered === "copilot.tencent.com" || lowered.endsWith(".copilot.tencent.com")) return "cn";
}
/**
* Gateway for a global credential.
*
* International accounts are NOT interchangeable across brand domains: a token
* issued at `codebuddy.ai` is rejected by the `workbuddy.ai` gateway (and vice
* versa), so the base must follow the credential's OWN domain rather than a
* single hardcoded host. Anything unrecognised falls back to `workbuddy.ai`,
* the desktop app's gateway.
*/
function globalBase(domain) {
	const lowered = domain.trim().toLowerCase();
	if (lowered === "codebuddy.ai" || lowered.endsWith(".codebuddy.ai")) return "https://www.codebuddy.ai";
	return GLOBAL_BASE;
}
function chatBase(credential) {
	return regionOf(credential.domain) === "global" ? globalBase(credential.domain) : CN_CHAT_BASE;
}
function billingBase(credential) {
	return regionOf(credential.domain) === "global" ? globalBase(credential.domain) : CN_BILLING_BASE;
}
function originReferer(credential) {
	return regionOf(credential.domain) === "global" ? globalBase(credential.domain) : CN_BILLING_BASE;
}
/** Headers every upstream request shares. */
function commonHeaders(credential) {
	return {
		"Accept": "application/json, text/plain, */*",
		"X-Requested-With": "XMLHttpRequest",
		"Origin": originReferer(credential),
		"Referer": `${originReferer(credential)}/`,
		"User-Agent": CLIENT_UA
	};
}
/**
* The conversation-identity triple the chat endpoint now requires (issue #66).
*
* WHY THIS EXISTS. The international gateway added a second gate on top of the
* `system`-head rule from 3.1.0: a chat request must carry the official desktop
* client's conversation identity, or every step is refused with business code
* `11128` / `conversation id missing`. That error's own `displayTips` blames
* "渠道校验 / 不是官方客户端发出的", which reads as a client-identity problem —
* the literal message is the actionable part, and it names a MISSING CONVERSATION
* ID, not a missing or wrong user agent.
*
* The two gates are independent, and both must hold (measured 2026-10-16 on the
* real international gateway, plugin body shape unchanged):
*
*   headers            messages          result
*   -----------------  ----------------  --------------
*   plugin as-shipped  ["system","user"] 400 / 11128
*   plugin + triple    ["system","user"] 200
*   plugin + triple    ["user"]          400 / 11128
*
* So `ensureSystemHead()` must NOT be dropped as "no longer needed": the system
* rule went from sufficient to necessary-but-not-sufficient. The last row is what
* keeps this function from being mistaken for a replacement for it.
*
* MINIMAL SET. The official bundle's `applyGatewayHeaders` also sends
* `acp-connection-id`, `X-Agent-Intent` / `X-Agent-Purpose`, `X-IDE-*`,
* `X-Private-Data` and trace headers. Bisecting them one at a time showed NONE of
* those is required: `X-Conversation-Request-ID` and `-Message-ID` are needed
* alongside `X-Conversation-ID` (that one alone still fails), and each of the
* extras is satisfied by the triple without it. Hence exactly three.
*
* VALUES. With explicit DSH session/turn context, conversation and turn IDs
* come from an account-scoped persistent link. Message IDs remain fresh for
* every model call. Calls without trustworthy context retain independent IDs.
*
* APPLIED TO BOTH REGIONS on purpose. The CN gateway tolerates their absence
* today, so this is harmless there — and a header that is only ever sent on one
* side is exactly how the 3.1.0 defect became international-only and invisible
* from the CN side. One code path, both regions.
*
* @returns the three headers, every value distinct.
*/
function conversationHeaders(association) {
	const valid = (value) => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
	const linked = valid(association?.conversationId) && valid(association?.conversationRequestId);
	return {
		"X-Conversation-ID": linked ? association.conversationId : randomUUID(),
		"X-Conversation-Request-ID": linked ? association.conversationRequestId : randomUUID(),
		"X-Conversation-Message-ID": randomUUID()
	};
}
/** Chat request headers, including the X-No-* conventions the official CLI uses. */
function chatHeaders(credential, association) {
	return {
		...commonHeaders(credential),
		"Content-Type": "application/json",
		...credential.uid === "" ? { "X-No-User-Id": "1" } : { "X-User-Id": credential.uid },
		...credential.enterpriseId === void 0 || credential.enterpriseId === "" ? { "X-No-Enterprise-Id": "1" } : { "X-Enterprise-Id": credential.enterpriseId },
		...credential.domain === "" ? { "X-No-Department-Info": "1" } : { "X-Domain": credential.domain },
		"X-Product": "SaaS",
		"X-IDE-Name": "WorkBuddy",
		"X-IDE-Type": "WorkBuddy",
		...conversationHeaders(association)
	};
}
/** Refresh-endpoint headers; X-Refresh-Token appears here and nowhere else. */
function refreshHeaders(credential) {
	const headers = {
		...commonHeaders(credential),
		"X-Refresh-Token": credential.refreshToken,
		"X-Auth-Refresh-Source": "workbuddy"
	};
	if (credential.enterpriseId !== void 0 && credential.enterpriseId !== "") headers["X-Enterprise-Id"] = credential.enterpriseId;
	return headers;
}
/** Billing request headers. */
function billingHeaders(credential) {
	const headers = {
		"Authorization": `Bearer ${credential.accessToken}`,
		"Accept": "application/json",
		"Content-Type": "application/json"
	};
	if (credential.uid !== "") headers["X-User-Id"] = credential.uid;
	if (credential.enterpriseId !== void 0 && credential.enterpriseId !== "") {
		headers["X-Enterprise-Id"] = credential.enterpriseId;
		headers["X-Tenant-Id"] = credential.enterpriseId;
	}
	if (credential.domain !== "") headers["X-Domain"] = credential.domain;
	return headers;
}
/**
* Stand-in system message for a request that reached the wire carrying none.
*
* Not a stylistic default: both WorkBuddy gateways want the conversation to
* OPEN with a system message, and the international one enforces it — a
* user-first body there is refused with business code 11128
* (`first message is not system prompt`), which the gateway surfaces as
* "blocked by security policy". The domestic gateway tolerates the same body,
* so the fault only ever shows up on the international route (issue: a global
* model failing every step while the CN one is fine).
*
* A system message can go missing before this module ever sees the body:
* `dsh-llm-pi-ai` folds a leading `system` message into `Context.systemPrompt`
* and pi-ai only emits that prompt `if (context.systemPrompt)` — so an EMPTY
* prompt emits no system message at all, and pi-ai demotes any `system` entry
* left in `messages` to `user`. By the time the shim holds the JSON, the real
* prompt is no longer recoverable, and a minimal placeholder is strictly better
* than a guaranteed 400.
*
* Deliberately tiny: this is a last-resort placeholder, not a persona. Inventing
* a longer one would quietly change model behaviour on every affected request.
*/
const WORKBUDDY_FALLBACK_SYSTEM_PROMPT = "You are a helpful assistant.";
/**
* Make the conversation open with a system message, prepending the fallback
* when nothing else supplies one.
*
* `developer` is normalized to `system` first (the gateways reject
* `developer`), so an ordinary DSH request already satisfies this and the
* function is a no-op for it.
*/
function ensureSystemHead(obj) {
	const messages = obj["messages"];
	if (!Array.isArray(messages) || messages.length === 0) return;
	const head = messages[0];
	if (typeof head !== "object" || head === null || Array.isArray(head)) return;
	const role = head["role"];
	if (typeof role === "string" && role.trim().toLowerCase() === "system") return;
	messages.unshift({
		role: "system",
		content: WORKBUDDY_FALLBACK_SYSTEM_PROMPT
	});
}
/**
* Normalize an OpenAI chat-completions body for the WorkBuddy upstream:
* force `stream: true` (the upstream rejects non-streaming), flatten
* `tool_choice` (the upstream's field is a string; object forms return 400),
* and guarantee a leading system message.
*/
/**
* The model id a prepared body asks for, when it carries one (issue #61).
*
* Lives beside {@link prepareChatBody} because it reads the same shape: the
* serialized OpenAI body, after this plugin's rewrites. Never throws — a
* malformed body is the upstream's to reject, not a reason to lose the failure
* measurement that this id is needed for.
*/
function modelIdOfBody(bodyJson) {
	try {
		const parsed = JSON.parse(bodyJson);
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return void 0;
		const model = parsed["model"];
		return typeof model === "string" && model !== "" ? model : void 0;
	} catch {
		return;
	}
}
/** Apply the requested U+200B substitution only to WorkBuddy system text. */
function rewriteWorkBuddySystemIdentity(message) {
	if (message["role"] !== "system") return;
	const rewrite = (text) => text.replaceAll("DeepSeek Harness", "DeepSeek\u200B Harness");
	const content = message["content"];
	if (typeof content === "string") {
		message["content"] = rewrite(content);
	} else if (Array.isArray(content)) {
		for (const part of content) {
			if (part !== null && typeof part === "object" && (part.type === "text" || part.type === "input_text") && typeof part.text === "string") part.text = rewrite(part.text);
		}
	}
}
function prepareChatBody(source) {
	let body;
	try {
		body = JSON.parse(source);
	} catch {
		return source;
	}
	if (typeof body !== "object" || body === null || Array.isArray(body)) return source;
	const obj = body;
	obj["stream"] = true;
	if (Array.isArray(obj["messages"])) for (const value of obj["messages"]) {
		if (typeof value !== "object" || value === null || Array.isArray(value)) continue;
		const message = value;
		if (message["role"] === "developer") message["role"] = "system";
		rewriteWorkBuddySystemIdentity(message);
	}
	ensureSystemHead(obj);
	normalizeToolChoice(obj);
	dropRefusedReasoning(obj);
	return JSON.stringify(obj);
}
/**
* Remove `reasoning_effort: "off"` when it would only ever fail (issue #83).
*
* The refusal table is keyed by exact model id, but the model a request
* actually names can change AFTER the catalog was built: account-pool failover
* re-points the body at another account's model, and `primary-model` is an
* alias whose concrete id the gateway resolves. A tick on such a row therefore
* reaches this plugin's prepared body as a bare `"off"` — exactly the value the
* table says the upstream answers with HTTP 400 / code 11133.
*
* Stripped HERE because this is the last thing the plugin controls before the
* upstream counts the body, and because the test is {@link offRefused} — the
* same single source of truth the catalog stamping uses, so the two cannot
* drift apart.
*
* Only the literal `"off"` is touched. Every other effort value returned 200 on
* every model measured for issue #34, and a body naming no effort is left
* alone: deleting a field the caller never sent would be inventing an opinion
* about how the model should think.
*/
function dropRefusedReasoning(obj) {
	const effort = obj["reasoning_effort"];
	if (typeof effort !== "string" || effort.trim().toLowerCase() !== "off") return;
	const model = obj["model"];
	if (typeof model !== "string" || !offRefused(model)) return;
	delete obj["reasoning_effort"];
}
/** Rewrite OpenAI `tool_choice` spellings into the upstream's string form. */
function normalizeToolChoice(obj) {
	const suppress = () => {
		delete obj["tools"];
		delete obj["functions"];
	};
	if (!("tool_choice" in obj)) return;
	const choice = obj["tool_choice"];
	if (typeof choice === "string") {
		if (choice.trim().toLowerCase() === "none") {
			delete obj["tool_choice"];
			suppress();
		}
		return;
	}
	if (typeof choice === "object" && choice !== null && !Array.isArray(choice)) {
		const wrapped = choice;
		const type = typeof wrapped["type"] === "string" ? wrapped["type"].trim().toLowerCase() : "";
		if (type === "none") {
			delete obj["tool_choice"];
			suppress();
		} else if (type === "auto" || type === "required") obj["tool_choice"] = type;
		else if (type === "function") {
			const fn = typeof wrapped["function"] === "object" && wrapped["function"] !== null ? wrapped["function"] : void 0;
			let name = typeof fn?.["name"] === "string" ? fn["name"] : "";
			if (name === "" && typeof wrapped["name"] === "string") name = wrapped["name"];
			name = name.trim();
			obj["tool_choice"] = name !== "" ? name : "auto";
		} else delete obj["tool_choice"];
		return;
	}
	delete obj["tool_choice"];
}
/**
* What a PREPARED request body declares, as the input to DSML recovery.
*
* This exists so the recovery path can answer the one question its gates turn
* on — "was this tool name offered in THIS request?" — without a second parse
* of the body and without a second source of truth. `prepareChatBody` has
* already parsed it; this reads the same prepared JSON.
*
* Two facts make it cheap and exact:
*
*   - `tool_choice: "none"` deletes `tools` outright in
*     {@link normalizeToolChoice}, so "no tools declared" is directly
*     observable here rather than a separate condition to remember;
*   - a pinned `tool_choice` arrives as the bare function name (the object form
*     was flattened), so anything that is not `auto`/`required`/`none` is a pin.
*
* Returns `undefined` when the body declares nothing usable — malformed JSON,
* no `tools` array, or an empty one. Callers must read that as "recovery is
* off" (gate 3), not as "no information, so guess".
*/
function declaredTools(bodyJson) {
	let body;
	try {
		body = JSON.parse(bodyJson);
	} catch {
		return;
	}
	if (typeof body !== "object" || body === null || Array.isArray(body)) return void 0;
	const obj = body;
	const tools = obj["tools"];
	if (!Array.isArray(tools)) return void 0;
	const names = /* @__PURE__ */ new Set();
	const requiredParameters = /* @__PURE__ */ new Map();
	const acceptedParameters = /* @__PURE__ */ new Map();
	for (const value of tools) {
		if (typeof value !== "object" || value === null || Array.isArray(value)) continue;
		const fn = value["function"];
		if (typeof fn !== "object" || fn === null || Array.isArray(fn)) continue;
		const definition = fn;
		const name = typeof definition["name"] === "string" ? definition["name"] : "";
		if (name === "") continue;
		names.add(name);
		const parameters = definition["parameters"];
		if (typeof parameters !== "object" || parameters === null || Array.isArray(parameters)) continue;
		const schema = parameters;
		const accepted = /* @__PURE__ */ new Set();
		const properties = schema["properties"];
		if (typeof properties === "object" && properties !== null && !Array.isArray(properties)) {
			for (const key of Object.keys(properties)) if (key !== "") accepted.add(key);
		}
		acceptedParameters.set(name, accepted);
		const required = schema["required"];
		if (!Array.isArray(required)) continue;
		const list = required.filter((entry) => typeof entry === "string" && entry !== "");
		if (list.length > 0) requiredParameters.set(name, list);
	}
	if (names.size === 0) return void 0;
	const pinned = typeof obj["tool_choice"] === "string" ? obj["tool_choice"].trim() : "";
	const pinnedToolName = pinned !== "" && ![
		"auto",
		"required",
		"none"
	].includes(pinned.toLowerCase()) ? pinned : void 0;
	return {
		names,
		requiredParameters,
		acceptedParameters,
		...pinnedToolName === void 0 ? {} : { pinnedToolName }
	};
}
/**
* Gateway (openresty/APISIX) rejection of a token it no longer accepts.
*
* The business APIs answer JSON; an edge rejection answers an HTML error page
* instead. A 401 that is not JSON therefore means the credential was refused
* before routing — almost always a revoked/expired token rather than a bug in
* the request. Detected from the body so a proxy's own error page (which would
* also be HTML) is still described accurately.
*/
function isGatewayAuthRejection(status, text) {
	if (status !== 401 && status !== 403) return false;
	const lower = text.toLowerCase();
	return lower.includes("openresty") || lower.includes("apisix") || lower.includes("authorization required");
}
/** Marker carried by {@link WorkBuddyCredentialRejectedError}; survives bundling. */
const CREDENTIAL_REJECTED_CODE = "WORKBUDDY_CREDENTIAL_REJECTED";
/**
* The upstream refused the CREDENTIAL itself rather than failing the request.
*
* This is a distinct, actionable class: the token is not usable and no retry
* with the same token will help. Callers use it to tell the user what to do
* about it (switch accounts, or sign in again) instead of showing a raw HTTP
* error, and the card must never confuse it with a transient upstream fault.
*
* Identified by {@link CREDENTIAL_REJECTED_CODE} rather than `instanceof`, so
* the check keeps working when the caller and the thrower end up in different
* module instances (bundled host half vs. a test's source import).
*/
var WorkBuddyCredentialRejectedError = class extends Error {
	code = CREDENTIAL_REJECTED_CODE;
	/** HTTP status the upstream answered with (401 or 403). */
	status;
	constructor(status) {
		super(`workbuddy: the signed-in credential was rejected by the upstream gateway (http ${status}). The stored token is no longer accepted — most likely a stale credential file from an earlier sign-in was selected. Re-sign in to the WorkBuddy desktop app, then pick that account in the plugin card. Run \`dsh-connect-workbuddy doctor\` to list every discovered credential.`);
		this.name = "WorkBuddyCredentialRejectedError";
		this.status = status;
	}
};
/** Whether an error reports that the upstream refused the credential itself. */
function isCredentialRejectedError(value) {
	return typeof value === "object" && value !== null && value.code === "WORKBUDDY_CREDENTIAL_REJECTED";
}
async function readEnvelope(response) {
	const text = await response.text();
	let parsed;
	try {
		parsed = JSON.parse(text);
	} catch {
		if (isGatewayAuthRejection(response.status, text)) throw new WorkBuddyCredentialRejectedError(response.status);
		throw new Error(`workbuddy upstream returned non-JSON (http ${response.status}): ${text.slice(0, 160)}`);
	}
	if (typeof parsed !== "object" || parsed === null) throw new Error(`workbuddy upstream returned an unexpected document (http ${response.status})`);
	const document = parsed;
	return {
		code: typeof document["code"] === "number" ? document["code"] : 0,
		msg: typeof document["msg"] === "string" ? document["msg"] : "",
		data: "data" in document ? document["data"] : void 0
	};
}
/**
* Fail an envelope whose business code is non-zero, classified like HTTP errors.
*
* A JSON body on a 401/403 is still a credential refusal: the edge answered in
* the business shape, but the token is just as unusable, so it maps to the same
* actionable error instead of a generic "client" failure.
*/
function envelopeError(status, envelope) {
	if (status === 401 || status === 403) return new WorkBuddyCredentialRejectedError(status);
	const kind = classifyUpstreamError(status, envelope.msg);
	return /* @__PURE__ */ new Error(`workbuddy upstream ${kind} (http ${status}): ${envelope.msg.slice(0, 160)}`);
}
/**
* Parse the upstream's `credits` string into a multiplier.
*
* Observed forms: `"x0.79 credits"`, `"x0.05"`, `"x0.00 credits"`,
* and absent. Unparsable values yield undefined rather than a guess — the
* card simply omits the rate instead of displaying a fabricated one.
*/
function parseCreditMultiplier(value) {
	if (typeof value !== "string") return void 0;
	const match = /x\s*([0-9]*\.?[0-9]+)/iu.exec(value);
	if (match === null) return void 0;
	const parsed = Number(match[1]);
	return Number.isFinite(parsed) && parsed >= 0 ? parsed : void 0;
}
/**
* The effort vocabulary the upstream's plural-form payloads declare across both
* gateways (live union of every `supportedEfforts` list seen; `minimal` has
* never appeared). Both gateways also accept every level in it on
* singular-form models — medium/xhigh fold into high, low/max answer with
* their own budgets — so the singular `effort` value is a DEFAULT, never the
* model's only level.
*/
const SINGULAR_EFFORT_LADDER = [
	"low",
	"medium",
	"high",
	"xhigh",
	"max"
];
/**
* The singular spelling of reasoning metadata: `effort` (plus the ignored
* `summary`) and none of the plural-form fields. Observed on CN
* `deepseek-v4.1-flash`/`kimi-k3-1`/`glm-5.2`… and global
* `deepseek-v4.1-flash`/`kimi-k3`/`gemini-3.5-flash`… (issue #7).
*/
function isSingularEffortForm(raw) {
	return typeof raw["effort"] === "string" && !Array.isArray(raw["supportedEfforts"]) && typeof raw["defaultEffort"] !== "string" && typeof raw["canDisableThinking"] !== "boolean";
}
/**
* Fold a singular-form `effort` into the plural shape the rest of the plugin
* already understands. Live probes on both gateways (issue #7) show these
* models answer with distinct `reasoning_content` across the whole ladder —
* and think NOT AT ALL when no `reasoning_effort` is sent — so the fold
* widens `supportedEfforts` and carries the declared value into
* `defaultEffort`. An unrecognized `effort` value passes through as the lone
* level, leaving its fate to the adapter's known-level filter.
*/
function singularEffortLadder(raw) {
	const effort = typeof raw["effort"] === "string" ? raw["effort"] : void 0;
	if (effort === void 0) return void 0;
	return SINGULAR_EFFORT_LADDER.includes(effort) ? [...SINGULAR_EFFORT_LADDER] : [effort];
}
/**
* Parse the upstream's `reasoning` object; unknown shapes degrade to `{}`.
* Both spellings normalize here: the plural form passes through as declared,
* and the singular `effort` form folds via {@link singularEffortLadder}.
*/
function parseReasoning(value) {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return void 0;
	const raw = value;
	const effort = typeof raw["effort"] === "string" ? raw["effort"] : void 0;
	const supportedEfforts = Array.isArray(raw["supportedEfforts"]) ? raw["supportedEfforts"].filter((entry) => typeof entry === "string") : singularEffortLadder(raw);
	const defaultEffort = typeof raw["defaultEffort"] === "string" ? raw["defaultEffort"] : effort;
	const canDisableThinking = typeof raw["canDisableThinking"] === "boolean" ? raw["canDisableThinking"] : isSingularEffortForm(raw) ? true : void 0;
	if (supportedEfforts === void 0 && defaultEffort === void 0 && canDisableThinking === void 0) return;
	return {
		...supportedEfforts === void 0 || supportedEfforts.length === 0 ? {} : { supportedEfforts },
		...defaultEffort === void 0 ? {} : { defaultEffort },
		...canDisableThinking === void 0 ? {} : { canDisableThinking }
	};
}
/** Parse one catalog entry; entries without usable token limits are dropped. */
function parseUpstreamModel(value) {
	if (typeof value !== "object" || value === null) return void 0;
	const raw = value;
	const id = typeof raw["id"] === "string" ? raw["id"] : "";
	if (id === "" || raw["disabled"] === true) return void 0;
	const input = typeof raw["maxInputTokens"] === "number" ? raw["maxInputTokens"] : 0;
	const output = typeof raw["maxOutputTokens"] === "number" ? raw["maxOutputTokens"] : 0;
	if (input <= 0 || output <= 0) return void 0;
	const name = typeof raw["name"] === "string" && raw["name"] !== "" ? raw["name"] : id;
	const descriptionZh = typeof raw["descriptionZh"] === "string" && raw["descriptionZh"] !== "" ? raw["descriptionZh"] : void 0;
	const descriptionEn = typeof raw["descriptionEn"] === "string" && raw["descriptionEn"] !== "" ? raw["descriptionEn"] : void 0;
	const creditMultiplier = parseCreditMultiplier(raw["credits"]);
	const reasoning = parseReasoning(raw["reasoning"]);
	const supportsToolCall = typeof raw["supportsToolCall"] === "boolean" ? raw["supportsToolCall"] : void 0;
	const supportsImages = raw["disabledMultimodal"] === true ? false : typeof raw["supportsImages"] === "boolean" ? raw["supportsImages"] : void 0;
	return {
		id,
		name,
		contextWindow: input,
		maxTokens: output,
		...creditMultiplier === void 0 ? {} : { creditMultiplier },
		...supportsImages === void 0 ? {} : { supportsImages },
		...reasoning === void 0 ? {} : { reasoning },
		...descriptionZh === void 0 ? {} : { descriptionZh },
		...descriptionEn === void 0 ? {} : { descriptionEn },
		...supportsToolCall === void 0 ? {} : { supportsToolCall }
	};
}
/**
* Select the chat-capable models from a catalog-shaped document: parse every
* entry, then keep the `cli` agent's roster in its declared order.
*
* Both the CN personal-models document and the global `/v3/config` document
* carry `models` plus an `agents` roster with the same entry shape, so one
* selector serves them. Without a usable `cli` roster the whole parsed catalog
* is exposed rather than nothing: the roster is an upstream detail that may
* change, and an empty answer would silently disarm the provider.
*/
function selectCliModels(rawModels, agents) {
	const byId = /* @__PURE__ */ new Map();
	for (const model of Array.isArray(rawModels) ? rawModels : []) {
		const parsed = parseUpstreamModel(model);
		if (parsed !== void 0) byId.set(parsed.id, parsed);
	}
	let cliIds;
	for (const agent of Array.isArray(agents) ? agents : []) if (typeof agent === "object" && agent !== null) {
		const wrapped = agent;
		if (wrapped["name"] === "cli" && Array.isArray(wrapped["models"])) {
			cliIds = wrapped["models"].filter((id) => typeof id === "string");
			break;
		}
	}
	const models = (cliIds !== void 0 && cliIds.length > 0 ? cliIds : [...byId.keys()]).map((id) => byId.get(id)).filter((model) => model !== void 0);
	if (models.length === 0) throw new Error("workbuddy model catalog resolved to an empty list");
	return models;
}
/**
* Upstream HTTP client. One instance serves the whole plugin; requests take
* the credential explicitly so token refreshes apply on the next call.
*/
var WorkBuddyUpstreamClient = class {
	onFallback;
	/**
	* Reports a fallback the catalog reader had to take.
	*
	* Injected rather than logged here because this module holds no logger on
	* purpose — it is pure transport + parsing, so it stays importable from
	* tests and the CLI without a cordis context. The host wires its
	* `ctx.logger.warn` in; without a host the event is dropped, which is the
	* same silence the previous single-source reader had.
	*/
	constructor(onFallback) {
		this.onFallback = onFallback;
	}
	/** POST the chat endpoint; a successful answer is the raw SSE response. */
	async chatStream(credential, bodyJson, signal, association) {
		let response;
		try {
			response = await fetch(`${chatBase(credential)}/v2/chat/completions`, {
				method: "POST",
				headers: {
					...chatHeaders(credential, association),
					"Authorization": `Bearer ${credential.accessToken}`
				},
				body: bodyJson,
				...signal === void 0 ? {} : { signal }
			});
		} catch (error) {
			return {
				ok: false,
				status: 0,
				kind: "server",
				message: `transport error: ${String(error)}`
			};
		}
		if (response.ok) return {
			ok: true,
			response
		};
		const text = (await response.text()).slice(0, ERROR_BODY_LIMIT);
		return {
			ok: false,
			status: response.status,
			kind: classifyUpstreamError(response.status, text),
			message: text,
			detail: parseUpstreamErrorDetail(text)
		};
	}
	/**
	* Send one minimal chat request for `bodyJson` and report the RAW answer.
	*
	* Shares {@link chatHeaders} and the chat base with {@link chatStream} on
	* purpose: a probe is only meaningful if it reaches the same endpoint with the
	* same authentication as a real request. The differences are deliberate and
	* narrow — it returns the headers (for `Retry-After`) and the failure body
	* (for classification) instead of a pre-classified error, so the probe module
	* owns the interpretation and the network layer stays a transport.
	*
	* The caller MUST drain {@link WorkBuddyProbeAnswer.response}; an unread body
	* holds the connection open.
	*
	* ALWAYS bounded in time. The pool's batch runner is serial and passes no
	* signal, so without a ceiling here one stalled connection blocks every account
	* behind it — the batch looks hung with no way to tell which member did it.
	* A supplied signal is COMBINED with the timeout rather than replacing it, so
	* the single-model route keeps its cancellation behaviour and still cannot
	* hang forever.
	*
	* The ceiling covers ONLY the wait for a response — it is cleared the moment
	* `fetch` resolves. Holding a timer over the body would put a healthy but slow
	* model on the same clock as a dead endpoint, and the whole point of the
	* period is to answer "did the upstream answer at all". A probe asks for a
	* single token, so the body that follows is one SSE line.
	*/
	async probeChat(credential, bodyJson, signal) {
		let response;
		const ceiling = new AbortController();
		const timer = setTimeout(() => {
			ceiling.abort(/* @__PURE__ */ new Error(`no response within ${PROBE_TIMEOUT_MS}ms`));
		}, PROBE_TIMEOUT_MS);
		try {
			response = await fetch(`${chatBase(credential)}/v2/chat/completions`, {
				method: "POST",
				headers: {
					...chatHeaders(credential),
					"Authorization": `Bearer ${credential.accessToken}`
				},
				body: bodyJson,
				signal: signal === void 0 ? ceiling.signal : AbortSignal.any([signal, ceiling.signal])
			});
		} catch (error) {
			return {
				ok: false,
				status: 0,
				retryAfter: null,
				body: `transport error: ${String(error)}`
			};
		} finally {
			clearTimeout(timer);
		}
		const retryAfter = response.headers.get("retry-after");
		if (response.ok) return {
			ok: true,
			status: response.status,
			retryAfter,
			response
		};
		const body = (await response.text()).slice(0, ERROR_BODY_LIMIT);
		return {
			ok: false,
			status: response.status,
			retryAfter,
			body
		};
	}
	/** POST the token-refresh endpoint; the caller merges the outcome. */
	async refreshToken(credential) {
		const response = await fetch(`${chatBase(credential)}/v2/plugin/auth/token/refresh`, {
			method: "POST",
			headers: refreshHeaders(credential),
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
		const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
		const accessToken = typeof data["accessToken"] === "string" ? data["accessToken"] : "";
		if (accessToken === "") throw new Error("workbuddy token refresh returned no accessToken; sign in again in the WorkBuddy app");
		const outcome = { accessToken };
		if (typeof data["refreshToken"] === "string" && data["refreshToken"] !== "") outcome.refreshToken = data["refreshToken"];
		if (typeof data["expiresIn"] === "number" && data["expiresIn"] > 0) outcome.expiresInSec = data["expiresIn"];
		if (typeof data["domain"] === "string" && data["domain"] !== "") outcome.domain = data["domain"];
		return outcome;
	}
	/**
	* Read the model directory for the credential's region.
	*
	* Both regions prefer `/v3/config`, the document the WorkBuddy app itself
	* consumes, so the plugin's roster and the app's agree. They differ in the
	* user agent that requests it, and that difference is load-bearing: `/v3`
	* serves a different roster per client channel. Global asks as the desktop
	* channel (see {@link DESKTOP_UA}); CN asks as the CLI channel
	* ({@link CLIENT_UA}), which is the only CN channel whose `/v3` answer
	* contains the free `hy4-preview-f` (see {@link GLOBAL_CONFIG_PATH}).
	*
	* CN falls back to the legacy {@link MODELS_CATALOG_PATH} when `/v3` fails,
	* returns a non-zero envelope, or resolves to no usable model, so a gateway
	* that stops serving the modern document degrades to the previous roster
	* instead of leaving the region empty. The fallback is silent when unused and
	* reported when it fires, because a roster that silently differs from the
	* app's is exactly the bug this ordering exists to fix.
	*
	* No user-side toggle is involved: the region comes from the credential's
	* `domain`.
	*/
	async fetchModels(credential, signal) {
		const timeout = signal ?? AbortSignal.timeout(JSON_TIMEOUT_MS);
		if (regionOf(credential.domain) === "global") {
			const response = await fetch(`${globalBase(credential.domain)}${GLOBAL_CONFIG_PATH}`, {
				headers: {
					"Authorization": `Bearer ${credential.accessToken}`,
					"Accept": "application/json",
					...credential.uid === "" ? {} : { "X-User-Id": credential.uid },
					...credential.domain === "" ? {} : { "X-Domain": credential.domain },
					"X-Product": "SaaS",
					"X-Requested-With": "XMLHttpRequest",
					"Connection": "close",
					"User-Agent": DESKTOP_UA
				},
				signal: timeout
			});
			const envelope = await readEnvelope(response);
			if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
			const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
			return selectCliModels(data["models"], data["agents"]);
		}
		const readCatalog = async (path) => {
			const response = await fetch(`${chatBase(credential)}${path}`, {
				headers: {
					"Authorization": `Bearer ${credential.accessToken}`,
					"Accept": "application/json",
					"Origin": originReferer(credential),
					"Referer": `${originReferer(credential)}/`,
					"User-Agent": CLIENT_UA
				},
				signal: timeout
			});
			const envelope = await readEnvelope(response);
			if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
			const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
			return selectCliModels(data["models"], data["agents"]);
		};
		try {
			return await readCatalog(GLOBAL_CONFIG_PATH);
		} catch (error) {
			this.onFallback?.(`CN model catalog ${GLOBAL_CONFIG_PATH} failed; falling back to ${MODELS_CATALOG_PATH}: ` + (error instanceof Error ? error.message : String(error)));
			return await readCatalog(MODELS_CATALOG_PATH);
		}
	}
	/** Query today's check-in status without changing account state. */
	async fetchCheckinStatus(credential) {
		const response = await fetch(`${billingBase(credential)}/v2/billing/meter/checkin-activity-status`, {
			method: "POST",
			headers: billingHeaders(credential),
			body: "{}",
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
		const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
		const numberField = (key) => typeof data[key] === "number" ? data[key] : 0;
		return {
			active: data["active"] === true,
			todayCheckedIn: data["today_checked_in"] === true,
			streakDays: numberField("streak_days"),
			dailyCredit: numberField("daily_credit"),
			todayCredit: numberField("today_credit"),
			isStreakDay: data["is_streak_day"] === true,
			nextStreakDay: numberField("next_streak_day"),
			streakBonusDays: numberField("streak_bonus_days"),
			streakBonusCredit: numberField("streak_bonus_credit"),
			...typeof data["claim_button_text"] === "string" && data["claim_button_text"] !== "" ? { claimButtonText: data["claim_button_text"] } : {}
		};
	}
	/**
	* Read every promotional benefit this account can still see, WITHOUT claiming
	* anything.
	*
	* The campaign list is closed and small on purpose: the gateway exposes one
	* status endpoint per campaign, so "discover benefits" is a fixed fan-out, not
	* a search. A new campaign type means a new `/billing/meter/*-status` endpoint
	* added here — the plugin never guesses at undocumented ones.
	*
	* Every endpoint is read in parallel because they are independent, and a
	* failure is per-endpoint: one campaign answering 5xx must not blank out the
	* other. The caller gets only what was actually read.
	*
	* Note the paths carry NO `/v2` prefix — these are not the `/v2/billing/meter`
	* family the credit and check-in calls use. Probing `/v2/...` answers 404, which
	* is how this distinction was found.
	*/
	async fetchBenefits(credential) {
		const read = async (kind, path) => {
			try {
				return await this.readBenefitStatus(kind, credential, path);
			} catch {
				return;
			}
		};
		const [compensation, gift] = await Promise.all([read("compensation", "/billing/meter/compensation-status"), read("gift", "/billing/meter/check-gift-claimed")]);
		return [compensation, gift].filter((benefit) => benefit !== void 0);
	}
	/** One benefit status endpoint. Throws on anything that is not a clean `code: 0`. */
	async readBenefitStatus(kind, credential, path) {
		const response = await fetch(`${billingBase(credential)}${path}`, {
			method: "POST",
			headers: billingHeaders(credential),
			body: "{}",
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
		const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
		const numberField = (key) => typeof data[key] === "number" ? data[key] : 0;
		/** The gateway states times as `YYYY-MM-DD HH:MM:SS` in local time. */
		const timeField = (key) => {
			const raw = data[key];
			if (typeof raw !== "string" || raw.trim() === "") return void 0;
			const parsed = Date.parse(raw.replace(" ", "T"));
			return Number.isNaN(parsed) ? void 0 : parsed;
		};
		const endsAtMs = timeField("end_time");
		const startsAtMs = timeField("start_time");
		const claimedAtMs = timeField("claimed_at");
		return {
			kind,
			active: data["active"] === true,
			claimed: data["claimed"] === true,
			creditNum: numberField("credit_num"),
			...endsAtMs === void 0 ? {} : { endsAtMs },
			...startsAtMs === void 0 ? {} : { startsAtMs },
			...claimedAtMs === void 0 ? {} : { claimedAtMs }
		};
	}
	/**
	* Claim one benefit campaign on ONE account (issue #79).
	*
	* This is a real credit grant on a real account, so it takes no account
	* argument other than the credential it is handed: the caller decides which
	* single account, and there is deliberately no batch form of this method. The
	* browser route is the only caller, and it guards the mutation with the same
	* POST + loopback checks the check-in route carries.
	*
	* The response is not parsed: the documented envelope of these two endpoints
	* is an empty success, and the caller re-reads the status afterwards. That
	* re-read, not this answer, is what the user is shown — which is why a claim
	* that silently did nothing still cannot leave the card saying 待领取.
	*/
	async claimBenefit(credential, kind) {
		const response = await fetch(`${billingBase(credential)}${BENEFIT_CLAIM_PATHS[kind]}`, {
			method: "POST",
			headers: billingHeaders(credential),
			body: "{}",
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
	}
	/** Claim today's check-in reward. The browser route guards this mutation. */
	async claimDailyCheckin(credential) {
		const response = await fetch(`${billingBase(credential)}/v2/billing/meter/daily-checkin`, {
			method: "POST",
			headers: billingHeaders(credential),
			body: "{}",
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
		const data = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
		const numberField = (key) => typeof data[key] === "number" ? data[key] : 0;
		return {
			credit: numberField("credit"),
			streakDays: numberField("streak_days"),
			isStreakDay: data["is_streak_day"] === true
		};
	}
	/**
	* POST the billing endpoint for the remaining credit, keeping every package
	* separate: the card groups monthly-cycle packages itself and lists the
	* nearest-expiring one-off packages, so aggregation here would lose the
	* dates it needs.
	*/
	async fetchCredits(credential) {
		const now = /* @__PURE__ */ new Date();
		const format = (date) => [
			date.getFullYear().toString().padStart(4, "0"),
			(date.getMonth() + 1).toString().padStart(2, "0"),
			date.getDate().toString().padStart(2, "0")
		].join("-") + " " + [
			date.getHours().toString().padStart(2, "0"),
			date.getMinutes().toString().padStart(2, "0"),
			date.getSeconds().toString().padStart(2, "0")
		].join(":");
		const response = await fetch(`${billingBase(credential)}/v2/billing/meter/get-user-resource`, {
			method: "POST",
			headers: billingHeaders(credential),
			body: JSON.stringify({
				PageNumber: 1,
				PageSize: 100,
				ProductCode: "p_tcaca",
				Status: [0, 3],
				PackageEndTimeRangeBegin: format(now),
				PackageEndTimeRangeEnd: format(new Date(now.getTime() + 3185136e6))
			}),
			signal: AbortSignal.timeout(JSON_TIMEOUT_MS)
		});
		const envelope = await readEnvelope(response);
		if (!response.ok || envelope.code !== 0) throw envelopeError(response.status, envelope);
		const responseWrapper = typeof envelope.data === "object" && envelope.data !== null ? envelope.data : {};
		const data = typeof responseWrapper["Response"] === "object" && responseWrapper["Response"] !== null ? responseWrapper["Response"] : {};
		const inner = typeof data["Data"] === "object" && data["Data"] !== null ? data["Data"] : {};
		const rawAccounts = Array.isArray(inner["Accounts"]) ? inner["Accounts"] : [];
		let total = 0;
		let nearestExpiryMs;
		let expiringSoon = 0;
		const SOON_MS = 2592e5;
		const parseDate = (raw) => {
			if (typeof raw === "number" && raw > 0xe8d4a51000) return raw;
			if (typeof raw === "string" && raw !== "") {
				const parsed = Date.parse(raw);
				if (!Number.isNaN(parsed)) return parsed;
			}
		};
		const packages = [];
		for (const raw of rawAccounts) {
			if (typeof raw !== "object" || raw === null) continue;
			const account = raw;
			const numberField = (key) => typeof account[key] === "number" ? account[key] : 0;
			const preciseField = (key) => {
				const raw = account[`${key}Precise`];
				if (typeof raw === "string" && raw !== "") {
					const parsed = Number.parseFloat(raw);
					if (Number.isFinite(parsed)) return parsed;
				}
				if (typeof raw === "number" && Number.isFinite(raw)) return raw;
				return numberField(key);
			};
			const monthly = numberField("CapacityType") === 4;
			const size = preciseField("CycleCapacitySize") || preciseField("CapacitySize");
			const remain = account["CycleCapacityRemain"] !== void 0 || account["CycleCapacityRemainPrecise"] !== void 0 ? preciseField("CycleCapacityRemain") : preciseField("CapacityRemain");
			const cappedRemain = remain < 0 ? 0 : remain;
			const cycleEndMs = parseDate(account["CycleEndTime"]);
			const expiresAtMs = monthly ? void 0 : parseDate(account["ExpiredTime"]) ?? cycleEndMs;
			const refreshAtMs = monthly ? cycleEndMs === void 0 ? void 0 : cycleEndMs + 1e3 : void 0;
			if (!monthly && (cappedRemain <= 0 || expiresAtMs !== void 0 && expiresAtMs <= Date.now())) continue;
			total += cappedRemain;
			const expiryMs = expiresAtMs;
			if (expiryMs !== void 0) {
				if (nearestExpiryMs === void 0 || expiryMs < nearestExpiryMs) nearestExpiryMs = expiryMs;
				if (expiryMs - Date.now() <= SOON_MS) expiringSoon += cappedRemain;
			}
			packages.push({
				packageName: typeof account["PackageName"] === "string" ? account["PackageName"] : "(unnamed)",
				remain: cappedRemain,
				size,
				monthly,
				...refreshAtMs === void 0 ? {} : { refreshAtMs },
				...expiresAtMs === void 0 ? {} : { expiresAtMs }
			});
		}
		return {
			total,
			packages,
			expiringSoon,
			...nearestExpiryMs === void 0 ? {} : { nearestExpiryMs }
		};
	}
};
//#endregion
//#region src/auth.ts
/**
* WorkBuddy credential resolution.
*
* 参考：corrinehu/dsh-workbuddy-connect（MIT，Copyright (c) 2026 Corrine Hu）
*   — 桌面端 auth 文件只读、刷新结果写入 $DSH_HOME 自有副本、双凭据取
*     到期更晚者、按需刷新（5 分钟余量）与单飞去重、刷新失败但 token
*     未过期则继续沿用旧 token。这些机制已在该项目验证，此处沿用。
* 改动：原版只解析单个 `workbuddy-desktop.info`。WorkBuddy 桌面端在
*   同一 auth 目录留下带时间戳的备份文件（`workbuddy-desktop.<stamp>.info`），
*   实测这些文件各自持有不同账号的可用凭据（本机 6 个文件 → 2 个账号）。
*   本实现改为扫描整个 auth 目录，按 uin 去重为多个可选账号。跟随 App
*   当前登录（live 文件）仍是默认行为；用户显式选择的账号被严格绑定，
*   不因积分多少而切换，失效时也不会静默改选其他账号。
*   另：store 可按区域（cn | global）限定可见账号 —— 国内版与国际版各持
*   一个 store，账号、刷新、选择完全隔离；插件自有刷新副本也按区域分
*   文件（`.workbuddy-auth.<region>.json`），双账号同时在线互不覆盖，
*   旧的单文件 `.workbuddy-auth.json` 作为迁移源保留读取。
*   另（issue #15）：`resolve()` 不再把「没登录」与「有登录但凭据读不出来」
*   合并成同一句话。后者（加密字段 + 取不到密钥）改为抛
*   `WorkBuddyEncryptedCredentialError`，给出「装 App / 用
*   WORKBUDDY_APP_EXECUTABLE 指定」这条**可执行**的建议——对已登录的用户说
*   「请重新登录一次」指向的是唯一无效的动作。判定只在失败路径上跑一次
*   `diagnose()`，健康路径不付任何代价。
*
* @module dsh-connect-workbuddy/auth
*/
/**
* Marker for a credential that EXISTS but could not be decrypted.
*
* Identified by {@link ENCRYPTED_CREDENTIAL_CODE} rather than `instanceof`, so
* the check keeps working when the thrower and the caller end up in different
* module instances (bundled host half vs. a test's source import) — the same
* convention `WorkBuddyCredentialRejectedError` uses.
*/
const ENCRYPTED_CREDENTIAL_CODE = "WORKBUDDY_ENCRYPTED_CREDENTIAL";
/**
* The user IS signed in, but the credential file cannot be read.
*
* This exists because "signed out" and "signed in with an unreadable credential"
* resolve to the same observable state — zero accounts — and were therefore
* reported with the same message. That message told a correctly signed-in user
* to sign in again, which cannot possibly help: from WorkBuddy 5.6.x the desktop
* app encrypts its token fields, and reading them requires asking the installed
* app for its key. If that app cannot be located, the fix is to point the plugin
* at it (`WORKBUDDY_APP_EXECUTABLE`), never to sign in again.
*
* No token material is carried: only the paths and the environment variable.
*/
var WorkBuddyEncryptedCredentialError = class extends Error {
	code = ENCRYPTED_CREDENTIAL_CODE;
	/** Files that hold an encrypted credential, for the user to recognize. */
	paths;
	constructor(paths) {
		const where = paths.length > 0 ? paths.join(", ") : "the credential file";
		super(`workbuddy: your WorkBuddy sign-in is present but encrypted, so it cannot be read (${where}). The WorkBuddy desktop app must be present to hand over its key; install it, or set ${WORKBUDDY_APP_EXECUTABLE_ENV} to its executable if it lives elsewhere. Signing in again will not change this.`);
		this.name = "WorkBuddyEncryptedCredentialError";
		this.paths = paths;
	}
};
/** Whether an error reports an unreadable (encrypted) credential. */
function isEncryptedCredentialError(value) {
	return typeof value === "object" && value !== null && value.code === "WORKBUDDY_ENCRYPTED_CREDENTIAL";
}
/** Legacy single-copy basename (pre-dual-provider); kept as migration source. */
const WORKBUDDY_AUTH_FILENAME = ".workbuddy-auth.json";
/** Env variable that overrides the desktop auth-file location. */
const WORKBUDDY_AUTH_FILE_ENV = "WORKBUDDY_AUTH_FILE";
/** Basename of the live WorkBuddy desktop auth file. */
const WORKBUDDY_LIVE_FILENAME = "workbuddy-desktop.info";
/** Prefix of the plugin-owned per-region credential copies (legacy, read-only). */
const WORKBUDDY_OWN_PREFIX = ".workbuddy-auth";
/**
* Directory name (inside the Harness home) of the plugin's unified credential
* vault: every account the plugin knows — scanned from the desktop app AND
* added through OAuth sign-in — is persisted there, one file per account.
*/
const WORKBUDDY_VAULT_DIRNAME = "workbuddy-vault";
/** Current on-disk format of the plugin-owned copy; readers reject others. */
const OWN_FORMAT_VERSION = 1;
/**
* Plugin-owned copy path for one region inside the Harness home. Each
* region's store refreshes into its own file so two simultaneously signed-in
* regions never overwrite each other's refreshed token.
*/
function workbuddyOwnAuthPath(region) {
	return join(resolveDshHome(), `${WORKBUDDY_OWN_PREFIX}.${region}.json`);
}
/**
* Pre-dual-provider single-copy path. Still read as a migration source (a
* legacy credential serves the region it belongs to until that region's own
* first refresh writes the per-region file), and removed by `logout`.
*/
function legacyWorkbuddyOwnAuthPath() {
	return join(resolveDshHome(), WORKBUDDY_AUTH_FILENAME);
}
/**
* The vault's directory for one region inside the Harness home.
*
* The vault is the plugin's UNIFIED credential store: every account the
* plugin knows lives here, whether it was scanned from the desktop app's
* auth directory or added through OAuth sign-in. One file per account,
* named by the account's stable id — so two sources yielding the same
* account converge on the same file instead of duplicating it.
*/
function workbuddyVaultDir(region) {
	return join(resolveDshHome(), WORKBUDDY_VAULT_DIRNAME, region);
}
/**
* Serialize a credential to the vault's on-disk document.
*
* The on-disk field names are the DESKTOP document's snake-free shapes
* (`expiresAt`, `lastRefreshTime`), which is what `parseWorkBuddyAuth`
* reads back. Serializing the runtime camelCase fields verbatim parsed as
* `expiresAtMs: 0` on the next read — every restart re-refreshed every
* account, and a refresh outage could then take down sign-ins that were
* perfectly healthy on disk.
*/
function vaultDocument(credential, accountId, refreshedFrom) {
	return {
		version: VAULT_FORMAT_VERSION,
		accountId,
		refreshedAtMs: Date.now(),
		...refreshedFrom === void 0 ? {} : { refreshedFrom: authFileName(refreshedFrom) },
		auth: {
			accessToken: credential.accessToken,
			refreshToken: credential.refreshToken,
			expiresAt: credential.expiresAtMs > 0 ? Math.floor(credential.expiresAtMs / 1e3) : 0,
			...credential.refreshExpiresAtMs === void 0 ? {} : { refreshExpiresAt: Math.floor(credential.refreshExpiresAtMs / 1e3) },
			...credential.lastRefreshAtMs === void 0 ? {} : { lastRefreshTime: Math.floor(credential.lastRefreshAtMs / 1e3) },
			domain: credential.domain
		},
		account: {
			uid: credential.uid,
			...credential.enterpriseId === void 0 ? {} : { enterpriseId: credential.enterpriseId },
			...credential.nickname === void 0 ? {} : { nickname: credential.nickname },
			...credential.uin === void 0 ? {} : { uin: credential.uin }
		}
	};
}
/** Parse the vault document; other versions and shapes are rejected. */
function parseVaultDocument(text, filePath) {
	const parsed = parseVaultDocumentWithWriteTime(text, filePath);
	return parsed === void 0 ? void 0 : parsed.credential;
}
/**
* Parse a vault document into its credential plus the LOCAL write time.
*
* Internal merge plumbing: `refreshedAtMs` is the freshness signal
* {@link isFresherVaultEntry} needs, but it is not part of
* {@link WorkBuddyCredential} (the runtime type is shared with OAuth and
* desktop reads, none of which carry it).
*/
function parseVaultDocumentWithWriteTime(text, filePath) {
	let parsed;
	try {
		parsed = JSON.parse(text);
	} catch {
		return;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return void 0;
	const document = parsed;
	if (document["version"] !== VAULT_FORMAT_VERSION) return void 0;
	if (typeof document["auth"] !== "object" || document["auth"] === null) return void 0;
	const credential = parseWorkBuddyAuth(JSON.stringify({
		auth: document["auth"],
		account: document["account"]
	}), filePath);
	if (credential === void 0) return void 0;
	const refreshedAtMs = typeof document["refreshedAtMs"] === "number" && document["refreshedAtMs"] > 0 ? document["refreshedAtMs"] : void 0;
	const refreshedFrom = typeof document["refreshedFrom"] === "string" && document["refreshedFrom"] !== "" ? document["refreshedFrom"] : void 0;
	return {
		credential: {
			...credential,
			source: "dsh"
		},
		...refreshedAtMs === void 0 ? {} : { refreshedAtMs },
		...refreshedFrom === void 0 ? {} : { refreshedFrom }
	};
}
/**
* File name of the vault's DISMISSAL list, stored beside the vault entries.
*
* It exists because deleting a vault entry is not the same as deleting an
* account. `syncVault` writes every desktop sign-in it scans back into the
* vault — that is how a desktop account becomes selectable — so removing the
* vault file alone was undone by the very next scan: the row came back and the
* button read as broken (measured: `removeAccount` returns true, `accounts()`
* still lists it).
*
* Dismissal means HIDDEN UNTIL THE USER SCANS AGAIN, not deleted forever. A
* desktop sign-in is still there in the app, and the manual 「re-scan」 action is
* the user asking to see what the machine currently has — so
* {@link WorkBuddyCredentialStore.clearDismissals} empties this list on every
* manual scan and the accounts return. Without that, a removed account could
* never be recovered except by reinstalling or re-signing in.
*
* The list must survive the automatic refreshes in between: the card polls the
* usage route every 60 seconds, and an in-memory-only dismissal would let the
* account reappear a minute after the user removed it. Hence a file.
*
* Dismissal is per ACCOUNT, not per source: hiding only the vault copy would
* leave the same account listed from the desktop file, which is the
* half-measure that produced the report.
*/
const VAULT_DISMISSED_FILENAME = ".dismissed.json";
/** Current on-disk format of the dismissal list; readers reject others. */
const DISMISSED_FORMAT_VERSION = 1;
/**
* How long one scan may be shared across callers, in milliseconds.
*
* One page open asks the store for the same account list six times (usage
* route: accounts, status, resolve; the credits panel: once PER ACCOUNT through
* `credentialFor`; and both region tabs poll together). Each read re-listed the
* auth directory, re-parsed every file and re-ran the vault merge — measured at
* 6 `readdir` and (6N+12) `readFile` for N accounts.
*
* Why a WINDOW and not a plain in-flight share: the six reads are SEQUENTIAL
* (`await` one after another), so by the time the second starts the first has
* resolved and there is nothing in flight to join. Sharing only concurrent reads
* optimised nothing at all.
*
* Why so short: this is not a cache of the account list, it is an admission that
* one page open IS one logical read. Anything longer would delay noticing a
* sign-in the desktop app made on its own — discovery IS polling, so this window
* is how long such a change may stay unseen. The card polls every 60 s, so 1 s is
* invisible to the user while collapsing one page open from six scans to one. A
* write through this store invalidates the window outright, so the actions the
* user takes here (OAuth add, import, remove, rescan) are never delayed by it.
*/
const SCAN_SHARE_WINDOW_MS = 1e3;
/** Read the dismissal list; an absent or unreadable file reads as empty. */
async function readDismissed(dir) {
	const dismissed = /* @__PURE__ */ new Set();
	try {
		const parsed = JSON.parse(await readFile(join(dir, VAULT_DISMISSED_FILENAME), "utf8"));
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return dismissed;
		const document = parsed;
		if (document["version"] !== DISMISSED_FORMAT_VERSION) return dismissed;
		const ids = document["accountIds"];
		if (!Array.isArray(ids)) return dismissed;
		for (const id of ids) if (typeof id === "string" && VAULT_ACCOUNT_ID_RE.test(id)) dismissed.add(id);
	} catch {}
	return dismissed;
}
/** Write the dismissal list atomically under its own lock. */
async function writeDismissed(dir, dismissed) {
	const path = join(dir, VAULT_DISMISSED_FILENAME);
	const ids = [...dismissed].slice(-1e3);
	const document = {
		version: DISMISSED_FORMAT_VERSION,
		accountIds: ids
	};
	await withFileLock(path, async () => {
		await writeFileAtomic(path, `${JSON.stringify(document, null, 2)}\n`, {
			mode: 384,
			dirMode: 448
		});
	});
}
/**
* Whether a directory entry is ONE ACCOUNT's vault credential file.
*
* The vault directory holds more than credentials: `writeDismissed` puts
* `.dismissed.json` here, and atomic writes transiently leave
* `<id>.json.<hex>.tmp` siblings behind. Both used to be swept up by a bare
* `endsWith('.json')` check, and the DISMISSAL file in particular is
* well-formed JSON that is not a credential — so `diagnose` reported it at the
* user as 「not a readable vault credential」 on any machine where an account
* had been removed, turning a healthy sign-in into an unreadable-looking one.
*
* The rule is the account id shape the store itself writes
* ({@link VAULT_ACCOUNT_ID_RE}), not a naming convention: a file qualifies only
* when its stem is a real id, which no bookkeeping or temp file can be. A
* `.tmp` sibling therefore fails on the dot of its extension, and a hidden file
* fails on its leading dot even before that.
*/
function isVaultCredentialFileName(name) {
	if (!name.endsWith(".json")) return false;
	return VAULT_ACCOUNT_ID_RE.test(name.slice(0, -5));
}
/** Current on-disk format of the vault; readers reject others. */
const VAULT_FORMAT_VERSION = 1;
/**
* Platform-default directories holding the WorkBuddy desktop app's auth file.
*
* Windows and Linux prefer the OS-issued env location and fall back to the
* home-derived convention when it is unset, so a redirected profile (OneDrive
* folder backup, enterprise policy) still resolves. macOS has no equivalent
* env variable; the single Application Support path is used as-is.
*
* `platform`, `home`, and `env` are injectable so the platform branches are
* testable on any host without touching a real machine.
*/
function defaultDesktopAuthDirs(platform = process.platform, home = homedir(), env = process.env) {
	if (platform === "darwin") return [join(home, "Library", "Application Support", "CodeBuddyExtension", "Data", "Public", "auth")];
	if (platform === "win32") {
		const local = nonEmptyEnv(env["LOCALAPPDATA"]) ?? join(home, "AppData", "Local");
		const roaming = nonEmptyEnv(env["APPDATA"]) ?? join(home, "AppData", "Roaming");
		return [join(local, "CodeBuddyExtension", "Data", "Public", "auth"), join(roaming, "CodeBuddyExtension", "Data", "Public", "auth")];
	}
	if (platform === "linux") {
		const config = nonEmptyEnv(env["XDG_CONFIG_HOME"]) ?? join(home, ".config");
		return [join(config, "CodeBuddyExtension", "Data", "Public", "auth")];
	}
	return [];
}
/** A non-empty, trimmed env value, or undefined when unset/blank. */
function nonEmptyEnv(value) {
	return typeof value === "string" && value.trim() !== "" ? value.trim() : void 0;
}
/** The live auth file's platform candidates, in probe order. */
function defaultDesktopAuthCandidates() {
	return defaultDesktopAuthDirs().map((dir) => join(dir, WORKBUDDY_LIVE_FILENAME));
}
/** First platform-default candidate; see {@link defaultDesktopAuthCandidates}. */
function defaultDesktopAuthPath() {
	return defaultDesktopAuthCandidates()[0];
}
/** Normalize an expiry that may arrive in seconds or milliseconds. */
function expiryToMs(value) {
	if (value <= 0) return 0;
	return value > 0xe8d4a51000 ? value : value * 1e3;
}
/**
* Resolve one string field, transparently opening the desktop app's
* encrypted-field wrapper when present.
*
* Windows builds of the WorkBuddy desktop app store `accessToken` and
* `refreshToken` as `{$wbEncrypted:1,envelope}` instead of plain strings; a
* reader that only accepts strings sees no credential at all and reports the
* account as signed out. The SAME treatment applies to `nickname`: it is
* encrypted in those builds too, so a strings-only reader silently degrades the
* account's display name to its uin (a bare number) even after tokens start
* working — which reads as "the plugin does not know who this is".
*
* `key` is undefined when the at-rest key could not be obtained, in which case
* an encrypted field resolves to undefined rather than to a fabricated value.
*/
function credentialField(value, key) {
	if (typeof value === "string") return value;
	if (!isEncryptedFieldWrapper(value)) return void 0;
	if (key === void 0) return void 0;
	return openEncryptedField(value, key);
}
/**
* An optional field that may legitimately be absent: an absent value, a
* non-string that is not a wrapper, and an unopenable wrapper all mean
* "unknown", which must not fail the whole document. Only the REQUIRED token
* fields treat an unopenable wrapper as fatal (see {@link parseWorkBuddyAuth}).
*
* `account.phoneNumber` is deliberately never read: it is encrypted in these
* builds, and the card's privacy contract admits nickname, masked uin, expiry
* and credits only — a phone number is none of the plugin's business.
*/
function optionalCredentialField(value, key) {
	if (typeof value === "string") return value === "" ? void 0 : value;
	if (!isEncryptedFieldWrapper(value)) return void 0;
	if (key === void 0) return void 0;
	try {
		return openEncryptedField(value, key);
	} catch {
		return;
	}
}
/**
* Parse a WorkBuddy auth document in either on-disk shape: the plugin OAuth
* nested form `{"auth":{...},"account":{...}}` and the flat panel form.
* Returns undefined when the document carries no access token.
*
* `atRestKey` is required to read documents whose fields are encrypted
* (see {@link credentialField}); pass the key obtained from
* `readAtRestKey()` when the plain read reports no token.
*/
function parseWorkBuddyAuth(text, filePath, atRestKey) {
	let parsed;
	try {
		parsed = JSON.parse(text);
	} catch {
		return;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return void 0;
	const document = parsed;
	let auth;
	let identity;
	if (typeof document["auth"] === "object" && document["auth"] !== null) {
		auth = document["auth"];
		identity = typeof document["account"] === "object" && document["account"] !== null ? document["account"] : {};
	} else {
		auth = document;
		identity = document;
	}
	let accessToken;
	let refreshToken;
	try {
		accessToken = credentialField(auth["accessToken"], atRestKey);
		refreshToken = credentialField(auth["refreshToken"], atRestKey);
	} catch {
		return;
	}
	if (accessToken === void 0 || accessToken === "") return void 0;
	const expiresAtMs = typeof auth["expiresAt"] === "number" ? expiryToMs(auth["expiresAt"]) : 0;
	const refreshExpiresAtMs = typeof auth["refreshExpiresAt"] === "number" ? expiryToMs(auth["refreshExpiresAt"]) : void 0;
	const lastRefreshAtMs = typeof auth["lastRefreshTime"] === "number" ? expiryToMs(auth["lastRefreshTime"]) : void 0;
	const enterpriseId = optionalCredentialField(identity["enterpriseId"], atRestKey);
	const nickname = optionalCredentialField(identity["nickname"], atRestKey);
	const uin = optionalCredentialField(identity["uin"], atRestKey);
	return {
		accessToken,
		refreshToken: refreshToken ?? "",
		expiresAtMs,
		...refreshExpiresAtMs === void 0 ? {} : { refreshExpiresAtMs },
		domain: optionalCredentialField(auth["domain"], atRestKey) ?? "",
		uid: optionalCredentialField(identity["uid"], atRestKey) ?? "",
		...enterpriseId === void 0 ? {} : { enterpriseId },
		...nickname === void 0 ? {} : { nickname },
		...uin === void 0 ? {} : { uin },
		...lastRefreshAtMs === void 0 ? {} : { lastRefreshAtMs },
		source: "desktop",
		filePath
	};
}
/**
* Whether a document carries any field the reader must decrypt — the token
* fields or the identity fields — i.e. whether reading it needs the at-rest key
* at all.
*
* Deciding this BEFORE asking the app for its key keeps the plain case (macOS
* and older Windows builds, and every plugin-owned copy) from paying for a
* child process on every credential read. Identity fields are included because
* a future build could encrypt the display name while leaving tokens plain;
* gating on tokens alone would then silently drop the name again.
*/
function hasEncryptedCredentialFields(text) {
	let parsed;
	try {
		parsed = JSON.parse(text);
	} catch {
		return false;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return false;
	const document = parsed;
	const auth = typeof document["auth"] === "object" && document["auth"] !== null ? document["auth"] : document;
	const identity = typeof document["account"] === "object" && document["account"] !== null ? document["account"] : document;
	return isEncryptedFieldWrapper(auth["accessToken"]) || isEncryptedFieldWrapper(auth["refreshToken"]) || isEncryptedFieldWrapper(identity["nickname"]) || isEncryptedFieldWrapper(identity["uin"]) || isEncryptedFieldWrapper(identity["uid"]) || isEncryptedFieldWrapper(identity["enterpriseId"]) || isEncryptedFieldWrapper(auth["domain"]);
}
/**
* Rank two candidate files for the same account.
*
* The live `workbuddy-desktop.info` always wins: it is the app's current
* sign-in, and the upstream revokes the tokens in the timestamped backups
* even though their stored `expiresAt` is still in the future (observed on a
* real machine — every backup claimed a 2027 expiry while only the live
* file's token was accepted). Expiry is therefore only a tie-breaker among
* backups, never the primary ordering.
*/
function fileRank(path) {
	return authFileName(path) === WORKBUDDY_LIVE_FILENAME ? 0 : 1;
}
/**
* Whether `candidate` is a better pick than `incumbent` for the same account.
*
* Ordering, strongest signal first:
*
* 1. the live `workbuddy-desktop.info` (the app's current sign-in);
* 2. the most recent `lastRefreshAtMs` — the upstream's own issuance time;
* 3. `expiresAtMs`, only as a fallback for documents that omit the field.
*
* Step 2 is what makes this correct. `expiresAt` describes how long the token
* was VALID FOR at issue time, not whether it is still accepted: a revoked
* backup keeps a far-future `expiresAt` (2027 in the observed case) and would
* otherwise outrank the working live credential, which is exactly how a
* signed-in account turned into an upstream HTML 401.
*/
function isFresher(candidate, incumbent) {
	const rankDiff = fileRank(candidate.filePath) - fileRank(incumbent.filePath);
	if (rankDiff !== 0) return rankDiff < 0;
	const candidateRefresh = candidate.lastRefreshAtMs;
	const incumbentRefresh = incumbent.lastRefreshAtMs;
	if (candidateRefresh !== void 0 && incumbentRefresh !== void 0) {
		if (candidateRefresh !== incumbentRefresh) return candidateRefresh > incumbentRefresh;
	} else if (candidateRefresh !== void 0) return true;
	else if (incumbentRefresh !== void 0) return false;
	return candidate.expiresAtMs > incumbent.expiresAtMs;
}
/**
* Filename of a path regardless of the host separator: Windows paths use `\`
* and this helper must keep working when a Windows path is compared on a
* POSIX host (e.g. tests injecting a Windows-style auth dir).
*/
function authFileName(path) {
	const separator = Math.max(path.lastIndexOf("/"), path.lastIndexOf("\\"));
	return separator === -1 ? path : path.slice(separator + 1);
}
/**
* The exact shape of an account id — sha256 hex sliced to 24 characters, which
* is what {@link workbuddyAccountId} emits and therefore the only string a
* vault file name can legitimately have.
*
* An id becomes a path segment, so this is also the traversal guard: an
* attacker-supplied `../..` is rejected before it can address anything outside
* the vault directory. Exported so the HTTP route can reject a bad id with a
* 400 instead of a thrown error.
*/
const VAULT_ACCOUNT_ID_RE = /^[0-9a-f]{24}$/;
/** Whether a string is a well-formed account id. */
function isVaultAccountId(value) {
	return typeof value === "string" && VAULT_ACCOUNT_ID_RE.test(value);
}
/**
* Stable account id. `uin` is the billing identity the upstream keys on and
* survives across re-login; `uid` is the fallback for documents without one.
*/
function workbuddyAccountId(credential) {
	const stable = credential.uin ?? credential.uid ?? credential.nickname ?? "unknown";
	return createHash("sha256").update(`workbuddy\0${stable}`).digest("hex").slice(0, 24);
}
/**
* Parse the plugin-owned copy; other versions and shapes are rejected.
*
* Pre-vault copies serialized the RUNTIME credential verbatim, whose expiry
* field is `expiresAtMs` (ms) rather than the desktop document's
* `expiresAt` (s) that `parseWorkBuddyAuth` reads — the round trip used to
* lose the expiry and force a refresh after every restart. The runtime field
* is read back here as the fallback.
*/
function parseOwnDocument(text, filePath) {
	let parsed;
	try {
		parsed = JSON.parse(text);
	} catch {
		return;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return void 0;
	const document = parsed;
	if (document["version"] !== OWN_FORMAT_VERSION) return void 0;
	if (typeof document["credential"] !== "object" || document["credential"] === null) return void 0;
	const credential = parseWorkBuddyAuth(JSON.stringify({ auth: document["credential"] }), filePath);
	if (credential === void 0) return void 0;
	const runtime = document["credential"];
	const legacyExpiry = typeof runtime["expiresAtMs"] === "number" ? expiryToMs(runtime["expiresAtMs"]) : 0;
	return {
		...credential,
		...credential.expiresAtMs === 0 && legacyExpiry > 0 ? { expiresAtMs: legacyExpiry } : {},
		source: "dsh"
	};
}
/** Whether a filesystem error reports an absent path. */
function isENOENT(error) {
	return error?.code === "ENOENT";
}
/**
* Whether a freshly scanned credential should REPLACE a vault entry of the
* same account.
*
* The vault is a CACHE of credentials the machine already holds, not a
* competing source. Two clocks decide, compared like against like:
*
* - The scan's clock is the DESKTOP document's `lastRefreshTime` (the
*   upstream's own issuance time) — and the live file ranks above every
*   backup regardless of time, per {@link fileRank}.
* - The vault entry's clock is `refreshedAtMs`, the local write time of the
*   plugin refresh (or OAuth login) that produced it. A wall clock is not
*   comparable to an issuance time, so the two are NOT mixed: the entry is
*   replaced only when the scan's read is the live sign-in AND either the
*   entry predates the scan read's issuance, or the entry was written before
*   the plugin refreshed into it — approximated by requiring the live file's
*   issuance to be NEWER than the entry's write time.
*
* The rule's purpose is breaking the refresh loop: without it, a scan keeps
* reinstating the (older, soon-to-expire) desktop token over the refreshed
* vault copy, so every restart re-refreshes every account. With it, the
* desktop token wins only while it is genuinely newer than what the plugin
* last wrote — after that the refreshed copy is the operative credential,
* exactly like the pre-vault per-region copies behaved.
*/
function shouldReplaceVaultEntry(candidate, incumbent) {
	if (fileRank(candidate.filePath) !== 0) return false;
	const writeMs = incumbent.refreshedAtMs;
	if (writeMs === void 0) return true;
	const issuanceMs = candidate.lastRefreshAtMs;
	if (issuanceMs === void 0) return candidate.expiresAtMs > incumbent.credential.expiresAtMs;
	return issuanceMs > writeMs;
}
/**
* Read every vault entry for one region directory.
*
* The map is keyed by account id, which the FILE NAME already carries: the
* id inside the document is checked against it and a mismatched file is
* skipped — a file copied between accounts (or a half-written rename) must
* not silently answer for the wrong account.
*/
async function readVaultEntries(dir) {
	const entries = /* @__PURE__ */ new Map();
	let names;
	try {
		names = await readdir(dir);
	} catch (error) {
		if (isENOENT(error)) return entries;
		throw error;
	}
	for (const name of names) {
		if (!isVaultCredentialFileName(name)) continue;
		const path = join(dir, name);
		try {
			const parsed = parseVaultDocumentWithWriteTime(await readFile(path, "utf8"), path);
			if (parsed === void 0) continue;
			const id = name.slice(0, -5);
			if (workbuddyAccountId(parsed.credential) !== id) continue;
			entries.set(id, parsed);
		} catch {}
	}
	return entries;
}
/** Write one vault entry atomically under the file lock. */
async function writeVaultEntry(dir, accountId, credential, refreshedFrom) {
	const path = join(dir, `${accountId}.json`);
	await withFileLock(path, async () => {
		await writeFileAtomic(path, `${JSON.stringify(vaultDocument(credential, accountId, refreshedFrom), null, 2)}\n`, {
			mode: 384,
			dirMode: 448
		});
	});
}
/**
* Probe one auth file, reporting WHY it yielded no credential.
*
* The desktop app encrypts its token fields on Windows builds. The plain read
* is tried first and the app is only asked for its at-rest key when the
* document actually carries encrypted wrappers, so the common case costs no
* child process. `resolveAtRestKey` is injectable so the encrypted path is
* testable without a real desktop install.
*/
async function probeAuthFile(path, resolveAtRestKey) {
	let text;
	try {
		text = await readFile(path, "utf8");
	} catch (error) {
		const missing = isENOENT(error);
		return { failure: {
			reason: missing ? "missing" : "unreadable",
			...missing ? {} : { message: error instanceof Error ? error.message : String(error) }
		} };
	}
	const plain = parseWorkBuddyAuth(text, path);
	if (plain !== void 0) return { credential: plain };
	if (!hasEncryptedCredentialFields(text)) return isParseableJson(text) ? { failure: {
		reason: "invalid",
		message: "no access token in the document"
	} } : { failure: {
		reason: "invalid",
		message: "the file is not valid JSON"
	} };
	let key;
	try {
		key = await resolveAtRestKey();
	} catch (error) {
		return { failure: {
			reason: "encrypted",
			message: error instanceof Error ? error.message : String(error)
		} };
	}
	if (key === void 0) return { failure: {
		reason: "encrypted",
		message: `the credential fields are encrypted and no key was available; the WorkBuddy desktop app must be present (or set ${WORKBUDDY_APP_EXECUTABLE_ENV})`
	} };
	const decrypted = parseWorkBuddyAuth(text, path, key);
	return decrypted === void 0 ? { failure: {
		reason: "invalid",
		message: "the encrypted credential fields could not be opened with the desktop app's key"
	} } : { credential: decrypted };
}
/** Whether text parses as JSON at all; distinguishes "wrong shape" from "not JSON". */
function isParseableJson(text) {
	try {
		JSON.parse(text);
		return true;
	} catch {
		return false;
	}
}
/**
* Read one auth file, tolerating absence and unparsable content. Kept as the
* credential-only view for callers that do not need the failure reason.
*/
async function readAuthFile(path, resolveAtRestKey) {
	const probe = await probeAuthFile(path, resolveAtRestKey);
	return "credential" in probe ? probe.credential : void 0;
}
/**
* Read-only credential store with demand-driven refresh and multi-account
* discovery.
*
* Refresh policy: refresh only when the access token is inside the margin
* (or already expired), keep the refreshed credential in the plugin-owned
* copy, and never write the desktop app's files. A failed refresh still
* returns a not-yet-expired token so an unreachable refresh endpoint does
* not take down a working session.
*/
var WorkBuddyCredentialStore = class {
	refresh;
	refreshMarginMs;
	region;
	ownPathExplicit;
	legacyOwnPath;
	legacyOwnPathExplicit;
	vaultDirExplicit;
	/** The vault directory, resolved ONCE at construction. */
	vaultDirResolved;
	authDirs;
	resolveAtRestKey;
	desktopPathOverride;
	accountId;
	inflight;
	/**
	* In-flight/completed scan for the CURRENT `readAll` generation.
	*
	* One page open calls `readAll` six times: the usage route asks for the
	* account list, the sign-in state and the resolved credential, then the
	* per-account credits panel asks once PER ACCOUNT (`credentialFor`), and both
	* region tabs poll on the same timer. Each call re-read every auth file,
	* re-parsed it, and re-ran the vault merge — measured at 6 `readdir` + (6N+12)
	* `readFile` for N accounts, i.e. the cost grew with the account count on a
	* path whose answer cannot change within one tick.
	*
	* This is deliberately NOT a time-based cache: a TTL would make the card show
	* a stale account list after an OAuth add or an import, which is the very
	* class of bug this card keeps chasing. Instead the promise is shared while it
	* is in flight, and every WRITE clears it (see `invalidateScan`) — so a read
	* after a write always scans again, and a read during a scan joins it.
	*
	* Undefined means "no scan in flight and nothing cached".
	*/
	scanInflight;
	/**
	* When the cached scan FINISHED, epoch ms; 0 means "not finished yet".
	*
	* Deliberately the finish time, not the start time (issue #77). The window
	* used to be measured from the START, so a scan that took longer than the
	* window was already outside it the moment it landed: the very next `readAll`
	* started a fresh scan, and the sharing this cache exists for never happened.
	* On the machine in that report one scan took 1.4–2.1 s against a 1000 ms
	* window, so every one of a page open's ~6 reads re-scanned the filesystem.
	*/
	scanFinishedAtMs = 0;
	/**
	* A RUNTIME account override from the pool's rotation, or undefined.
	*
	* Deliberately separate from {@link accountId}, which is the user's persisted
	* choice. Rotation decides who serves *now*; it must never rewrite what the
	* user picked, or "turn rotation off" could not restore their selection and
	* the plugin would be unable to tell the two apart afterwards. Nothing here
	* is ever written to settings.
	*/
	rotatedAccountId;
	constructor(options) {
		this.refresh = options.refresh;
		this.refreshMarginMs = options.refreshMarginMs ?? 3e5;
		this.region = options.region;
		this.ownPathExplicit = options.ownPath;
		this.legacyOwnPath = options.legacyOwnPath ?? legacyWorkbuddyOwnAuthPath();
		this.legacyOwnPathExplicit = options.legacyOwnPath;
		this.vaultDirExplicit = options.vaultDir;
		this.vaultDirResolved = options.vaultDir ?? (options.region !== void 0 ? workbuddyVaultDir(options.region) : join(resolveDshHome(), "workbuddy-vault"));
		this.authDirs = options.authDirs;
		this.desktopPathOverride = options.desktopPath;
		this.resolveAtRestKey = options.resolveAtRestKey ?? readAtRestKey;
	}
	/** Whether a credential's login domain belongs to this store's region. */
	matchesRegion(domain) {
		return this.region === void 0 || regionOf(domain) === this.region;
	}
	/**
	* The path this store refreshes into: the per-region file for a
	* region-scoped store, the legacy single file otherwise, or an explicitly
	* injected path in tests.
	*/
	ownAuthPath() {
		if (this.ownPathExplicit !== void 0) return this.ownPathExplicit;
		return this.region !== void 0 ? workbuddyOwnAuthPath(this.region) : this.legacyOwnPath;
	}
	/**
	* Every plugin-owned copy to read, most preferred first. A region-scoped
	* store reads the legacy single copy as its migration source (readAll's
	* region filter drops it when it carries the other region's credential); an
	* unscoped store reads everything so diagnostics see both regions.
	*
	* With an explicitly injected own path the legacy source is read ONLY when
	* it was injected too — a test that pins one file must not accidentally see
	* the real machine's legacy copy.
	*/
	ownCandidates() {
		if (this.ownPathExplicit !== void 0) return this.legacyOwnPathExplicit !== void 0 ? [this.ownPathExplicit, this.legacyOwnPathExplicit] : [this.ownPathExplicit];
		if (this.region !== void 0) return [workbuddyOwnAuthPath(this.region), this.legacyOwnPath];
		return [
			this.legacyOwnPath,
			workbuddyOwnAuthPath("cn"),
			workbuddyOwnAuthPath("global")
		];
	}
	/** The vault directory this store reads and writes (fixed at construction). */
	vaultDir() {
		return this.vaultDirResolved;
	}
	/** Repoint the desktop file or directory; applies on the next read. */
	setDesktopPath(path) {
		this.desktopPathOverride = path;
		this.inflight = void 0;
		this.invalidateScan();
	}
	/**
	* Select an account by id; tokens stay outside settings.
	*
	* The empty string is the settings-level sentinel for "no explicit
	* selection" (the card's Clear action writes it), so it is normalized here
	* rather than being kept as an id that can never match an account. Every
	* caller therefore gets the documented default — follow the app's current
	* sign-in — instead of a dead selection.
	*/
	selectAccount(accountId) {
		this.accountId = accountId === "" ? void 0 : accountId;
		this.inflight = void 0;
		this.invalidateScan();
	}
	/** Selected account id, for diagnostics and route assembly. */
	selectedAccountId() {
		return this.accountId;
	}
	/**
	* Whether the region runs a SAVED choice rather than the documented default.
	*
	* `false` means "follow the app's current sign-in", which is also what the
	* card's Clear action restores. The two states can resolve to the very same
	* account — an upgraded user whose pre-split `accountId` happens to be the
	* app's current sign-in clears the choice and sees no change at all — so the
	* card cannot infer this from the account list alone. It reports the state
	* here so clearing is observable instead of looking like a dead button.
	*/
	hasExplicitSelection() {
		return this.accountId !== void 0;
	}
	/** The auth-file path candidates, in probe order. */
	resolveDesktopCandidates() {
		const fromEnv = process.env[WORKBUDDY_AUTH_FILE_ENV];
		const explicit = this.desktopPathOverride ?? (fromEnv !== void 0 && fromEnv.trim() !== "" ? fromEnv : void 0);
		if (explicit !== void 0) return [explicit];
		return defaultDesktopAuthCandidates();
	}
	/** The resolved desktop auth-file path, for diagnostics. */
	desktopAuthPath() {
		return this.resolveDesktopCandidates()[0];
	}
	/**
	* Every auth file to scan: the live file plus the timestamped backups
	* WorkBuddy leaves beside it.
	*
	* An explicitly configured path pins the *directory*: its siblings are
	* still scanned, because a user who points the plugin at their auth file
	* expects account switching to work the same way it does on the default
	* path. Only the file ordering changes.
	*
	* A corrupt or signed-out file must never hide the others, so each read is
	* independent and failures are skipped rather than propagated.
	*/
	async candidateFiles() {
		const explicitPath = this.desktopPathOverride ?? ((process.env["WORKBUDDY_AUTH_FILE"] ?? "").trim() !== "" ? process.env["WORKBUDDY_AUTH_FILE"] : void 0);
		const files = [];
		if (explicitPath !== void 0) {
			files.push(explicitPath);
			for (const backup of await this.backupsBeside(explicitPath)) files.push(backup);
			return files;
		}
		const dirs = this.authDirs ?? defaultDesktopAuthDirs();
		for (const dir of dirs) {
			const live = join(dir, WORKBUDDY_LIVE_FILENAME);
			files.push(live);
			for (const backup of await this.backupsBeside(live)) files.push(backup);
		}
		return files;
	}
	/** Timestamped siblings of one auth file, newest first by filename. */
	async backupsBeside(path) {
		const dir = dirname(path);
		const base = path.slice(dir.length + 1);
		try {
			return (await readdir(dir)).filter((name) => name !== base && name.endsWith(".info")).sort().reverse().map((name) => join(dir, name));
		} catch {
			return [];
		}
	}
	/**
	* Read every local credential, deduplicated by account id. Files are
	* probed newest-first, so the first entry for an account is its freshest.
	*
	* A region-scoped store sees only its own region's credentials: the other
	* region's accounts are invisible to selection, refresh, and status alike,
	* which is what keeps the two regions' providers from cross-billing.
	*/
	/**
	* The account list, sharing one scan across concurrent and repeated reads.
	*
	* See {@link scanInflight}: the whole performance fix lives in these lines.
	* Every caller keeps its OWN semantics (`current`, `accounts`, `status`,
	* `selectionLost`), because this returns the same array they used to build
	* themselves — only the filesystem work is shared.
	*
	* The window runs from the FINISH time ({@link scanFinishedAtMs}), so a scan
	* slower than the window still serves the reads that follow it. A scan in
	* flight is always joined, however long it has been running.
	*
	* A failed scan is NOT cached: the rejection clears the slot so the next read
	* retries, rather than pinning every later caller to one transient failure.
	*/
	async readAll() {
		if (this.scanInflight !== void 0) {
			if (this.scanFinishedAtMs === 0 || Date.now() - this.scanFinishedAtMs < SCAN_SHARE_WINDOW_MS) return this.scanInflight;
		}
		this.scanFinishedAtMs = 0;
		const scan = this.scanAll().then((credentials) => {
			this.scanFinishedAtMs = Date.now();
			return credentials;
		}, (error) => {
			if (this.scanInflight === scan) this.scanInflight = void 0;
			this.scanFinishedAtMs = 0;
			throw error;
		});
		this.scanInflight = scan;
		return scan;
	}
	/** Drop the shared scan so the next read re-reads the filesystem. */
	invalidateScan() {
		this.scanInflight = void 0;
		this.scanFinishedAtMs = 0;
	}
	/** The one real scan: read every candidate file, then merge the vault. */
	async scanAll() {
		const files = await this.candidateFiles();
		const dismissed = await readDismissed(this.vaultDir());
		const byId = /* @__PURE__ */ new Map();
		for (const file of files) {
			const credential = await readAuthFile(file, this.resolveAtRestKey);
			if (credential === void 0 || !this.matchesRegion(credential.domain)) continue;
			const id = workbuddyAccountId(credential);
			if (dismissed.has(id)) continue;
			const existing = byId.get(id);
			if (existing === void 0) {
				byId.set(id, credential);
				continue;
			}
			if (isFresher(credential, existing)) byId.set(id, credential);
		}
		for (const own of await this.readOwns()) {
			if (!this.matchesRegion(own.domain)) continue;
			const id = workbuddyAccountId(own);
			if (dismissed.has(id)) continue;
			const existing = byId.get(id);
			if (existing === void 0) byId.set(id, own);
			else if (fileRank(existing.filePath) !== 0 && own.expiresAtMs > existing.expiresAtMs) byId.set(id, own);
		}
		await this.syncVault(byId, dismissed);
		return Object.freeze([...byId.values()]);
	}
	/**
	* Merge the scan's credentials into the vault and read the vault back.
	*
	* Incremental by contract: a scan that finds nothing new writes nothing.
	* Per account, the vault entry is written only when the scan's read is
	* fresher by {@link isFresherVaultEntry} — so a steady-state scan is
	* mtime-stable and every sync is O(accounts) reads with at most one write
	* per CHANGED account.
	*/
	async syncVault(scan, dismissed) {
		const dir = this.vaultDir();
		let entries;
		try {
			entries = await readVaultEntries(dir);
		} catch {
			return;
		}
		let dirReady = false;
		for (const [id, credential] of scan) {
			if (dismissed.has(id)) continue;
			const incumbent = entries.get(id);
			if (incumbent !== void 0 && !shouldReplaceVaultEntry(credential, incumbent)) continue;
			if (!dirReady) try {
				await mkdir(dir, { recursive: true });
				dirReady = true;
			} catch {
				return;
			}
			await writeVaultEntry(dir, id, credential);
			entries.set(id, {
				credential: {
					...credential,
					source: "dsh"
				},
				refreshedAtMs: Date.now()
			});
		}
		for (const [id, entry] of entries) {
			if (!this.matchesRegion(entry.credential.domain)) continue;
			const existing = scan.get(id);
			if (dismissed.has(id)) continue;
			if (existing === void 0) scan.set(id, entry.credential);
			else if (entry.refreshedFrom !== void 0 && entry.refreshedFrom === authFileName(existing.filePath)) scan.set(id, entry.credential);
			else if (fileRank(existing.filePath) !== 0 && entry.credential.expiresAtMs > existing.expiresAtMs) scan.set(id, entry.credential);
		}
	}
	/**
	* Default when no account is explicitly selected: the live sign-in, else the
	* freshest credential. Following the app's current sign-in is the documented
	* default behaviour; the backups exist so the user can switch explicitly.
	* This is NOT credit-seeking — it never reorders accounts to find one with
	* remaining credit.
	*/
	preferred(credentials) {
		if (credentials.length === 0) return void 0;
		return credentials.reduce((best, credential) => isFresher(credential, best) ? credential : best);
	}
	/**
	* Token-free account list for the plugin card.
	*
	* `selected` answers one question only: which row is the account the plugin
	* is actually going to use? That is exactly what {@link current} decides, so
	* the two must never disagree — the card renders this list while the shim
	* bills through `current()`, and a row marked "selected" that `current()`
	* refuses to use is what made the dropdown look healthy while every request
	* failed with 401.
	*
	* So an explicit selection that matches NO local account marks nothing as
	* selected (no silent fallback to a different account — see {@link current}).
	* The implicit default is marked only when nothing was explicitly chosen.
	*/
	async accounts() {
		const credentials = await this.readAll();
		if (credentials.length === 0) return [];
		const hasExplicitSelection = this.accountId !== void 0;
		const selectedExists = hasExplicitSelection && credentials.some((credential) => workbuddyAccountId(credential) === this.accountId);
		const defaultSelected = this.preferred(credentials);
		return credentials.map((credential) => {
			const id = workbuddyAccountId(credential);
			return {
				id,
				accountName: credential.nickname ?? "",
				domain: credential.domain,
				source: credential.source,
				tokenExpiresAtMs: credential.expiresAtMs,
				filePath: credential.filePath,
				selected: selectedExists ? id === this.accountId : !hasExplicitSelection && credential === defaultSelected
			};
		});
	}
	/**
	* Whether a persisted selection exists that matches no local account, while
	* other local sign-ins ARE available to choose from.
	*
	* The card uses this to explain the state honestly (the account is signed in,
	* but the SAVED choice is gone) instead of showing the generic "sign in
	* again" hint, which misdirects: the token is usually perfectly healthy and
	* signing in again does not repair an orphaned id. When no local sign-in is
	* available at all, that hint IS accurate and this returns false.
	*/
	async selectionLost() {
		if (this.accountId === void 0) return false;
		const credentials = await this.readAll();
		if (credentials.length === 0) return false;
		return !credentials.some((credential) => workbuddyAccountId(credential) === this.accountId);
	}
	/** The freshest stored credential for the current selection, no refresh. */
	async current() {
		const credentials = await this.readAll();
		if (this.rotatedAccountId !== void 0) {
			const rotated = credentials.find((credential) => workbuddyAccountId(credential) === this.rotatedAccountId);
			if (rotated !== void 0) return rotated;
		}
		if (this.accountId === void 0) return this.preferred(credentials);
		return credentials.find((credential) => workbuddyAccountId(credential) === this.accountId);
	}
	/**
	* Set (or clear) the pool's RUNTIME account override.
	*
	* Never persists and never touches {@link accountId}: the user's saved choice
	* is what `clear` restores, which is the whole reason this lives in its own
	* field. Rotation is off when this is called with `undefined`.
	*/
	setRotatedAccount(accountId) {
		this.rotatedAccountId = accountId;
	}
	/** The account the pool is currently rotating to, if any. */
	rotatedAccount() {
		return this.rotatedAccountId;
	}
	/**
	* One account's stored credential by id, WITHOUT consulting or changing the
	* current selection.
	*
	* Exists so a recovery probe can test whether some OTHER local account is
	* still accepted upstream while a rejected one stays selected: mutating the
	* live selection to find that out would be exactly the silent account switch
	* this store refuses to perform (it would bill the wrong account mid-probe).
	*/
	async credentialFor(accountId) {
		return (await this.readAll()).find((credential) => workbuddyAccountId(credential) === accountId);
	}
	/** The credential to send upstream: {@link current}, refreshed on demand. */
	async resolve() {
		const credential = await this.current();
		if (credential === void 0) throw await this.describeMissingCredential();
		if (!this.needsRefresh(credential)) return credential;
		this.inflight ??= this.refreshNow(credential).finally(() => {
			this.inflight = void 0;
		});
		return this.inflight;
	}
	/**
	* WHY no credential resolved, as the error to throw.
	*
	* `resolve()` seeing zero accounts has two very different causes, and the
	* generic "sign in once" message is only correct for one of them. When a
	* probe finds a credential file whose fields are encrypted and the key could
	* not be obtained, the user is signed in and re-authenticating cannot help —
	* so that case gets {@link WorkBuddyEncryptedCredentialError} instead, naming
	* the file and the environment variable that actually fixes it.
	*
	* The probe is only run on the failure path, so the healthy case pays nothing.
	* A probe that itself throws must not replace the real error, hence the
	* fallback to the generic message.
	*/
	async describeMissingCredential() {
		let failures = [];
		try {
			failures = (await this.diagnose()).failures;
		} catch {}
		const encrypted = failures.filter((failure) => failure.reason === "encrypted").map((failure) => failure.path);
		if (encrypted.length > 0) return new WorkBuddyEncryptedCredentialError(encrypted);
		const candidates = this.resolveDesktopCandidates();
		const desktop = candidates.length > 0 ? candidates.join(" or ") : "(no desktop path on this platform)";
		return /* @__PURE__ */ new Error(`workbuddy: no signed-in WorkBuddy account found; sign in once in the WorkBuddy desktop app (expected ${desktop} or ${WORKBUDDY_AUTH_FILE_ENV}), or refresh an existing session`);
	}
	/** Read-only sign-in summary; never refreshes and never throws. */
	async status() {
		try {
			const credential = await this.current();
			if (credential === void 0) return { state: "signed-out" };
			return {
				state: "signed-in",
				expiresAtMs: credential.expiresAtMs,
				...credential.refreshExpiresAtMs === void 0 ? {} : { refreshExpiresAtMs: credential.refreshExpiresAtMs },
				...credential.nickname === void 0 ? {} : { nickname: credential.nickname },
				...credential.domain === "" ? {} : { domain: credential.domain },
				source: credential.source
			};
		} catch {
			return { state: "signed-out" };
		}
	}
	/**
	* Persist one OAuth sign-in into the unified vault.
	*
	* The credential is written with `source: 'dsh'` (it never came from a
	* desktop file) and joins the readable set on the next scan — the vault is
	* read by `readAll()` like any other source, so no separate discovery path
	* exists. The write is idempotent per account id: re-adding an account
	* replaces its entry, which is exactly what re-issuing a login means.
	*
	* Returns the account id so the card can offer to select it right away.
	*/
	async addOAuthAccount(credential) {
		const id = workbuddyAccountId(credential);
		const dir = this.vaultDir();
		await mkdir(dir, { recursive: true });
		await writeVaultEntry(dir, id, {
			...credential,
			source: "dsh",
			filePath: join(dir, `${id}.json`)
		});
		await this.forgetDismissal(dir, id);
		this.inflight = void 0;
		this.invalidateScan();
		return id;
	}
	/**
	* Drop one id from the dismissal list, if present.
	*
	* Silent by design: called on the ADD paths, where a list that cannot be
	* updated must not fail the action the user asked for. The consequence is the
	* same as before the fix — the account stays hidden until the list is
	* repaired — while throwing here would report a failed sign-in that succeeded.
	*/
	async forgetDismissal(dir, accountId) {
		try {
			const dismissed = await readDismissed(dir);
			if (!dismissed.has(accountId)) return;
			dismissed.delete(accountId);
			await writeDismissed(dir, dismissed);
		} catch {}
	}
	/**
	* Forget EVERY dismissal, so the next read lists the accounts again.
	*
	* Called by the manual 「re-scan」 action. Dismissal is "hide until I ask to
	* scan again", and this is that ask: the user is requesting what the machine
	* currently has, which includes accounts they hid earlier. Leaving the list
	* intact would make a removed desktop account unrecoverable from the UI — the
	* opposite of what the button promises.
	*
	* PREFERS deleting the file over writing an empty list: an absent file is the
	* normal state and keeps the vault directory free of a file that says nothing.
	* The unlink is not the only way, though, because on Windows removing a file
	* another process holds open fails with `EPERM`/`EBUSY`/`EACCES` — an antivirus
	* scanner, OneDrive, or an editor with it open, the same contention the card's
	* `isFileContentionWriteError` classifies for profile writes and the reason
	* `docs/WINDOWS.md` keeps a register. POSIX unlinks open files happily, so
	* without this the promise 「点重新检测账号即可找回」 would be false, silently,
	* on exactly the platform it matters most on.
	*
	* So the unlink falls back to OVERWRITING the list with an empty one through
	* the same locked atomic write `writeDismissed` uses. For every reader the two
	* outcomes are identical (`readDismissed` yields no ids either way), so the
	* user's ask is honoured even when the file itself cannot be removed.
	*
	* Returns whether the dismissals are now genuinely gone, VERIFIED by reading
	* the list back rather than by trusting the write: the caller renders this, so
	* "it worked" has to mean it. A `false` is the honest answer for a vault that
	* could neither be unlinked nor rewritten, and the rescan still runs — the
	* accounts simply stay hidden, which the UI can now say out loud instead of
	* showing a button that appears to have done nothing.
	*/
	async clearDismissals() {
		this.inflight = void 0;
		this.invalidateScan();
		const dir = this.vaultDir();
		const path = join(dir, VAULT_DISMISSED_FILENAME);
		try {
			await rm(path, { force: true });
		} catch {
			try {
				await writeDismissed(dir, /* @__PURE__ */ new Set());
			} catch {
				return (await readDismissed(dir)).size === 0;
			}
		}
		return (await readDismissed(dir)).size === 0;
	}
	/**
	* Remove ONE account from this region's list, by id.
	*
	* The account is DISMISSED, not merely deleted. Deleting the vault entry alone
	* was undone by the next scan: `syncVault` writes every desktop sign-in it
	* finds back into the vault (that is how a desktop account becomes selectable)
	* and `readAll` merges the vault in afterwards, so a removed desktop account
	* returned on the very next read while `removeAccount` had reported `true`.
	* Measured before the fix: remove → `true`, then `accounts()` still lists it.
	*
	* The desktop app's auth files stay read-only: dismissing hides the account
	* from this plugin without signing the app out, and that is exactly what the
	* card says. Re-adding it (a fresh OAuth sign-in or an import) clears the
	* dismissal, so the account is not lost to a future login.
	*
	* Returns whether a stored vault entry was also removed, so the caller can
	* still tell "removed" from "only a desktop sign-in" — the dismissal happens
	* either way, because only the user gets to decide an account is dismissed.
	*/
	async removeAccount(accountId) {
		if (!VAULT_ACCOUNT_ID_RE.test(accountId)) throw new Error(`workbuddy: refusing to remove '${accountId}': not a vault account id`);
		const path = join(this.vaultDir(), `${accountId}.json`);
		const removed = await withFileLock(path, async () => {
			let existed = true;
			try {
				await rm(path, { force: false });
			} catch (error) {
				if (!isENOENT(error)) throw error;
				existed = false;
			}
			await rm(`${path}.lock`, { force: false }).catch(() => {});
			return existed;
		});
		if (this.rotatedAccountId === accountId) this.rotatedAccountId = void 0;
		if (this.accountId === accountId) this.accountId = void 0;
		const dir = this.vaultDir();
		try {
			await mkdir(dir, { recursive: true });
			const dismissed = await readDismissed(dir);
			if (!dismissed.has(accountId)) {
				dismissed.add(accountId);
				await writeDismissed(dir, dismissed);
			}
		} catch {}
		this.inflight = void 0;
		this.invalidateScan();
		return removed;
	}
	/**
	* Remove every plugin-owned copy this store could read (per-region file,
	* legacy single file, and their lock siblings); the desktop files are
	* untouched. A region store's logout therefore also clears the legacy
	* migration source — deliberate: `logout` is the user's "forget what the
	* plugin stored" action, not a per-account toggle.
	*/
	async logout() {
		for (const path of this.ownCandidates()) {
			await rm(path, { force: true });
			await rm(`${path}.lock`, { force: true });
		}
		await rm(this.vaultDir(), {
			recursive: true,
			force: true
		});
		this.invalidateScan();
	}
	needsRefresh(credential) {
		if (credential.expiresAtMs <= 0) return true;
		return Date.now() + this.refreshMarginMs >= credential.expiresAtMs;
	}
	async refreshNow(credential) {
		if (credential.refreshToken === "") {
			if (credential.expiresAtMs > Date.now() + 3e4) return credential;
			throw new Error("workbuddy: access token expired and no refresh token is stored; sign in again in the WorkBuddy desktop app");
		}
		try {
			const outcome = await this.refresh(credential);
			const refreshed = {
				...credential,
				accessToken: outcome.accessToken,
				...outcome.refreshToken === void 0 ? {} : { refreshToken: outcome.refreshToken },
				expiresAtMs: outcome.expiresInSec !== void 0 ? Date.now() + outcome.expiresInSec * 1e3 : credential.expiresAtMs,
				...outcome.domain === void 0 || outcome.domain === "" ? {} : { domain: outcome.domain },
				source: "dsh"
			};
			this.invalidateScan();
			await writeVaultEntry(this.vaultDir(), workbuddyAccountId(credential), refreshed, credential.filePath).catch(() => {});
			return refreshed;
		} catch (error) {
			if (credential.expiresAtMs > Date.now() + 3e4) return credential;
			throw new Error(`workbuddy: token refresh failed and the access token is expired (${String(error)}); open the WorkBuddy desktop app once to sign in again`);
		}
	}
	/**
	* Every readable plugin-owned copy, in candidate order; absent or corrupt
	* files are skipped rather than propagated.
	*/
	async readOwns() {
		const copies = [];
		for (const path of this.ownCandidates()) try {
			const parsed = parseOwnDocument(await readFile(path, "utf8"), path);
			if (parsed !== void 0) copies.push(parsed);
		} catch {}
		return copies;
	}
	/** Whether any candidate file exists as a regular file; diagnostics only. */
	async desktopFilePresent() {
		for (const path of this.resolveDesktopCandidates()) try {
			if ((await stat(path)).isFile()) return true;
		} catch {}
		return false;
	}
	/**
	* Which paths were probed and why each one yielded no credential.
	*
	* Read-only and token-free: it exists so a signed-out card can explain
	* itself. A bare "not signed in" is undiagnosable on a machine whose layout
	* differs from the ones this plugin was written against — and on Windows it
	* is actively misleading, because encrypted token fields need the desktop app
	* present to be read at all. Only paths this store actually consults are
	* reported, and only files that failed: one healthy sibling would make the
	* whole list noise.
	*/
	async diagnose() {
		const candidates = [];
		for (const path of await this.candidateFiles()) {
			const probe = await probeAuthFile(path, this.resolveAtRestKey);
			if ("credential" in probe) {
				if (!this.matchesRegion(probe.credential.domain)) candidates.push({
					path,
					source: "desktop",
					reason: "wrong-region",
					message: `holds a ${regionOf(probe.credential.domain)} sign-in, but this tab reads the ${this.region} region`
				});
				continue;
			}
			candidates.push({
				path,
				source: "desktop",
				...probe.failure
			});
		}
		for (const path of this.ownCandidates()) {
			let text;
			try {
				text = await readFile(path, "utf8");
			} catch (error) {
				if (isENOENT(error)) continue;
				candidates.push({
					path,
					source: "dsh",
					reason: "unreadable",
					message: error instanceof Error ? error.message : String(error)
				});
				continue;
			}
			const parsed = parseOwnDocument(text, path);
			if (parsed === void 0 || !this.matchesRegion(parsed.domain)) {
				candidates.push({
					path,
					source: "dsh",
					reason: "invalid",
					message: "not a readable plugin-owned credential"
				});
				continue;
			}
		}
		const tried = [...await this.candidateFiles(), ...this.ownCandidates()];
		try {
			const names = await readdir(this.vaultDir());
			for (const name of names.filter(isVaultCredentialFileName)) {
				const path = join(this.vaultDir(), name);
				tried.push(path);
				let text;
				try {
					text = await readFile(path, "utf8");
				} catch (error) {
					if (!isENOENT(error)) candidates.push({
						path,
						source: "dsh",
						reason: "unreadable",
						message: error instanceof Error ? error.message : String(error)
					});
					continue;
				}
				if (parseVaultDocument(text, path) === void 0) candidates.push({
					path,
					source: "dsh",
					reason: "invalid",
					message: "not a readable vault credential"
				});
			}
		} catch {}
		return {
			tried,
			failures: candidates
		};
	}
};
//#endregion
//#region src/catalog.ts
/**
* Static CLI models captured from the CN endpoint (2026-08-30). The upstream
* refresh replaces this list at startup; it exists so the provider registers
* with a usable catalog even while the first fetch is in flight or offline.
*/
const FALLBACK_WORKBUDDY_MODELS = [
	{
		id: "auto",
		name: "Auto",
		contextWindow: 168e3,
		maxTokens: 32e3
	},
	{
		id: "hy3",
		name: "Hy3",
		contextWindow: 192e3,
		maxTokens: 64e3
	},
	{
		id: "glm-5v-turbo",
		name: "GLM-5v-Turbo",
		contextWindow: 2e5,
		maxTokens: 64e3
	},
	{
		id: "glm-5.3",
		name: "GLM-5.3",
		contextWindow: 1e6,
		maxTokens: 48e3
	},
	{
		id: "glm-5.2",
		name: "GLM-5.2",
		contextWindow: 1e6,
		maxTokens: 48e3
	},
	{
		id: "glm-5.1",
		name: "GLM-5.1",
		contextWindow: 2e5,
		maxTokens: 48e3
	},
	{
		id: "minimax-m3",
		name: "MiniMax-M3",
		contextWindow: 512e3,
		maxTokens: 128e3
	},
	{
		id: "kimi-k3-1",
		name: "Kimi-K3",
		contextWindow: 1e6,
		maxTokens: 32e3
	},
	{
		id: "kimi-k2.7",
		name: "Kimi-K2.7-Code",
		contextWindow: 256e3,
		maxTokens: 32e3
	},
	{
		id: "kimi-k2.6",
		name: "Kimi-K2.6",
		contextWindow: 256e3,
		maxTokens: 32e3
	},
	{
		id: "deepseek-v4-flash",
		name: "Deepseek-V4-Flash",
		contextWindow: 1e6,
		maxTokens: 5e4
	},
	{
		id: "deepseek-v4-pro",
		name: "Deepseek-V4-Pro",
		contextWindow: 1e6,
		maxTokens: 5e4
	}
];
/**
* Static CLI models captured from the INTERNATIONAL gateway's desktop-channel
* product config (`www.workbuddy.ai/v3/config`, 2026-09-11). The two regions
* expose different rosters — the CN list has no `gpt-*`/`gemini-*` entries —
* so a global account must never be seeded with the CN list. Like the CN
* fallback this is replaced by the live refresh; it only keeps the provider
* usable before the first fetch lands. Order and rates mirror the upstream.
*/
const FALLBACK_WORKBUDDY_MODELS_GLOBAL = [
	{
		id: "default-model",
		name: "Auto",
		contextWindow: 176e3,
		maxTokens: 24e3,
		creditMultiplier: .79
	},
	{
		id: "fast-model",
		name: "Fast",
		contextWindow: 2e5,
		maxTokens: 32e3,
		creditMultiplier: .34
	},
	{
		id: "balanced-model",
		name: "Balanced",
		contextWindow: 256e3,
		maxTokens: 32e3,
		creditMultiplier: .59
	},
	{
		id: "primary-model",
		name: "Primary",
		contextWindow: 272e3,
		maxTokens: 72e3,
		creditMultiplier: 3.31
	},
	{
		id: "deep-model",
		name: "Deep",
		contextWindow: 176e3,
		maxTokens: 24e3,
		creditMultiplier: 3.33
	},
	{
		id: "deepseek-v4.1-flash",
		name: "Deepseek-V4.1-Flash",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: 0
	},
	{
		id: "gpt-6-astra",
		name: "GPT-6-Astra",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: 6.67
	},
	{
		id: "hy4-preview",
		name: "Hy4 preview",
		contextWindow: 1e6,
		maxTokens: 64e3,
		creditMultiplier: 0
	},
	{
		id: "hy3",
		name: "Hy3",
		contextWindow: 192e3,
		maxTokens: 64e3,
		creditMultiplier: 0
	},
	{
		id: "gpt-5.6-sol",
		name: "GPT-5.6-Sol",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: 3.47
	},
	{
		id: "gpt-5.6-terra",
		name: "GPT-5.6-Terra",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: 1.39
	},
	{
		id: "gpt-5.6-luna",
		name: "GPT-5.6-Luna",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: .14
	},
	{
		id: "gpt-5.5",
		name: "GPT-5.5",
		contextWindow: 1e6,
		maxTokens: 128e3,
		creditMultiplier: 3.31
	},
	{
		id: "gpt-5.4",
		name: "GPT-5.4",
		contextWindow: 272e3,
		maxTokens: 72e3,
		creditMultiplier: 1.65
	},
	{
		id: "gpt-5.3-codex",
		name: "GPT-5.3-Codex",
		contextWindow: 272e3,
		maxTokens: 72e3,
		creditMultiplier: 1.25
	},
	{
		id: "gemini-3.5-flash",
		name: "Gemini-3.5-Flash",
		contextWindow: 1e6,
		maxTokens: 65536,
		creditMultiplier: .99
	},
	{
		id: "glm-5.3",
		name: "GLM-5.3",
		contextWindow: 1e6,
		maxTokens: 48e3,
		creditMultiplier: .79
	},
	{
		id: "glm-5.2",
		name: "GLM-5.2",
		contextWindow: 1e6,
		maxTokens: 48e3,
		creditMultiplier: .79
	},
	{
		id: "kimi-k3",
		name: "Kimi-K3",
		contextWindow: 1e6,
		maxTokens: 32e3,
		creditMultiplier: 1.62
	},
	{
		id: "kimi-k2.6",
		name: "Kimi-K2.6",
		contextWindow: 256e3,
		maxTokens: 32e3,
		creditMultiplier: .52
	}
];
/**
* Static fallback directory for a region. Each region keeps its own model
* slot in settings; the fallback must match the region so an account never
* shows the other region's roster.
*/
function fallbackModelsFor(region) {
	return region === "global" ? FALLBACK_WORKBUDDY_MODELS_GLOBAL : FALLBACK_WORKBUDDY_MODELS;
}
/**
* Apply the saved local DSH budget. A budget is an UPPER LIMIT the user sets,
* never a default: a model without an explicit budget keeps its own window, and
* a budget can only ever LOWER a window (`Math.min`), so a value above the
* native one is not an error, just a no-op.
*
* There used to be a `?? 200_000` default here (issue #33), on the rationale
* that a lower advertised window makes DSH compact sooner and so keeps a long
* session under the upstream throttle. That rationale does not hold: the
* measured throttle fires on a SINGLE request past 20k-30k input tokens (the
* CHANGELOG's own wording calls 200K "远超上游 ~20–30k 的节流线"), so clamping to
* 200K bought none of the safety it claimed while costing every >200K model its
* real window — and contradicting the static roster in this very file, which
* declares those models as 1M. A user who wants the conservative behaviour sets
* a budget explicitly; the card offers that per model.
*/
function applyContextBudgets(catalog, budgets = {}) {
	return catalog.map((model) => ({
		...model,
		contextWindow: budgets[model.id] === void 0 ? model.contextWindow : Math.min(model.contextWindow, budgets[model.id])
	}));
}
function deriveCatalog(catalog, enabled, budgets = {}) {
	return applyContextBudgets(enabled.size === 0 ? catalog : catalog.filter((model) => enabled.has(model.id)), budgets);
}
/**
* Mutable catalog shared by the shim's `/v1/models` and the adapter.
*
* The static fallback exists so an OFFLINE upstream never leaves a provider
* empty — but "offline" presumes the region is usable at all. A region with no
* local sign-in cannot serve one single request, so seeding it with a roster
* puts models in the picker that are guaranteed to 401 (issue #12). The host
* distinguishes the two cases through {@link WorkBuddyCatalog.setRegionUsable}.
*/
var WorkBuddyCatalog = class {
	models;
	/**
	* Whether this region has any local sign-in. Defaults to `true` — the
	* permissive direction — so a scan that has not run yet (or failed) keeps
	* serving the fallback rather than blanking a region that may be fine.
	*/
	usable = true;
	/**
	* Whether the user has switched this region's provider ON. Opt-out: defaults
	* to `true`, so a config predating this switch (and the pre-region-split flat
	* fields, which never carry `enabled`) keeps both providers running. Set to
	* `false` by `setRegionEnabled` to withdraw the region from DSH's model
	* picker entirely (its adapter route and configurable-provider entry are
	* pulled by the Host via `AdapterRegistrationHandle.replace([])` /
	* `DirectoryRegistrationHandle.replace([])`). This is a USER choice, distinct
	* from {@link usable}: a region the user switched off keeps its catalog,
	* accounts, and model picks intact and reappears the moment they re-check it.
	*/
	enabled = true;
	/**
	* @param region Seeds the static fallback for this region; each region's
	* provider must never serve the other region's roster before its first
	* live refresh lands.
	*/
	constructor(region = "cn") {
		this.models = fallbackModelsFor(region);
	}
	/**
	* Current entries; the fallback list until the upstream answer lands.
	*
	* Empty while this region has no local sign-in OR the user has switched it
	* off: the provider stays registered (its row still hosts the settings card
	* and the account picker), but it advertises nothing to pick. DSH drops empty
	* provider groups from the picker (`buildModelCatalog` filters
	* `models.length > 0`), so the group disappears exactly when it would be pure
	* noise. The two empty causes are independent: `usable` is the automatic
	* "no account" gate (issue #12), `enabled` is the user's explicit on/off.
	*/
	current() {
		return this.usable && this.enabled ? this.models : [];
	}
	/** Whether this region currently advertises any model. */
	isRegionUsable() {
		return this.usable;
	}
	/** Whether the user has this region's provider switched on. */
	isRegionEnabled() {
		return this.enabled;
	}
	/**
	* Record whether this region has a local sign-in.
	*
	* Separate from {@link set} on purpose: `set()` carries a live upstream
	* answer and must never be empty, while "this region has no account" is a
	* legitimate empty state that must survive across refreshes. Keeping them
	* apart is what lets the non-empty guard stay strict.
	*
	* @returns whether the value changed, so the caller can skip invalidating
	*   its adapter snapshots when nothing moved.
	*/
	setRegionUsable(usable) {
		if (this.usable === usable) return false;
		this.usable = usable;
		return true;
	}
	/**
	* Record the user's explicit on/off choice for this region's provider.
	*
	* Distinct from {@link setRegionUsable}: that is the automatic "no local
	* account" gate, while this is a deliberate user action (issue #11-style
	* region switch). The Host reads the same `enabled` flag through
	* `regionStateOf(config, region).enabled !== false`, so the two halves never
	* disagree. Returns whether the value changed.
	*/
	setRegionEnabled(enabled) {
		if (this.enabled === enabled) return false;
		this.enabled = enabled;
		return true;
	}
	/** Replace the list; callers invalidate their adapter snapshot after this. */
	set(models) {
		if (models.length === 0) throw new Error("workbuddy model catalog cannot be empty");
		this.models = models.map((model) => ({ ...model }));
	}
};
//#endregion
//#region src/pi-ai-runtime.ts
/**
* Which `@earendil-works/pi-ai` copy THIS plugin actually resolves at runtime.
*
* WHY THIS EXISTS (issues #24 / #25 / #26). The plugin declares pi-ai as a
* peer (`>=0.85.0 <0.88.0 || >=1.0.0-0 <2.0.0`), so the module it imports is
* whatever the host environment provides — and the two generations consume
* DIFFERENT context shapes:
*
* - 0.87 hands providers a normalized transcript: `{ messages: [ {role:
*   'system', content, toolsAdded}, … ] }`. The system prompt and the tool
*   declarations live ONLY on that leading system message; the top-level
*   `systemPrompt` / `tools` fields are gone, and 0.87's api never reads them
*   back.
* - 0.85 expects `{ systemPrompt, tools, messages }` and has no branch for a
*   `system` message inside `messages` at all: its token estimator crashes on
*   one (issue #24), and its wire converter silently drops it.
*
* Which copy resolves is NOT predictable from the host version alone: a
* nested 0.85.1 on disk shadows the host's 0.87.1 (the plugin author's own
* symlinked checkout does exactly that), while a clean market install has no
* local copy at all and resolves the host's (issue #26's reporter). The
* adapter therefore gates on what THIS plugin resolves.
*
* HOW it is detected — deliberately WITHOUT importing pi-ai:
* `import.meta.resolve` locates the entry file (executing nothing), the
* package manifest sitting beside it names the version, and the generation is
* decided from that version (0.87+ → modern). A static import would drag
* pi-ai into the standalone `doctor` CLI, which must also run from market
* installs where NO local pi-ai exists and the module is only provided inside
* the DSH host — a static import there is a load-time crash. Resolution
* itself does not need the module: the manifest version and the
* `normalizeContext` feature (present from 0.87 on, not before) agree across
* `0.87+` AND `1.x` — 1.x is 0.87's successor and keeps the same normalized
* transcript, so the split is `>= 0.87` rather than "within 0.x".
*
* When resolution FAILS — the normal state of a market install probed outside
* the host — there is no local copy, so inside DSH the host supplies pi-ai.
* Every DSH host that normalizes contexts (0.2.0+, via 0.87.1) also supplies
* 0.87.1, so the reported generation defaults to `modern` (pass-through).
* That default is safe on older hosts too: they never normalize, and a native
* 0.85-shaped context is an identity under BOTH branches of the adapter gate.
*
* `doctor` prints the resolved version and path so mixed-generation setups —
* the silent kind that produced #24 and #26 — become visible on demand.
*
* @module dsh-connect-workbuddy/pi-ai-runtime
*/
let cached;
/** Resolve (once) which pi-ai copy this plugin runs against. */
function piAiRuntimeInfo() {
	cached ??= piAiRuntimeFor(resolveModuleFile());
	return cached;
}
/**
* The decision table for one resolution result, exported so the branch that
* only ever runs on OTHER machines (a market install with no local pi-ai) is
* pinned by tests rather than by this comment.
*
* `undefined` means nothing local resolved — inside DSH the host supplies the
* module, and every host that normalizes contexts (0.2.0+, via 0.87.1) ships
* the modern generation, so that is the default. Older hosts never normalize,
* and their native 0.85-shaped context is an identity under either gate
* branch, so the default cannot hurt them.
*/
function piAiRuntimeFor(resolvedFrom) {
	if (resolvedFrom === void 0) return {
		generation: "modern",
		resolvedFrom: void 0,
		version: void 0,
		resolvedLocally: false
	};
	const version = readVersion(resolvedFrom);
	return {
		generation: generationOf(version),
		resolvedFrom,
		version,
		resolvedLocally: true
	};
}
/**
* The file the main entry resolved to; undefined when resolution itself fails.
*
* `import.meta.resolve` (not `require.resolve`): pi-ai's exports map exposes
* "." under the `import` condition only, so the CJS resolver refuses the bare
* specifier outright — the ESM resolver is the one this plugin actually
* imports with, so it is also the one whose answer is meaningful here.
*/
function resolveModuleFile() {
	try {
		return fileURLToPath(import.meta.resolve("@earendil-works/pi-ai"));
	} catch {
		return;
	}
}
/**
* The version of the package owning `moduleFile`. pi-ai's exports map does
* not expose `./package.json`, so the manifest is found by walking up from
* the resolved entry — `dist/index.js` sits directly inside the package —
* and claiming it only when its `name` matches, so a same-named ancestor
* (a pnpm virtual-store parent, for instance) cannot be misread.
*/
function readVersion(moduleFile) {
	if (moduleFile === void 0) return void 0;
	let directory = dirname(moduleFile);
	for (let depth = 0; depth < 4; depth++) {
		try {
			const parsed = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
			if (parsed.name === "@earendil-works/pi-ai" && typeof parsed.version === "string") return parsed.version;
		} catch {}
		const parent = dirname(directory);
		if (parent === directory) return void 0;
		directory = parent;
	}
}
/**
* The generation a manifest version belongs to.
*
* `0.87+` consumes normalized transcripts natively; earlier releases want the
* 0.85 shape, so within the declared peer range the split is exactly `0.87+`
* versus earlier. An unreadable version conservatively reports `legacy` (and
* doctor shows it as unreadable, so the guess is visible rather than silent).
*
* The 1.x line MUST land on `modern`, and it is not a hypothetical: DSH
* 0.2.1-alpha.2 ships `@earendil-works/pi-ai@1.0.2`, and DSH 0.2.1-rc.1 ships
* 1.1.0 (which is the version this plugin is now tested against). Both 0.87.1
* and 1.0.2/1.1.0 read tools from the same place
* (`context.messages` → the leading system message's `toolsAdded`; see
* pi-ai's `resolveTranscriptTools`). Matching only `0.<minor>.` classified
* 1.0.2 as `legacy`, so `withLegacyContext` folded the transcript into the
* 0.85 shape — which moves the tool declarations to a top-level `tools` field
* pi-ai 1.x never reads. Every request then went upstream with NO tools
* declared: no model could emit a `toolCall`, and the domestic gateway, which
* tolerates a prompt-less body, had the model write DSML markup into the answer
* text instead. The host's compatibility gate does not catch this: it only
* evaluates peers in the `@deepseek-ai/dsh*` namespace, so an out-of-range
* `@earendil-works/pi-ai` peer is never enforced.
*
* Deliberately match major 1 only. This establishes compatibility for the
* observed 1.x line (including current 1.1.0) without assuming an uninspected
* future major such as 2.x keeps the same transcript contract. The declared
* peer range remains the plugin's supported-version boundary.
*
* ⚠️ Being on `modern` is necessary but NOT sufficient. The modern branch must
* also FOLD a raw seam envelope: `dsh-llm-pi-ai` never normalizes, and 1.x
* reads only `context.messages`. Between 4.4.2 (which taught this function
* about major 1) and the 0.2.1-rc.1 fix, `withLegacyContext` returned the
* modern api UNWRAPPED, so every request on pi-ai 1.x went upstream with no
* prompt and no tool declarations. See `foldModernContext` in `adapter.ts`.
*/
function generationOf(version) {
	if (version === void 0) return "legacy";
	const major = /^(\d+)\./u.exec(version);
	if (major !== null && Number(major[1]) === 1) return "modern";
	const match = /^0\.(\d+)\./u.exec(version);
	if (match === null) return "legacy";
	return Number(match[1]) >= 87 ? "modern" : "legacy";
}
//#endregion
//#region src/version.ts
/** The npm package version this build was produced from. */
const WORKBUDDY_CONNECT_VERSION = "4.12.0";
//#endregion
//#region src/host-heartbeat.ts
/**
* Host-side heartbeat: a small JSON file written under `$DSH_HOME` once the
* `workbuddy` provider is registered. The status CLI reads it to report
* whether the host bundle is alive, independent of the browser card.
*
* 参考：corrinehu/dsh-workbuddy-connect（MIT，Copyright (c) 2026 Corrine Hu）
*   — 该机制由其设计：浏览器端无法写文件，其健康只能靠 console.error 上报，
*     因此由宿主写心跳文件，缺失即代表宿主从未启动；崩溃后的陈旧心跳
*     通过 PID 存活检查识别。
* 改动：无。机制本身已完备，原样沿用。
*
* @module dsh-connect-workbuddy/host-heartbeat
*/
/** Basename of the host heartbeat file inside the Harness home. */
const WORKBUDDY_HOST_HEARTBEAT_FILENAME = ".workbuddy-host-heartbeat.json";
/** Current on-disk heartbeat format; readers reject others. */
const HEARTBEAT_FORMAT_VERSION = 1;
/** Absolute path of the host heartbeat file. */
function workbuddyHostHeartbeatPath() {
	return join(resolveDshHome(), WORKBUDDY_HOST_HEARTBEAT_FILENAME);
}
/**
* Spawn options every external probe in this module is run with.
*
* `windowsHide` is REQUIRED here, not cosmetic. Windows hands a console-program
* child a brand-new console window whenever its PARENT has none — and the
* parent frequently has none: the DSH Desktop host is an Electron GUI process
* (`MainWindowHandle = 0`, verified on the live host), as are Task Scheduler
* jobs and CI services. That window is created VISIBLE, so every start-time
* probe would flash a black box on the user's screen.
*
* Evidence (real Windows 11, parent created console-less through the WMI
* service — the same condition as the Electron host; the child reports the
* console it actually owns):
*
*   - without `windowsHide`: `NEW-console hwnd=1573500 visible=True`
*   - with `windowsHide: true`: `no-console`
*
* Invoked from a terminal the parent HAS a console and the child simply
* inherits it (no new window), which is why this never showed up in local
* runs — the same blind spot `docs/WINDOWS.md` §0 describes. The sibling call
* site `src/at-rest.ts` (the at-rest key fetch) already sets this option; this
* module did not, and that inconsistency is what the test pins.
*
* On POSIX the option is ignored.
*/
const PROCESS_PROBE_OPTIONS = {
	encoding: "utf8",
	windowsHide: true
};
/**
* The command one start-time probe runs, per platform.
*
* Split out as a value so the spawn can be asserted WITHOUT a Windows machine:
* `tests/host-heartbeat.spec.ts` checks the platform branch and the options
* object directly, which is the only way a Windows-only spawn defect here can
* be caught on the macOS development machine (§0: not "unreproduced" but
* "impossible to reproduce").
*
* `platform` is injectable for the same reason, in the same spirit as
* `defaultDesktopAuthDirs()` and `workbuddyAppExecutableCandidates()`.
*/
function processStartProbe(pid, platform = process.platform) {
	if (platform === "win32") return {
		file: "powershell",
		args: [
			"-NoProfile",
			"-NonInteractive",
			"-Command",
			`(Get-Process -Id ${pid} -ErrorAction SilentlyContinue).StartTime.ToUniversalTime().ToString('o')`
		]
	};
	return {
		file: "ps",
		args: [
			"-o",
			"lstart=",
			"-p",
			String(pid)
		]
	};
}
/**
* Process start time in epoch milliseconds; undefined when unavailable.
*
* POSIX reads `ps -o lstart=`; Windows has no such command, so the creation
* time is taken from PowerShell's `Get-Process` StartTime, emitted as UTC ISO
* 8601 so `Date.parse` understands it without locale assumptions. Absent or
* unqueryable processes (other users' processes) degrade to undefined.
*/
function processStartTimeMs(pid) {
	try {
		const probe = processStartProbe(pid);
		const output = execFileSync(probe.file, probe.args, PROCESS_PROBE_OPTIONS);
		const parsed = Date.parse(output.trim());
		return Number.isFinite(parsed) ? parsed : void 0;
	} catch {
		return;
	}
}
/**
* Whether the recorded host process still matches the heartbeat's PID.
*
* A PID can be reused after a crash, so the recorded start time is compared
* against the live process: a different start time means a different process.
*/
function isHeartbeatProcessAlive(heartbeat) {
	if (!Number.isInteger(heartbeat.pid) || heartbeat.pid <= 0) return false;
	try {
		process.kill(heartbeat.pid, 0);
	} catch {
		return false;
	}
	const startedAt = processStartTimeMs(heartbeat.pid);
	if (startedAt === void 0) return true;
	return Math.abs(startedAt - heartbeat.registeredAt) < 6e4;
}
/** Read the heartbeat; absent or unparsable files report undefined. */
async function readHostHeartbeat() {
	try {
		const parsed = JSON.parse(await readFile(workbuddyHostHeartbeatPath(), "utf8"));
		if (typeof parsed !== "object" || parsed === null) return void 0;
		const document = parsed;
		if (document["version"] !== HEARTBEAT_FORMAT_VERSION) return void 0;
		if (document["package"] !== "dsh-connect-workbuddy") return void 0;
		const pid = document["pid"];
		const registeredAt = document["registeredAt"];
		if (typeof pid !== "number" || typeof registeredAt !== "number") return void 0;
		return {
			version: HEARTBEAT_FORMAT_VERSION,
			package: "dsh-connect-workbuddy",
			pluginVersion: typeof document["pluginVersion"] === "string" ? document["pluginVersion"] : WORKBUDDY_CONNECT_VERSION,
			registeredAt,
			pid
		};
	} catch {
		return;
	}
}
/** Write the heartbeat for the current process. */
async function writeHostHeartbeat() {
	const heartbeat = {
		version: HEARTBEAT_FORMAT_VERSION,
		package: "dsh-connect-workbuddy",
		pluginVersion: WORKBUDDY_CONNECT_VERSION,
		registeredAt: Date.now(),
		pid: process.pid
	};
	await writeFile(workbuddyHostHeartbeatPath(), `${JSON.stringify(heartbeat, null, 2)}\n`, { mode: 384 });
}
/** Remove the heartbeat; called when the plugin is disposed. */
async function clearHostHeartbeat() {
	await rm(workbuddyHostHeartbeatPath(), { force: true });
}
//#endregion
export { WORKBUDDY_APP_EXECUTABLE_ENV as $, workbuddyAccountId as A, modelIdOfBody as B, defaultDesktopAuthDirs as C, isVaultAccountId as D, isEncryptedCredentialError as E, WorkBuddyUpstreamClient as F, regionOf as G, parseReasoning as H, classifyUpstreamError as I, lastRefusalFor as J, REFUSES_OFF_MODEL_IDS as K, declaredTools as L, CREDENTIAL_REJECTED_CODE as M, WORKBUDDY_FALLBACK_SYSTEM_PROMPT as N, legacyWorkbuddyOwnAuthPath as O, WorkBuddyCredentialRejectedError as P, withEffectiveOff as Q, isCredentialRejectedError as R, defaultDesktopAuthCandidates as S, hasEncryptedCredentialFields as T, parseUpstreamModel as U, parseCreditMultiplier as V, prepareChatBody as W, offRefused as X, offDefaultFor as Y, recordRefusal as Z, WORKBUDDY_AUTH_FILENAME as _, readHostHeartbeat as a, findWorkbuddyAppExecutableWithSource as at, WorkBuddyEncryptedCredentialError as b, WORKBUDDY_CONNECT_VERSION as c, macosBundleExecutable as ct, FALLBACK_WORKBUDDY_MODELS_GLOBAL as d, readAtRestKey as dt, clearAtRestKeyCache as et, WorkBuddyCatalog as f, workbuddyAppExecutableCandidates as ft, ENCRYPTED_CREDENTIAL_CODE as g, writeFileAtomic as gt, fallbackModelsFor as h, withFileLock as ht, processStartTimeMs as i, findWorkbuddyAppExecutable as it, workbuddyOwnAuthPath as j, parseWorkBuddyAuth as k, piAiRuntimeInfo as l, macosNestedAppBundles as lt, deriveCatalog as m, resolveDshHome as mt, clearHostHeartbeat as n, deriveAtRestKeyId as nt, workbuddyHostHeartbeatPath as o, isEncryptedFieldWrapper as ot, applyContextBudgets as p, hostPeersInfo as pt, defaultOffModelIds as q, isHeartbeatProcessAlive as r, fetchAtRestKeyPayload as rt, writeHostHeartbeat as s, isWorkbuddyBundle as st, WORKBUDDY_HOST_HEARTBEAT_FILENAME as t, deriveAtRestKey as tt, FALLBACK_WORKBUDDY_MODELS as u, openEncryptedField as ut, WORKBUDDY_AUTH_FILE_ENV as v, defaultDesktopAuthPath as w, authFileName as x, WorkBuddyCredentialStore as y, knownRegionOf as z };
