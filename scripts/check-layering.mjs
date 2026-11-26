// Static architectural dependency DAG verifier enforcing ADR-001 and ADR-003 across all internal packages.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PACKAGES_DIR = join(ROOT_DIR, "packages");

/** Allowed internal package dependencies + external npm imports per package. */
const LAYER_RULES = {
	schema: {
		allowedInternal: new Set(),
		allowedExternal: new Set(),
		allowNodeBuiltins: false,
	},
	telemetry: {
		allowedInternal: new Set(),
		allowedExternal: new Set(),
		allowNodeBuiltins: false,
	},
	ai: {
		allowedInternal: new Set(["schema", "telemetry"]),
		allowedExternal: new Set(["openai", "openai/azure"]),
		allowNodeBuiltins: true,
		allowDynamicImports: true,
	},
	agent: {
		allowedInternal: new Set(["ai", "schema", "telemetry"]),
		allowedExternal: new Set(),
		allowNodeBuiltins: true,
	},
	session: {
		allowedInternal: new Set(["agent", "ai", "schema", "telemetry"]),
		allowedExternal: new Set(),
		allowNodeBuiltins: true,
	},
	"blob-s3": {
		allowedInternal: new Set(["session", "telemetry"]),
		allowedExternal: new Set(),
		allowNodeBuiltins: true,
	},
	postgres: {
		allowedInternal: new Set(["session", "telemetry", "ai"]),
		allowedExternal: new Set(["pg"]),
		allowNodeBuiltins: true,
	},
	cluster: {
		allowedInternal: new Set(["session", "postgres", "agent", "ai", "schema", "telemetry"]),
		allowedExternal: new Set(),
		allowNodeBuiltins: true,
	},
};

function walkTsFiles(dir, out = []) {
	for (const entry of readdirSync(dir)) {
		const full = join(dir, entry);
		const st = statSync(full);
		if (st.isDirectory()) {
			walkTsFiles(full, out);
		} else if (entry.endsWith(".ts")) {
			out.push(full);
		}
	}
	return out;
}

const IMPORT_RE = /(?:import|export)\s+(?:type\s+)?(?:[\s\S]*?\s+from\s+)?["']([^"']+)["']/g;
const DYNAMIC_IMPORT_RE = /\bimport\s*\(/g;

const violations = [];

for (const pkgName of readdirSync(PACKAGES_DIR)) {
	const pkgPath = join(PACKAGES_DIR, pkgName);
	if (!statSync(pkgPath).isDirectory()) continue;

	const rules = LAYER_RULES[pkgName];
	if (!rules) {
		violations.push(`Unregistered package "${pkgName}" in packages/: update scripts/check-layering.mjs & ADR-003.`);
		continue;
	}

	const srcFiles = walkTsFiles(join(pkgPath, "src"));
	for (const file of srcFiles) {
		const relFile = relative(ROOT_DIR, file);
		const source = readFileSync(file, "utf8");

		// Strip single-line and block comments before checking imports
		const codeWithoutComments = source.replace(/\/\*[\s\S]*?\*\/|\/\/.*/g, "");
		if (!rules.allowDynamicImports && DYNAMIC_IMPORT_RE.test(codeWithoutComments)) {
			violations.push(`${relFile}: inline/dynamic import() is forbidden by coding rules.`);
		}

		for (const match of codeWithoutComments.matchAll(IMPORT_RE)) {
			const spec = match[1];
			if (spec.startsWith(".")) {
				const targetAbs = resolve(dirname(file), spec);
				const relToPackages = relative(PACKAGES_DIR, targetAbs);
				const targetPkg = relToPackages.split("/")[0];
				if (targetPkg && targetPkg !== pkgName && !rules.allowedInternal.has(targetPkg)) {
					violations.push(
						`${relFile}: illegal cross-layer import from "${pkgName}" -> "${targetPkg}" (${spec})`,
					);
				}
			} else if (spec.startsWith("node:")) {
				if (!rules.allowNodeBuiltins) {
					violations.push(`${relFile}: node:* builtin "${spec}" is forbidden in zero-host package "${pkgName}"`);
				}
			} else {
				const allowed =
					rules.allowedExternal.has(spec) ||
					[...rules.allowedExternal].some((prefix) => spec.startsWith(`${prefix}/`));
				if (!allowed) {
					violations.push(
						`${relFile}: unauthorized external dependency "${spec}" in package "${pkgName}"`,
					);
				}
			}
		}
	}
}

if (violations.length > 0) {
	console.error("Module Layering Violations Detected:");
	for (const v of violations) {
		console.error(`  - ${v}`);
	}
	process.exit(1);
}

console.log(`Layering check passed across ${Object.keys(LAYER_RULES).length} packages (0 violations).`);
