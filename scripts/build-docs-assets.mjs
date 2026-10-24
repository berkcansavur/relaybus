#!/usr/bin/env node
// Generates everything in the documentation that is derived from other files, so it never goes stale:
//   - docs-site/packages/<name>.md        from packages/<name>/README.md (not committed)
//   - docs-site/public/<page>.md          Markdown copy of every page for agents (not committed)
//   - docs-site/public/llms.txt, llms-full.txt   (not committed)
//   - llms.txt at the repository root     (committed)
//   - shared snippets (comparison table, when to use) inside README.md, package READMEs and
//     docs-site/comparison.md between `<!-- relaybus:snippet:NAME -->` and `<!-- relaybus:end -->`.
//
// Usage: node scripts/build-docs-assets.mjs [--check]
// With --check nothing is written: the script exits 1 when a committed file is out of date.
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const site = join(root, 'docs-site');
const check = process.argv.includes('--check');
const { packages, pageUrl, repoUrl, sections, siteDescription, siteTitle, siteUrl } = await import(
  join(site, 'site.data.mjs')
);

const stale = [];
/** Content of files this script generates or rewrites, so check mode can read them without writing. */
const generatedFiles = new Map();

/** Writes a file, or in check mode records it as stale when the content differs. */
function emit(file, content, { committed }) {
  if (check) {
    if (!committed) return;
    const current = existsSync(file) ? readFileSync(file, 'utf8') : null;
    if (current !== content) stale.push(file.replace(`${root}/`, ''));
    return;
  }
  mkdirSync(dirname(file), { recursive: true });
  if (!existsSync(file) || readFileSync(file, 'utf8') !== content) writeFileSync(file, content);
}

// 1. Shared snippets inside committed Markdown files.
const snippets = {
  comparison: readFileSync(join(site, 'snippets', 'comparison.md'), 'utf8').trim(),
  'when-to-use': readFileSync(join(site, 'snippets', 'when-to-use.md'), 'utf8').trim(),
};
const snippetPattern = /<!-- relaybus:snippet:([a-z-]+) -->\n[\s\S]*?<!-- relaybus:end -->/g;
const snippetHosts = [
  join(root, 'README.md'),
  join(site, 'comparison.md'),
  ...packages.map((p) => join(root, 'packages', p.name, 'README.md')),
];
for (const file of snippetHosts) {
  const original = readFileSync(file, 'utf8');
  const next = original.replace(snippetPattern, (_match, name) => {
    if (!(name in snippets)) throw new Error(`Unknown snippet "${name}" in ${file}`);
    return `<!-- relaybus:snippet:${name} -->\n\n${snippets[name]}\n\n<!-- relaybus:end -->`;
  });
  generatedFiles.set(file, next);
  emit(file, next, { committed: true });
}

// 2. Package pages from the package READMEs.
const slugs = new Set(packages.map((p) => p.name));
function rewritePackageLinks(markdown) {
  return markdown
    .replace(/\]\(\.\.\/\.\.\/README\.md\)/g, `](${repoUrl}#readme)`)
    .replace(/\]\(\.\.\/([a-z]+)\/?\)/g, (match, name) =>
      slugs.has(name) ? `](/packages/${name})` : match,
    );
}
const packageMeta = new Map();
for (const p of packages) {
  const manifest = JSON.parse(readFileSync(join(root, 'packages', p.name, 'package.json'), 'utf8'));
  packageMeta.set(p.name, manifest);
  const readme = readFileSync(join(root, 'packages', p.name, 'README.md'), 'utf8');
  const title = `@relaybus/${p.name} - ${manifest.description.split(':')[0]}`;
  const front = `---\ntitle: ${JSON.stringify(title)}\ndescription: ${JSON.stringify(manifest.description)}\n---\n\n`;
  const pageFile = join(site, 'packages', `${p.name}.md`);
  const pageContent =
    front +
    rewritePackageLinks(generatedFiles.get(join(root, 'packages', p.name, 'README.md')) ?? readme);
  generatedFiles.set(pageFile, pageContent);
  emit(pageFile, pageContent, { committed: false });
}

// 3. Collect every page in navigation order.
function parseFrontmatter(source) {
  const match = /^---\n([\s\S]*?)\n---\n/.exec(source);
  if (!match) return { data: {}, body: source };
  const data = {};
  for (const line of match[1].split('\n')) {
    const field = /^([a-zA-Z]+):\s*(.*)$/.exec(line);
    if (field) {
      const raw = field[2].trim();
      data[field[1]] = raw.startsWith('"') ? JSON.parse(raw) : raw;
    }
  }
  return { data, body: source.slice(match[0].length) };
}

function stripSnippetMarkers(markdown) {
  return markdown.replace(/<!-- relaybus:(snippet:[a-z-]+|end) -->\n?/g, '');
}

function absolutize(markdown) {
  return markdown.replace(/\]\(\/([^)\s]*)\)/g, (_m, target) => `](${siteUrl}${target})`);
}

const pages = [];
for (const section of sections) {
  for (const item of section.items) {
    const file = item.path.endsWith('/')
      ? join(site, item.path, 'index.md')
      : join(site, `${item.path}.md`);
    const source = generatedFiles.get(file) ?? readFileSync(file, 'utf8');
    const { data, body } = parseFrontmatter(source);
    pages.push({
      section: section.title,
      path: item.path,
      url: pageUrl(item.path),
      title: item.title,
      description: item.description ?? data.description ?? '',
      body: stripSnippetMarkers(body).trim(),
    });
  }
}
const home = parseFrontmatter(readFileSync(join(site, 'index.md'), 'utf8'));
const homeBody = stripSnippetMarkers(home.body).trim();

// 4. Markdown copies next to every page (llms.txt spec: page.md).
function markdownPath(page) {
  return page.path.endsWith('/') ? `${page.path}index.md` : `${page.path}.md`;
}
function markdownUrl(page) {
  return `${siteUrl}${markdownPath(page)}`;
}
function renderPageMarkdown(page) {
  const hasHeading = /^# /.test(page.body);
  const heading = hasHeading ? '' : `# ${page.title}\n\n`;
  return `${heading}${absolutize(page.body)}\n`;
}
for (const page of pages) {
  emit(join(site, 'public', markdownPath(page)), renderPageMarkdown(page), { committed: false });
}
emit(
  join(site, 'public', 'index.md'),
  `# ${siteTitle}\n\n> ${siteDescription}\n\n${absolutize(homeBody)}\n`,
  { committed: false },
);

// 5. llms.txt (index) and llms-full.txt (everything).
const summary =
  'relaybus stores every event in the same database transaction as your business data (the transactional outbox pattern), ' +
  'then delivers it to each handler and broker with independent retries, leases and dead letters. ' +
  'Packages: @relaybus/core, @relaybus/nestjs, @relaybus/postgres, @relaybus/mongodb, @relaybus/kafka, @relaybus/rabbitmq, @relaybus/testing, @relaybus/eslint-plugin. ' +
  'Requires Node.js 22.12 or newer; NestJS 11 or 12 for the NestJS module.';
const header = `# ${siteTitle}\n\n> ${siteDescription}\n\n${summary}\n`;

let index = `${header}\n`;
for (const section of sections) {
  index += `## ${section.title}\n\n`;
  for (const page of pages.filter((p) => p.section === section.title)) {
    const description =
      page.description || packageMeta.get(page.path.replace('packages/', ''))?.description || '';
    index += `- [${page.title}](${markdownUrl(page)})${description ? `: ${description}` : ''}\n`;
  }
  index += '\n';
}
index +=
  '## Optional\n\n' +
  `- [Full documentation in one file](${siteUrl}llms-full.txt): every page above concatenated as Markdown\n` +
  `- [Documentation website](${siteUrl}): the same content as HTML\n` +
  `- [Source repository](${repoUrl}): code, issues and GitHub Releases (the changelog)\n` +
  '- [npm: @relaybus/core](https://www.npmjs.com/package/@relaybus/core): the engine package\n';

let full = `${header}\nThis file contains the complete relaybus documentation. Index: ${siteUrl}llms.txt\n`;
full += `\n---\n\nSource: ${siteUrl}\n\n# ${siteTitle}\n\n${absolutize(homeBody)}\n`;
for (const page of pages) {
  full += `\n---\n\nSource: ${page.url}\n\n${renderPageMarkdown(page)}`;
}

emit(join(root, 'llms.txt'), index, { committed: true });
emit(join(site, 'public', 'llms.txt'), index, { committed: false });
emit(join(site, 'public', 'llms-full.txt'), full, { committed: false });

// 6. Remove generated Markdown copies that no longer correspond to a page (not in check mode).
if (!check) {
  const keep = new Set([...pages.map(markdownPath), 'index.md']);
  const publicDir = join(site, 'public');
  const walk = (dir, prefix = '') => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const relative = prefix + entry.name;
      if (entry.isDirectory()) walk(join(dir, entry.name), `${relative}/`);
      else if (relative.endsWith('.md') && !keep.has(relative)) rmSync(join(dir, entry.name));
    }
  };
  walk(publicDir);
}

if (check && stale.length > 0) {
  console.error(
    'Generated documentation files are out of date. Run: pnpm --filter relaybus-docs docs:assets',
  );
  for (const file of stale) console.error(`  - ${file}`);
  process.exit(1);
}
console.log(
  check
    ? 'Generated documentation files are up to date.'
    : `Generated ${pages.length} pages, llms.txt and llms-full.txt.`,
);
