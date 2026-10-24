import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { defineConfig, type HeadConfig } from 'vitepress';
import { repoUrl, sections, siteDescription, siteTitle, siteUrl } from '../site.data.mjs';

const base = '/relaybus/';
const docsRoot = join(import.meta.dirname, '..');

/** `getting-started.md` -> `getting-started`, `index.md` -> ``, `packages/index.md` -> `packages/`. */
function pagePath(relativePath: string): string {
  const withoutExtension = relativePath.replace(/\.md$/, '');
  if (withoutExtension === 'index') return '';
  return withoutExtension.replace(/index$/, '');
}

/** Builds FAQPage structured data from the question headings of faq.md. */
function faqStructuredData(): Record<string, unknown> {
  const source = readFileSync(join(docsRoot, 'faq.md'), 'utf8');
  const entries = source
    .split('\n## ')
    .slice(1)
    .map((block) => {
      const [question, ...rest] = block.split('\n');
      const answer = rest.join('\n').trim().split('\n\n')[0] ?? '';
      return {
        '@type': 'Question',
        name: question.trim(),
        acceptedAnswer: {
          '@type': 'Answer',
          text: answer.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/`/g, ''),
        },
      };
    });
  return { '@context': 'https://schema.org', '@type': 'FAQPage', mainEntity: entries };
}

const sidebar = sections.map((section) => ({
  text: section.title,
  collapsed: false,
  items: section.items.map((item) => ({
    text: item.title,
    link: `/${item.path}`,
  })),
}));

export default defineConfig({
  title: siteTitle,
  titleTemplate: ':title | relaybus',
  description: siteDescription,
  lang: 'en-US',
  base,
  cleanUrls: true,
  srcExclude: ['snippets/**', 'public/**', 'README.md'],
  sitemap: { hostname: siteUrl },
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}favicon.svg` }],
    ['link', { rel: 'describedby', type: 'text/plain', href: `${siteUrl}llms.txt` }],
    ['meta', { name: 'theme-color', content: '#3451b2' }],
    ['meta', { property: 'og:site_name', content: 'relaybus' }],
    ['meta', { property: 'og:locale', content: 'en_US' }],
    ['meta', { name: 'twitter:card', content: 'summary' }],
    [
      'meta',
      {
        name: 'keywords',
        content:
          'nestjs event bus, transactional outbox, outbox pattern, inbox pattern, reliable events, event driven, domain events, message bus, at-least-once, idempotent consumer, dead letter queue, postgres queue, mongodb outbox, kafka outbox, rabbitmq, nestjs microservices',
      },
    ],
  ],
  transformHead({ pageData }) {
    const path = pagePath(pageData.relativePath);
    const url = `${siteUrl}${path}`;
    const title = (pageData.frontmatter.title as string | undefined) ?? pageData.title;
    const description = pageData.description || siteDescription;
    const isHome = path === '';
    const head: HeadConfig[] = [
      ['link', { rel: 'canonical', href: url }],
      [
        'link',
        {
          rel: 'alternate',
          type: 'text/markdown',
          href: `${siteUrl}${path === '' ? 'index' : path.endsWith('/') ? `${path}index` : path}.md`,
        },
      ],
      ['meta', { property: 'og:type', content: isHome ? 'website' : 'article' }],
      ['meta', { property: 'og:title', content: title }],
      ['meta', { property: 'og:description', content: description }],
      ['meta', { property: 'og:url', content: url }],
      ['meta', { name: 'twitter:title', content: title }],
      ['meta', { name: 'twitter:description', content: description }],
    ];
    let structured: Record<string, unknown>;
    if (isHome) {
      structured = {
        '@context': 'https://schema.org',
        '@type': 'SoftwareSourceCode',
        name: 'relaybus',
        description: siteDescription,
        url: siteUrl,
        codeRepository: repoUrl,
        programmingLanguage: 'TypeScript',
        runtimePlatform: 'Node.js',
        license: 'https://opensource.org/license/mit',
        author: { '@type': 'Person', name: 'Berkcan Şavur' },
      };
    } else if (path === 'faq') {
      structured = faqStructuredData();
    } else {
      structured = {
        '@context': 'https://schema.org',
        '@type': 'TechArticle',
        headline: title,
        description,
        url,
        inLanguage: 'en',
        author: { '@type': 'Person', name: 'Berkcan Şavur' },
        isPartOf: { '@type': 'WebSite', name: 'relaybus', url: siteUrl },
      };
    }
    head.push(['script', { type: 'application/ld+json' }, JSON.stringify(structured)]);
    return head;
  },
  themeConfig: {
    nav: [
      { text: 'Getting started', link: '/getting-started' },
      { text: 'Concepts', link: '/concepts/outbox' },
      { text: 'Packages', link: '/packages/' },
      { text: 'Recipes', link: '/recipes/prisma' },
      { text: 'FAQ', link: '/faq' },
      { text: 'Comparison', link: '/comparison' },
      { text: 'Performance', link: '/performance' },
      { text: 'Upgrading', link: '/upgrading' },
      { text: 'Releases', link: '/releases' },
    ],
    sidebar,
    socialLinks: [
      { icon: 'github', link: repoUrl },
      { icon: 'npm', link: 'https://www.npmjs.com/org/relaybus' },
    ],
    search: { provider: 'local' },
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © Berkcan Şavur',
    },
  },
});
