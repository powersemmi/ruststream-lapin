import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {resolveMarkdownLinks, type PageRoots} from './markdownLinks';

let site: string;
let roots: PageRoots;

function write(file: string): void {
  const full = path.join(site, file);
  fs.mkdirSync(path.dirname(full), {recursive: true});
  fs.writeFileSync(full, '# Page\n');
}

// The link check hands the hook the page that holds the link and the link as written.
function resolve(sourceFile: string, url: string): string | undefined {
  const hook = resolveMarkdownLinks(roots);
  const result = hook({url, sourceFilePath: path.join(site, sourceFile)} as Parameters<
    typeof hook
  >[0]);
  return result ?? undefined;
}

beforeAll(() => {
  site = fs.mkdtempSync(path.join(os.tmpdir(), 'markdown-links-'));
  roots = {docs: path.join(site, 'docs'), translations: path.join(site, 'i18n')};
  for (const page of [
    'docs/guides/index.md',
    'docs/guides/testing.md',
    'docs/components/broker.md',
    'docs/architecture/path.md',
    'docs/architecture/benchmarks/index.md',
    'i18n/ru/docusaurus-plugin-content-docs/current/guides/index.md',
    'i18n/zh/docusaurus-plugin-content-docs/current/guides/index.md',
    'i18n/ru/docusaurus-plugin-content-docs/current/architecture/benchmarks/index.md',
  ]) {
    write(page);
  }
});

afterAll(() => {
  fs.rmSync(site, {recursive: true, force: true});
});

describe('resolveMarkdownLinks', () => {
  it('sends a translated page to an English-only neighbour in the same language', () => {
    const source = 'i18n/ru/docusaurus-plugin-content-docs/current/guides/index.md';
    expect(resolve(source, '../components/broker.md')).toBe('/components/broker/');
  });

  it('sends an English-only page to a translated page in the language being built', () => {
    expect(resolve('docs/components/broker.md', '../guides/index.md')).toBe('/guides/');
  });

  it('keeps the anchor of a link from an English-only page to a translated one', () => {
    expect(resolve('docs/architecture/path.md', 'benchmarks/index.md#numbers')).toBe(
      '/architecture/benchmarks/#numbers',
    );
  });

  it('refuses a link to a page that does not exist', () => {
    expect(() => resolve('docs/components/broker.md', '../guides/missing.md')).toThrow(
      "Markdown link '../guides/missing.md' in",
    );
  });

  it('refuses a link from a translated page to a page that does not exist', () => {
    const source = 'i18n/zh/docusaurus-plugin-content-docs/current/guides/index.md';
    expect(() => resolve(source, 'missing.md')).toThrow("Markdown link 'missing.md' in");
  });

  it('refuses a link that leaves the pages', () => {
    write('README.md');
    expect(() => resolve('docs/guides/testing.md', '../../README.md')).toThrow(
      "Markdown link '../../README.md' in",
    );
  });
});
