import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {redirects} from './redirects';

const docsRoot = path.resolve(__dirname, '../docs');

// Targets whose pages land in a later change; the redirect is written now so the old address
// keeps a home. Once the page exists, its entry here goes.
const awaited = new Set<string>();

const moved = redirects();

// The site address of every page: its `slug` when the front matter names one, otherwise its path
// (`a/b.md` and `a/b/index.md` both serve `a/b/`). A page no locale translated is served from the
// English file, so an address with an English page exists in every language.
function addresses(): Set<string> {
  const found = new Set<string>();
  for (const file of fs.readdirSync(docsRoot, {recursive: true, encoding: 'utf8'})) {
    if (!/\.mdx?$/.test(file)) continue;
    const derived = file.replace(/\.mdx?$/, '').replace(/(^|\/)index$/, '');
    const slug = /^---\n[\s\S]*?^slug:\s*(\S+)\s*$[\s\S]*?^---$/m.exec(
      fs.readFileSync(path.join(docsRoot, file), 'utf8'),
    )?.[1];
    const address = slug?.startsWith('/')
      ? slug
      : slug
        ? path.posix.join(path.posix.dirname(derived), slug)
        : derived;
    found.add(address.replace(/^\/|\/$/g, ''));
  }
  return found;
}
const pages = addresses();
const page = (address: string): boolean => pages.has(address.replace(/^\/|\/$/g, ''));

describe('redirects', () => {
  it('sends the benchmarks page of the old site to its home in Architecture', () => {
    expect(moved.benchmarks).toBe('/architecture/benchmarks/');
  });

  it('sends the guides of the old site to the pages that teach the same topic now', () => {
    expect(moved.queues).toBe('/components/subscriptions/');
    expect(moved.publishing).toBe('/components/publishing/');
    expect(moved['request-reply']).toBe('/guides/rpc/');
    expect(moved.documenting).toBe('/components/asyncapi/');
    expect(moved.testing).toBe('/guides/testing/');
  });

  it('gives every page the old site published a target', () => {
    // The pages of the 0.7 line in every language; the version root is a page of the new site.
    const published = ['benchmarks', 'documenting', 'publishing', 'queues', 'request-reply', 'testing'];
    expect(Object.keys(moved).sort()).toEqual(published);
  });

  it('points every address on the site at a page the build has', () => {
    for (const [from, to] of Object.entries(moved)) {
      expect(to.startsWith('/'), `${from} -> ${to}`).toBe(true);
      if (awaited.has(to)) {
        expect(page(to), `${to} exists now: drop it from the awaited targets`).toBe(false);
      } else {
        expect(page(to), `${from} -> ${to}`).toBe(true);
      }
    }
  });

  it('never writes over a page: an old address has no page of its own', () => {
    for (const from of Object.keys(moved)) {
      expect(page(from), from).toBe(false);
    }
  });
});
