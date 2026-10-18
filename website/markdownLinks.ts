import fs from 'node:fs';
import path from 'node:path';
import type {OnBrokenMarkdownLinksFunction} from '@docusaurus/types';

/** Where the English pages and their translations live. */
export interface PageRoots {
  /** The English pages, the source of every page a language has not translated. */
  docs: string;
  /** The `i18n` folder that holds each language's translated pages. */
  translations: string;
}

const siteRoots: PageRoots = {
  docs: path.resolve(__dirname, '../docs'),
  translations: path.resolve(__dirname, 'i18n'),
};
const translationTree = /^(ru|zh)\/docusaurus-plugin-content-docs\/current\/(.*)$/;

/**
 * Resolves the Markdown links Docusaurus cannot.
 *
 * Every page links to its neighbours by relative path to the English file. A language build
 * reads each page from its translation when there is one and from the English file otherwise,
 * and Docusaurus finds a link's target only where that build reads it. So a translated page
 * that links to an English-only one, and an English-only page that links to a translated one,
 * both miss. A link whose English file exists goes to that page's address in the language being
 * built, where the build's link check still verifies it. A link to a file that does not exist,
 * or to one outside the pages, fails the build.
 */
export function resolveMarkdownLinks(
  roots: PageRoots = siteRoots,
): OnBrokenMarkdownLinksFunction {
  return ({url, sourceFilePath}) => {
    const source = path.resolve(__dirname, sourceFilePath);
    const translated = translationTree.exec(path.relative(roots.translations, source));
    const original = translated ? path.join(roots.docs, translated[2]!) : source;
    const [file, hash] = url.split('#');
    const target = path.resolve(path.dirname(original), file!);
    const page = path.relative(roots.docs, target).replace(/\.md$/, '');
    const address = page.replace(/(^|\/)index$/, '');
    const suffix = `${address}${address ? '/' : ''}${hash ? `#${hash}` : ''}`;
    if (!page.startsWith('..') && fs.existsSync(target)) {
      return `/${suffix}`;
    }
    throw new Error(`Markdown link '${url}' in '${sourceFilePath}' does not resolve`);
  };
}
