import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> =>
  Object.fromEntries(
    LAYOUTS[name](words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  const every = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
    Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
  );

  it.each(every)('$name, $box: $href is a page of the site', ({href}) => {
    // A root-relative route: the link prepends the language's base URL, so one route serves
    // every translation, and the site ends every route with a slash.
    expect(href).toMatch(/^\/[a-z0-9/-]+\/$/);
    expect(page(href), `no page under docs/ for ${href}`).toBe(true);
  });
});

describe('a box that names a part with a page of its own', () => {
  it('links to that page in the RabbitMQ model', () => {
    expect(hrefs('model')).toEqual({
      exchange: '/architecture/rabbitmq/routing/',
      queue: '/architecture/rabbitmq/queues/',
      consumer: '/architecture/rabbitmq/consumers/',
    });
  });

  it('links to that page in the design of the crate', () => {
    expect(hrefs('design')).toEqual({
      broker: '/architecture/lifecycle/',
      descriptors: '/architecture/rabbitmq/queues/',
      subscriber: '/architecture/path/',
      publishers: '/architecture/rabbitmq/publishing/',
      requester: '/architecture/rabbitmq/reply-to/',
      connection: '/architecture/rabbitmq/',
    });
  });

  it('links to that page on the path of a delivery', () => {
    expect(hrefs('path')).toEqual({
      deliver: '/architecture/rabbitmq/consumers/',
      dispatch: '/architecture/lanes/',
      publisher: '/architecture/rabbitmq/publishing/',
      settle: '/architecture/settlement/',
    });
  });

  it('links to that page in the lifecycle', () => {
    expect(hrefs('lifecycle')).toEqual({
      subscriptions: '/architecture/rabbitmq/consumers/',
      delayCheck: '/architecture/settlement/',
      ownChannels: '/architecture/rabbitmq/publishing/',
    });
  });

  it('links the plugin exchange to the lanes it stands in for', () => {
    expect(hrefs('exchanges')).toEqual({hash: '/architecture/lanes/'});
  });

  it('links to that page in the retries, the lanes and direct reply-to', () => {
    expect(hrefs('retries')).toEqual({quorum: '/architecture/rabbitmq/queues/'});
    expect(hrefs('lanes')).toEqual({hash: '/architecture/rabbitmq/routing/'});
    expect(hrefs('replyTo')).toEqual({requests: '/architecture/rabbitmq/consumers/'});
  });
});
