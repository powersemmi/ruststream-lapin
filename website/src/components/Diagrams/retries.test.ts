import {expect, it} from 'vitest';
import {delays} from './delays';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const targets = (links: {from: string; to: string}[], id: string): string[] =>
  links.filter((link) => link.from === id).map((link) => link.to);

it('lets the queue type decide who counts a retry', () => {
  const {links} = retries(words);
  expect(targets(links, 'outcome')).toEqual(['classic', 'quorum']);
  // A classic queue counts nothing: a retry is a plain requeue, and a drop leaves for the
  // dead-letter route. A quorum queue counts on the server and moves a spent delivery itself.
  expect(targets(links, 'classic')).toEqual(['classicRequeue', 'classicDropped']);
  expect(targets(links, 'quorum')).toEqual(['requeue', 'quorumSpent']);
  expect(targets(links, 'classicRequeue')).toEqual(['arrives']);
  expect(targets(links, 'requeue')).toEqual(['arrives']);
});

it('draws a requeue without a delay and the two places a delayed retry waits, each returning to the origin queue', () => {
  const {boxes, links} = delays(words);
  const columns = ['immediate', 'waitingQueue', 'pluginExchange'];
  expect(columns.every((id) => boxes.some((box) => box.id === id))).toBe(true);
  expect(targets(links, 'outcome')).toEqual(columns);
  for (const column of columns) {
    expect(targets(links, column), column).toEqual(['origin']);
  }
});
