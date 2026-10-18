import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The exchange types the drawing compares, top to bottom; the last is a plugin's.
export const KINDS = ['direct', 'topic', 'fanout', 'headers', 'hash'] as const;

const TOP = 94;
const ROW = 60;

/*
 * One message offered to five exchanges side by side: each type reads a different part of it
 * (the routing key, a pattern over it, nothing, the headers, a hash of the key) to pick the
 * queues that get a copy.
 */
export function exchanges(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'message', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'message')},
    {
      id: 'exchanges',
      x: 0,
      y: 68,
      w: 360,
      h: TOP + KINDS.length * ROW - 68,
      kind: 'group',
      lines: lines(words, 'exchanges'),
    },
  ];
  KINDS.forEach((kind, row) => {
    const y = TOP + row * ROW;
    boxes.push(
      {
        id: kind,
        x: 12,
        y,
        w: 160,
        h: 48,
        kind: 'broker',
        lines: lines(words, kind),
        // Spreading by a hash is the server-side form of keyed lanes, which that page compares.
        href: kind === 'hash' ? '/architecture/lanes/' : undefined,
      },
      {
        id: `${kind}Queues`,
        x: 188,
        y,
        w: 160,
        h: 48,
        kind: 'good',
        lines: lines(words, `${kind}Queues`),
      },
    );
  });
  const links: Link[] = [
    {from: 'message', to: 'exchanges', label: word(words, 'offered')},
    // The plugin exchange is drawn dashed: it routes only where its plugin is enabled.
    ...KINDS.map((kind) => ({from: kind, to: `${kind}Queues`, dashed: kind === 'hash'})),
  ];
  return {boxes, links};
}
