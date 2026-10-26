import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// What a delayed retry becomes, left to right, and the centre of each column.
const COLUMNS = [
  ['immediate', 56],
  ['waitingQueue', 180],
  ['pluginExchange', 304],
] as const;

/*
 * A delayed retry waits in a TTL waiting queue or in the delayed-message exchange of a plugin, and
 * each sends the copy back to the queue it came from. A queue with neither requeues the delivery
 * at once and drops the delay.
 */
export function delays(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'outcome', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    ...COLUMNS.map(
      ([id, centre]): Box => ({
        id,
        x: centre - 56,
        y: 84,
        w: 112,
        h: 76,
        kind: id === 'immediate' ? 'step' : 'broker',
        lines: lines(words, id),
      }),
    ),
    {id: 'origin', x: 12, y: 200, w: 336, h: 44, kind: 'broker', lines: lines(words, 'origin')},
    {id: 'restart', x: 0, y: 268, w: 360, h: 44, kind: 'note', lines: lines(words, 'restart')},
  ];
  const links: Link[] = [
    ...COLUMNS.map(([id, centre]) => ({from: 'outcome', to: id, exitAt: (centre - 12) / 336})),
    ...COLUMNS.map(([id, centre]) => ({
      from: id,
      to: 'origin',
      enterAt: (centre - 12) / 336,
      label: id === 'immediate' ? word(words, 'requeued') : word(words, 'released'),
    })),
  ];
  return {boxes, links};
}
