import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The three places a delayed retry waits, left to right, and the centre of each column.
const COLUMNS = [
  ['process', 56],
  ['waitingQueue', 180],
  ['pluginExchange', 304],
] as const;

/*
 * A delayed retry waits in one of three places: in the service process by default, in a TTL
 * waiting queue, or in the delayed-message exchange of a plugin. Each sends the copy back to the
 * queue it came from.
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
        kind: id === 'process' ? 'core' : 'broker',
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
      label: id === 'process' ? word(words, 'republished') : word(words, 'released'),
    })),
  ];
  return {boxes, links};
}
