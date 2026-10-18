import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: the queue pushes to the subscription's channel within its
 * prefetch window, one stream hands the deliveries over one at a time, and `workers(n, by_key)`
 * keys each delivery to a lane by its partition key header.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const row = (id: string, y: number, kind: Box['kind']): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    row('queue', 0, 'broker'),
    row('channel', 72, 'broker'),
    row('stream', 144, 'broker'),
    row('keyed', 216, 'core'),
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 292,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {
      id: 'hash',
      x: 0,
      y: 374,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'hash'),
      href: '/architecture/rabbitmq/routing/',
    },
  ];
  const links: Link[] = [
    {from: 'queue', to: 'channel', label: word(words, 'pushed')},
    {from: 'channel', to: 'stream'},
    {from: 'stream', to: 'keyed', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({from: 'keyed', to: `lane${lane}`, exitAt: (x + 80 - 12) / 336})),
  ];
  return {boxes, links};
}
