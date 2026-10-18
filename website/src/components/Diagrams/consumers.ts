import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A queue pushing to two consumers, each with a window of unacknowledged deliveries: an ack
 * frees a slot, and a closed channel hands its unacknowledged deliveries back to the queue.
 */
export function consumers(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, x: number, y: number, h: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w: 160,
    h,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {id: 'queue', x: 12, y: 0, w: 336, h: 44, kind: 'broker', lines: lines(words, 'queue')},
    box('consumer1', 12, 92, 62, {kind: 'service'}),
    box('consumer2', 188, 92, 62, {kind: 'service'}),
    box('ack', 12, 186, 62, {kind: 'good'}),
    box('closed', 188, 186, 76, {kind: 'bad'}),
    {
      id: 'roundRobin',
      x: 0,
      y: 284,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'roundRobin'),
    },
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'queue', to: 'consumer1', exitAt: along(92), label: word(words, 'deliver')},
    {from: 'queue', to: 'consumer2', exitAt: along(268), label: word(words, 'deliver')},
    {from: 'consumer1', to: 'ack'},
    {from: 'consumer2', to: 'closed', dashed: true},
  ];
  return {boxes, links};
}
