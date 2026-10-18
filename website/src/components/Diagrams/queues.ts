import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The two queue types side by side: where each keeps its messages, what a requeue does to a
 * message, and what ends a message that keeps coming back.
 */
export function queues(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, x: number, y: number, h: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w: 148,
    h,
    kind: 'broker',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {id: 'classic', x: 0, y: 0, w: 172, h: 260, kind: 'group', lines: lines(words, 'classic')},
    {id: 'quorum', x: 188, y: 0, w: 172, h: 260, kind: 'group', lines: lines(words, 'quorum')},
    box('classicStore', 12, 28, 48),
    box('classicRequeue', 12, 100, 62, {kind: 'step'}),
    box('classicReject', 12, 186, 62, {kind: 'bad'}),
    box('quorumStore', 200, 28, 48),
    box('quorumRequeue', 200, 100, 62, {kind: 'step'}),
    box('quorumSpent', 200, 186, 62, {kind: 'bad'}),
    {id: 'limit', x: 0, y: 276, w: 360, h: 44, kind: 'note', lines: lines(words, 'limit')},
  ];
  const links: Link[] = [{from: 'quorumRequeue', to: 'quorumSpent', label: word(words, 'spent')}];
  return {boxes, links};
}
