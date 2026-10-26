import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry and a drop under the retry steps: the queue type decides which steps it takes. A
 * classic queue counts nothing, so a retry is a plain requeue and only a drop leaves for the
 * dead-letter route; a quorum queue counts its own deliveries and moves a spent one itself.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const half = (id: string, x: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y: 92,
    w: 172,
    h: 62,
    kind: 'core',
    lines: lines(words, id),
    ...fields,
  });
  const leaf = (id: string, x: number, kind: Box['kind']): Box => ({
    id,
    x,
    y: 186,
    w: 82,
    h: 76,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    {id: 'outcome', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    half('classic', 0),
    half('quorum', 188, {kind: 'broker', href: '/architecture/rabbitmq/queues/'}),
    leaf('classicRequeue', 0, 'step'),
    leaf('classicDropped', 90, 'bad'),
    leaf('requeue', 188, 'step'),
    leaf('quorumSpent', 278, 'bad'),
    {id: 'arrives', x: 0, y: 294, w: 270, h: 44, kind: 'broker', lines: lines(words, 'arrives')},
    {id: 'plain', x: 0, y: 362, w: 360, h: 44, kind: 'note', lines: lines(words, 'plain')},
  ];
  const links: Link[] = [
    {from: 'outcome', to: 'classic', exitAt: (86 - 12) / 336},
    {from: 'outcome', to: 'quorum', exitAt: (274 - 12) / 336},
    {from: 'classic', to: 'classicRequeue', exitAt: 41 / 172},
    {from: 'classic', to: 'classicDropped', exitAt: 131 / 172},
    {from: 'quorum', to: 'requeue', exitAt: 41 / 172, label: word(words, 'below')},
    {from: 'quorum', to: 'quorumSpent', exitAt: 131 / 172},
    {from: 'classicRequeue', to: 'arrives', enterAt: 41 / 270},
    {from: 'requeue', to: 'arrives', enterAt: 229 / 270},
  ];
  return {boxes, links};
}
