import type {Box, Link} from './flow';
import {chain} from './chain';
import {lines, type Words} from './words';

/*
 * A request over direct reply-to: the requester consumes a pseudo-queue on its own channel, the
 * request goes to an ordinary queue, and the reply comes straight back to that channel with no
 * queue in between.
 */
export function replyTo(words: Words): {boxes: Box[]; links: Link[]} {
  const {boxes, links} = chain(words, [
    {key: 'consume', kind: 'broker'},
    {key: 'request', kind: 'service'},
    {key: 'requests', kind: 'broker', href: '/architecture/rabbitmq/consumers/'},
    {key: 'responder', kind: 'service'},
    {key: 'reply'},
    {key: 'back', kind: 'broker'},
    {key: 'resolved', kind: 'good'},
  ]);
  const bottom = Math.max(...boxes.map((box) => box.y + box.h));
  boxes.push({
    id: 'atMostOnce',
    x: 0,
    y: bottom + 24,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'atMostOnce'),
  });
  return {boxes, links};
}
