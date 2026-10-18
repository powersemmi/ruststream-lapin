import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the lapin client, and the RabbitMQ node at the bottom. One connection carries the
 * service; each subscription consumes on a channel of its own, and the publishers share one.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the channels
  // under it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 218, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/rabbitmq/publishing/',
    }),
    box('broker', left, 294, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/lifecycle/',
    }),
    box('descriptors', right, 294, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/rabbitmq/queues/',
    }),
    box('requester', right, 362, column, 44, {
      kind: 'broker',
      code: true,
      href: '/architecture/rabbitmq/reply-to/',
    }),
    box('client', 0, 442, 360, 154, {kind: 'group'}),
    box('connection', 12, 468, 336, 44, {kind: 'broker', href: '/architecture/rabbitmq/'}),
    box('subChannels', left, 522, column, 62, {kind: 'broker'}),
    box('pubChannels', right, 522, column, 62, {kind: 'broker'}),
    box('rabbitmq', 12, 620, 336, 36, {kind: 'broker'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'subChannels', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'pubChannels', exit: 'right', enter: 'right', enterAt: 0.35},
    {from: 'requester', to: 'pubChannels', exit: 'right', enter: 'right', enterAt: 0.65},
    {from: 'subChannels', to: 'rabbitmq', enterAt: centre(left)},
    {from: 'pubChannels', to: 'rabbitmq', enterAt: centre(right)},
  ];
  return {boxes, links};
}
