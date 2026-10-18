import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The AMQP 0.9.1 model: a producer publishes to an exchange, bindings copy the message into
 * queues, and a consumer reads one queue. The exchange and the queue live in a virtual host; the
 * clients talk over channels that one connection carries.
 */
export function model(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const channel = (id: string, x: number): Box => ({
    id,
    x,
    y: 350,
    w: 104,
    h: 34,
    kind: 'broker',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    box('producer', 0, {kind: 'service'}),
    {id: 'vhost', x: 0, y: 72, w: 360, h: 164, kind: 'group', lines: lines(words, 'vhost')},
    box('exchange', 98, {kind: 'broker', href: '/architecture/rabbitmq/routing/'}),
    box('queue', 176, {kind: 'broker', href: '/architecture/rabbitmq/queues/'}),
    box('consumer', 264, {kind: 'service', href: '/architecture/rabbitmq/consumers/'}),
    {
      id: 'connection',
      x: 0,
      y: 324,
      w: 360,
      h: 72,
      kind: 'group',
      lines: lines(words, 'connection'),
    },
    channel('channel1', 12),
    channel('channel2', 128),
    channel('channelN', 244),
    {
      id: 'channels',
      x: 0,
      y: 412,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'channels'),
    },
  ];
  const links: Link[] = [
    {from: 'producer', to: 'exchange', label: word(words, 'publishes')},
    {from: 'exchange', to: 'queue', label: word(words, 'binds')},
    {from: 'queue', to: 'consumer', label: word(words, 'delivers')},
  ];
  return {boxes, links};
}
