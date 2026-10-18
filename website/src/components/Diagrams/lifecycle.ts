import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does
 * on RabbitMQ: the connection at connect, a channel and a consumer per subscription at startup,
 * the channels of the stronger publishers on first use, and the close at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, {h = 58, href}: {h?: number; href?: string} = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const note = (id: string, y: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x: 0,
    y,
    w: 360,
    h: 44,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 414, kind: 'group', lines: lines(words, 'states')},
    {id: 'rabbit', x: 184, y: 0, w: 176, h: 414, kind: 'group', lines: lines(words, 'rabbit')},
    left('unconnected', 28),
    left('connected', 168),
    left('closed', 344),
    right('config', 28),
    right('connection', 98),
    right('subscriptions', 168, {h: 90, href: '/architecture/rabbitmq/consumers/'}),
    right('ownChannels', 270, {h: 62, href: '/architecture/rabbitmq/publishing/'}),
    right('close', 344),
    note('delayCheck', 438, 'bad', '/architecture/settlement/'),
    note('afterShutdown', 494, 'note'),
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'connection'},
    {from: 'connection', to: 'subscriptions'},
    {from: 'subscriptions', to: 'ownChannels', dashed: true},
    {from: 'ownChannels', to: 'close'},
  ];
  return {boxes, links};
}
