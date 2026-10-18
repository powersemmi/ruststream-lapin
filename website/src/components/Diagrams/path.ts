import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one delivery that is answered, from the queue to its acknowledgement. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'queue', kind: 'broker'},
    {key: 'deliver', kind: 'broker', href: '/architecture/rabbitmq/consumers/'},
    {key: 'message', kind: 'broker'},
    {key: 'dispatch', kind: 'core', href: '/architecture/lanes/'},
    {key: 'decode'},
    {key: 'body', kind: 'service'},
    {key: 'encode'},
    {key: 'publisher', kind: 'broker', href: '/architecture/rabbitmq/publishing/'},
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
  ]);
}
