import {consumers} from './consumers';
import {delays} from './delays';
import {design} from './design';
import {exchanges} from './exchanges';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {model} from './model';
import {path} from './path';
import {publishing} from './publishing';
import {queues} from './queues';
import {replyTo} from './replyTo';
import {retries} from './retries';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  model,
  exchanges,
  consumers,
  queues,
  publishing,
  replyTo,
  design,
  path,
  lifecycle,
  retries,
  delays,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
