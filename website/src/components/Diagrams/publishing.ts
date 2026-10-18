import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The centre of each mode's column, and the width of its steps.
const COLUMNS = {fireAndForget: 60, confirms: 180, transaction: 300} as const;
const STEP = 108;

type Mode = keyof typeof COLUMNS;

/*
 * The three ways a publish can end on RabbitMQ, as columns of frames: fire-and-forget returns
 * when the frame is written, confirms wait for the server's answer per message, and a channel
 * transaction makes its publishes visible at the commit.
 */
export function publishing(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (
    id: string,
    mode: Mode,
    y: number,
    {h = 48, kind = 'broker'}: {h?: number; kind?: Box['kind']} = {},
  ): Box => ({
    id,
    x: COLUMNS[mode] - STEP / 2,
    y,
    w: STEP,
    h,
    kind,
    code: kind === 'broker',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    step('ffPublish', 'fireAndForget', 30),
    step('ffDone', 'fireAndForget', 104, {h: 62, kind: 'good'}),
    step('confirmSelect', 'confirms', 30),
    step('confirmPublish', 'confirms', 104),
    step('confirmAck', 'confirms', 178, {h: 62, kind: 'good'}),
    step('txSelect', 'transaction', 30),
    step('txPublish', 'transaction', 104),
    step('txCommit', 'transaction', 178),
    step('txVisible', 'transaction', 252, {h: 62, kind: 'good'}),
    {
      id: 'unroutable',
      x: 0,
      y: 338,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'unroutable'),
    },
  ];
  const links: Link[] = [
    {from: 'ffPublish', to: 'ffDone'},
    {from: 'confirmSelect', to: 'confirmPublish'},
    {from: 'confirmPublish', to: 'confirmAck'},
    {from: 'txSelect', to: 'txPublish'},
    {from: 'txPublish', to: 'txCommit'},
    {from: 'txCommit', to: 'txVisible'},
  ];
  const lanes: Lane[] = (Object.keys(COLUMNS) as Mode[]).map((mode) => ({
    x: COLUMNS[mode],
    label: word(words, mode),
  }));
  return {boxes, links, lanes};
}
