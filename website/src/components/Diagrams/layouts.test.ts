import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The drawing area `Flow` draws on: a phone shows it at full width.
const WIDTH = 360;

const every = (Object.keys(LAYOUTS) as DiagramName[]).map((name) => ({name}));

describe('a layout', () => {
  it.each(every)('$name keeps every box inside the drawing area', ({name}) => {
    for (const box of LAYOUTS[name](words).boxes) {
      expect(box.x, box.id).toBeGreaterThanOrEqual(0);
      expect(box.y, box.id).toBeGreaterThanOrEqual(0);
      expect(box.x + box.w, box.id).toBeLessThanOrEqual(WIDTH);
    }
  });

  it.each(every)('$name names each box once and links only boxes it draws', ({name}) => {
    const {boxes, links} = LAYOUTS[name](words);
    const ids = boxes.map((box) => box.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const link of links) {
      expect(ids, `${link.from} -> ${link.to}`).toContain(link.from);
      expect(ids, `${link.from} -> ${link.to}`).toContain(link.to);
    }
  });

  it.each(every)('$name gives every box at least one line', ({name}) => {
    for (const box of LAYOUTS[name](words).boxes) {
      expect(box.lines.length, box.id).toBeGreaterThan(0);
    }
  });
});

describe('a timeline', () => {
  it('runs every step of a publishing mode in that mode’s column', () => {
    const {boxes, lanes = []} = LAYOUTS.publishing(words);
    expect(lanes.map((lane) => lane.label)).toEqual(['fireAndForget', 'confirms', 'transaction']);
    const steps = boxes.filter((box) => box.kind !== 'note');
    for (const box of steps) {
      expect(
        lanes.map((lane) => lane.x),
        box.id,
      ).toContain(box.x + box.w / 2);
    }
  });
});
