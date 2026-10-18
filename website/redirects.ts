// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The guides of the earlier site, each sent to the page that teaches its topic now, and the
// benchmarks, which explain what the architecture costs.
const pages: Record<string, string> = {
  queues: '/components/subscriptions/',
  publishing: '/components/publishing/',
  'request-reply': '/guides/rpc/',
  documenting: '/components/asyncapi/',
  testing: '/guides/testing/',
  benchmarks: '/architecture/benchmarks/',
};

/** Old address to its target, relative to the build's base URL. */
export function redirects(): Record<string, string> {
  return {...pages};
}
