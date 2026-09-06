// ESM entry. The implementation layer (dist/index.js) is CommonJS; this
// shim gives `import { LaminarDB } from '@laminardb/node'` real named ESM
// exports on every Node version, independent of CJS named-export detection.
// __test__/package-surface.spec.mjs asserts this list matches the CJS
// surface, so the two cannot drift.
import laminardb from './dist/index.js'

export const {
  LaminarDB,
  Connection,
  Writer,
  Subscription,
  PushSubscription,
  QueryStream,
  toLaminarError,
  tableFrom,
  LaminarError,
  LaminarConnectionError,
  LaminarSchemaError,
  LaminarIngestionError,
  LaminarQueryError,
  LaminarSubscriptionError,
  LaminarInternalError,
} = laminardb
