import test from 'ava'

import { MapAsyncServer, messageToDrop } from '../index.js'

test('start and stop the server', async (t) => {
  const server = new MapAsyncServer(
    async (datum) => {
      const value = datum.value.toString('utf-8')
      console.log(value)
      return [messageToDrop()]
    },
    '/tmp/map.sock',
    '/tmp/map.info',
  )

  setTimeout(() => {
    server.stop()
  }, 1000)
  await server.start()
  t.pass()
})
