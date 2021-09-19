import ../redisclient, asyncdispatch, times, locks
from os import sleep
from net import Port

## A fork of zedeus's redpool for redisclient
## https://github.com/zedeus/redpool

type
  RedisConn[T: Redis|AsyncRedis] = ref object
    taken: bool
    conn: T

when compileOption("threads"):
  type RedisPool*[T: Redis|AsyncRedis] = ref object
    conns: ptr UncheckedArray[RedisConn[T]]
    host: string
    port: Port
    db: int
    maxConns: int
    noConnectivityTest: bool
    isBlocking: bool
    waitTimeout: float
    lock: Lock
else:
  type RedisPool*[T: Redis|AsyncRedis] = ref object
    conns: seq[RedisConn[T]]
    host: string
    port: Port
    db: int
    maxConns: int
    noConnectivityTest: bool
    isBlocking: bool
    waitTimeout: float
    lock: Lock


const
  WAIT_TIME_IN_MILISECONDS = 10

proc newAsyncRedisConn(pool: RedisPool[AsyncRedis]): Future[RedisConn[AsyncRedis]] {.async.} =
  result = RedisConn[AsyncRedis](
    conn: await openAsync(pool.host, pool.port)
  )
  discard await result.conn.select(pool.db)

proc newRedisConn(pool: RedisPool[Redis]): RedisConn[Redis] =
  result = RedisConn[Redis](
    conn: open(pool.host, pool.port)
  )
  discard result.conn.select(pool.db)

template newPoolImpl(x) =
  result = RedisPool[x](
    host: host,
    port: Port(port),
    db: db,
    maxConns: maxConns,
    noConnectivityTest: noConnectivityTest,
    isBlocking: isBlocking,
    waitTimeout: waitTimeout
  )
  when compileOption("threads"):
    result.conns = cast[ptr UncheckedArray[RedisConn[x]]](allocShared0(sizeof(RedisConn[x]) * maxConns))
  else:
    result.conns = newSeq[RedisConn[x]](maxConns)

  let initSize = if initSize > maxConns: maxConns else: initSize
  for i in 0 ..< initSize:
    when x is Redis:
      var conn = newRedisConn(result)
    else:
      var conn = await newAsyncRedisConn(result)
    result.conns[i] = conn

proc newAsyncRedisPool*(initSize: int, maxConns=10, host="localhost", port=6379, db=0, noConnectivityTest=false, isBlocking=true, waitTimeout=10.0): Future[RedisPool[AsyncRedis]] {.async.} =
  ## Create new AsyncRedis pool
  newPoolImpl(AsyncRedis)

proc newRedisPool*(initSize: int, maxConns=10, host="localhost", port=6379, db=0, noConnectivityTest=false, isBlocking=true, waitTimeout=10.0): RedisPool[Redis] =
  ## Create new Redis pool
  newPoolImpl(Redis)
  initLock(result.lock)

template acquireImpl(x) =
  let now = epochTime()
  var freeSlotIndex = -1
  while true:
    for i in 0..<pool.maxConns:
      if unlikely(pool.conns[i] == nil):
        freeSlotIndex = i
      elif not pool.conns[i].taken:
        # FIXME: Why does noConnectivityTest make it slower
        if unlikely(not pool.noConnectivityTest):
          # Try to send PING to server to test for connection
          try:
            when x is Redis:
              let val = pool.conns[i].conn.ping()
            else:
              let val = await pool.conns[i].conn.ping()
            if val.getStr() != "PONG":
              raiseError(InvalidReply, "PONG expected.")
          except:
            pool.conns[i] = nil
            break
        pool.conns[i].taken = true
        result = pool.conns[i].conn
        break
    if result != nil:
      # break `while true` loop
      break

    # All connections are busy, and no more slot to make new connection
    if unlikely(freeSlotIndex < 0):
      # If `isBlocking` is set, wait for a connection till waitTimeout exceed
      if pool.isBlocking and epochTime() - now < pool.waitTimeout:
        when x is Redis:
          sleep(WAIT_TIME_IN_MILISECONDS)
        else:
          await sleepAsync(WAIT_TIME_IN_MILISECONDS)
        continue
      # Raise exception if not blocking, or blocking timed out
      raiseError(ConnectionError, "No connection available.")
    else:
      # Still having free slot, making new connection
      when x is Redis:
        let newConn = newRedisConn(pool)
      else:
        let newConn = await newAsyncRedisConn(pool)
      newConn.taken = true
      pool.conns[freeSlotIndex] = newConn
      result = newConn.conn
      break

proc acquire*(pool: RedisPool[AsyncRedis]): Future[AsyncRedis] {.async.} =
  ## Acquires AsyncRedis connection from pool
  acquireImpl(AsyncRedis)

proc acquire*(pool: RedisPool[Redis]): Redis =
  ## Acquires Redis connection from pool
  pool.lock.acquire()
  defer: pool.lock.release()
  acquireImpl(Redis)

proc release*[T: Redis|AsyncRedis](pool: RedisPool[T], conn: T) =
  ## Returns connection to pool
  for i in 0..<pool.maxConns:
    if pool.conns[i] != nil and pool.conns[i].conn == conn:
      pool.conns[i].taken = false
      break

proc close(pool: RedisPool[AsyncRedis]) {.async.} =
  ## Close all connections
  for i in 0..<pool.maxConns:
    if pool.conns[i] != nil:
      pool.conns[i].taken = false
      discard await pool.conns[i].conn.quit()
      pool.conns[i] = nil
  when compileOption("threads"):
    deallocShared(cast[pointer](pool.conns))

proc close(pool: RedisPool[Redis]) =
  ## Close all connections
  for i in 0..<pool.maxConns:
    if pool.conns[i] != nil:
      pool.conns[i].taken = false
      discard pool.conns[i].conn.quit()
      pool.conns[i] = nil
  deinitLock(pool.lock)
  when compileOption("threads"):
    deallocShared(cast[pointer](pool.conns))

template withAcquire*[T: Redis|AsyncRedis](pool: RedisPool[T], conn, body: untyped) =
  ## Automatically acquire and release a connection
  when T is Redis:
    let `conn` {.inject.} = pool.acquire()
  else:
    let `conn` {.inject.} = await pool.acquire()
  try:
    body
  finally:
    pool.release(`conn`)


when isMainModule:
  proc main {.async.} =
    let pool = await newAsyncRedisPool(1)
    let conn = await pool.acquire()
    echo (await conn.ping()).getStr()
    pool.release(conn)

    pool.withAcquire(conn2):
      echo (await conn2.ping()).getStr()
    await pool.close()

  proc sync =
    let pool = newRedisPool(1)
    let conn = pool.acquire()
    echo conn.ping().getStr()
    pool.release(conn)

    pool.withAcquire(conn2):
      echo conn2.ping().getStr()
    pool.close()

  proc timeout =
    let pool = newRedisPool(3, maxConns=5, waitTimeout=1)
    for i in 0..6:
      let conn = pool.acquire()
      echo conn.ping().getStr()
    pool.close()

  proc closed =
    # test for connection closed befor acquire
    # restart redis server for example
    let pool = newRedisPool(3, maxConns=5, noConnectivityTest=true)
    echo "Restart Redis server, and press Enter to continue.."
    discard stdin.readLine
    for i in 0..5:
      pool.withAcquire(conn):
        echo conn.ping().getStr()
    pool.close()

  waitFor main()
  sync()
  closed()
  timeout()





