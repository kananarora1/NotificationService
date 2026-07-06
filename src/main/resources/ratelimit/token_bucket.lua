-- Atomic per-tenant token-bucket rate limiter.
--
-- Runs entirely inside Redis via EVAL, so the read → refill → check → decrement is a
-- single atomic step. This is what makes it correct across concurrent requests from
-- multiple app instances: a GET-then-SET from Java would have a TOCTOU race where two
-- instances both read "1 token left" and both decrement.
--
-- KEYS[1]           bucket key, e.g. ratelimit:{tenantId}
-- ARGV[1] capacity    max tokens (== rateLimitPerMinute)
-- ARGV[2] refillPerMs tokens replenished per millisecond (capacity / 60000)
-- ARGV[3] now         caller's current time in epoch millis
-- ARGV[4] requested   tokens to consume (normally 1)
-- ARGV[5] ttlMs       idle expiry for the key
-- Returns { allowed (1|0), retryAfterMs }

local capacity    = tonumber(ARGV[1])
local refillPerMs = tonumber(ARGV[2])
local now         = tonumber(ARGV[3])
local requested   = tonumber(ARGV[4])
local ttlMs       = tonumber(ARGV[5])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil then
  tokens = capacity   -- new bucket starts full
  ts = now
end

-- Continuous refill based on elapsed time, capped at capacity.
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * refillPerMs)
  ts = now
end

local allowed = 0
local retryAfterMs = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retryAfterMs = math.ceil((requested - tokens) / refillPerMs)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
redis.call('PEXPIRE', KEYS[1], ttlMs)

return { allowed, retryAfterMs }
