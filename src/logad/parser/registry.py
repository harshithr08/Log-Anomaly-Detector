"""
registry.py — one global ID per template text, shared by every parser worker.

Without it, two workers could number the same template differently. The
lookup-or-create runs as a Lua script inside Redis; Redis runs scripts one at a
time, so two workers can never create two IDs for the same text.
"""

from redis.asyncio import Redis

from logad.common.streams import Keys

# KEYS: text->id hash, id->text hash, counter.  ARGV: template text.
# Returns {id, 1 if newly created else 0}.
GET_OR_CREATE = """
local id = redis.call('HGET', KEYS[1], ARGV[1])
if id then return {tonumber(id), 0} end
id = redis.call('INCR', KEYS[3])
redis.call('HSET', KEYS[1], ARGV[1], id)
redis.call('HSET', KEYS[2], id, ARGV[1])
return {id, 1}
"""


class PatternRegistry:
    def __init__(self, redis: Redis, keys: Keys):
        self.keys = keys
        self._script = redis.register_script(GET_OR_CREATE)
        self._cache: dict[str, int] = {}  # IDs never change, so caching is safe

    async def id_for(self, template: str) -> tuple[int, bool]:
        """(template ID, whether this call created it)."""
        if template in self._cache:
            return self._cache[template], False
        template_id, created = await self._script(
            keys=[self.keys.template_ids, self.keys.template_texts, self.keys.template_counter],
            args=[template],
        )
        self._cache[template] = int(template_id)
        return int(template_id), bool(created)
