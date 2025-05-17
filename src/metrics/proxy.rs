use std::sync::Arc;

use super::ConnectionGuard;
use goodmetrics::SumHandle;

use super::{RpcCallGuard, RpcMetrics};

pub trait ProxyMetrics: Clone + Send + Sync + 'static {
    fn begin_connection(&self) -> ConnectionGuard;

    // memcached RPCs
    fn begin_memcached_get(&self) -> RpcCallGuard;
    fn begin_memcached_set(&self) -> RpcCallGuard;
    fn begin_memcached_delete(&self) -> RpcCallGuard;
    fn begin_memcached_unimplemented(&self) -> RpcCallGuard;

    // resp RPCs
    fn begin_resp_del(&self) -> RpcCallGuard;
    fn begin_resp_get(&self) -> RpcCallGuard;
    fn begin_resp_hdel(&self) -> RpcCallGuard;
    fn begin_resp_hexists(&self) -> RpcCallGuard;
    fn begin_resp_hget(&self) -> RpcCallGuard;
    fn begin_resp_hgetall(&self) -> RpcCallGuard;
    fn begin_resp_hincrby(&self) -> RpcCallGuard;
    fn begin_resp_hkeys(&self) -> RpcCallGuard;
    fn begin_resp_hlen(&self) -> RpcCallGuard;
    fn begin_resp_hmget(&self) -> RpcCallGuard;
    fn begin_resp_hset(&self) -> RpcCallGuard;
    fn begin_resp_hvals(&self) -> RpcCallGuard;
    fn begin_resp_lindex(&self) -> RpcCallGuard;
    fn begin_resp_llen(&self) -> RpcCallGuard;
    fn begin_resp_lpop(&self) -> RpcCallGuard;
    fn begin_resp_lrange(&self) -> RpcCallGuard;
    fn begin_resp_lpush(&self) -> RpcCallGuard;
    fn begin_resp_rpush(&self) -> RpcCallGuard;
    fn begin_resp_rpop(&self) -> RpcCallGuard;
    fn begin_resp_set(&self) -> RpcCallGuard;
    fn begin_resp_sadd(&self) -> RpcCallGuard;
    fn begin_resp_srem(&self) -> RpcCallGuard;
    fn begin_resp_sdiff(&self) -> RpcCallGuard;
    fn begin_resp_sunion(&self) -> RpcCallGuard;
    fn begin_resp_sinter(&self) -> RpcCallGuard;
    fn begin_resp_smembers(&self) -> RpcCallGuard;
    fn begin_resp_sismember(&self) -> RpcCallGuard;
    fn begin_resp_zcard(&self) -> RpcCallGuard;
    fn begin_resp_zincrby(&self) -> RpcCallGuard;
    fn begin_resp_zscore(&self) -> RpcCallGuard;
    fn begin_resp_zmscore(&self) -> RpcCallGuard;
    fn begin_resp_zrem(&self) -> RpcCallGuard;
    fn begin_resp_zrank(&self) -> RpcCallGuard;
    fn begin_resp_zrange(&self) -> RpcCallGuard;
    fn begin_resp_zadd(&self) -> RpcCallGuard;
    fn begin_resp_zrevrank(&self) -> RpcCallGuard;
    fn begin_resp_zcount(&self) -> RpcCallGuard;
    fn begin_resp_zunionstore(&self) -> RpcCallGuard;
    fn begin_resp_unimplemented(&self) -> RpcCallGuard;
}

#[derive(Clone, Debug)]
pub struct DefaultProxyMetrics {
    pub(crate) connections_opened: SumHandle,
    pub(crate) connections_closed: SumHandle,

    // memcached RPCs
    pub(crate) memcached_get: RpcMetrics,
    pub(crate) memcached_set: RpcMetrics,
    pub(crate) memcached_delete: RpcMetrics,
    pub(crate) memcached_unimplemented: RpcMetrics,

    // resp RPCs
    pub(crate) resp_del: RpcMetrics,
    pub(crate) resp_get: RpcMetrics,
    pub(crate) resp_hdel: RpcMetrics,
    pub(crate) resp_hexists: RpcMetrics,
    pub(crate) resp_hget: RpcMetrics,
    pub(crate) resp_hgetall: RpcMetrics,
    pub(crate) resp_hincrby: RpcMetrics,
    pub(crate) resp_hkeys: RpcMetrics,
    pub(crate) resp_hlen: RpcMetrics,
    pub(crate) resp_hmget: RpcMetrics,
    pub(crate) resp_hset: RpcMetrics,
    pub(crate) resp_hvals: RpcMetrics,
    pub(crate) resp_lindex: RpcMetrics,
    pub(crate) resp_llen: RpcMetrics,
    pub(crate) resp_lpop: RpcMetrics,
    pub(crate) resp_lrange: RpcMetrics,
    pub(crate) resp_lpush: RpcMetrics,
    pub(crate) resp_rpush: RpcMetrics,
    pub(crate) resp_rpop: RpcMetrics,
    pub(crate) resp_set: RpcMetrics,
    pub(crate) resp_sadd: RpcMetrics,
    pub(crate) resp_srem: RpcMetrics,
    pub(crate) resp_sdiff: RpcMetrics,
    pub(crate) resp_sunion: RpcMetrics,
    pub(crate) resp_sinter: RpcMetrics,
    pub(crate) resp_smembers: RpcMetrics,
    pub(crate) resp_sismember: RpcMetrics,
    pub(crate) resp_zcard: RpcMetrics,
    pub(crate) resp_zincrby: RpcMetrics,
    pub(crate) resp_zscore: RpcMetrics,
    pub(crate) resp_zmscore: RpcMetrics,
    pub(crate) resp_zrem: RpcMetrics,
    pub(crate) resp_zrank: RpcMetrics,
    pub(crate) resp_zrange: RpcMetrics,
    pub(crate) resp_zadd: RpcMetrics,
    pub(crate) resp_zrevrank: RpcMetrics,
    pub(crate) resp_zcount: RpcMetrics,
    pub(crate) resp_zunionstore: RpcMetrics,
    pub(crate) resp_unimplemented: RpcMetrics,
}

impl ProxyMetrics for DefaultProxyMetrics {
    fn begin_connection(&self) -> ConnectionGuard {
        ConnectionGuard::new(
            self.connections_opened.clone(),
            self.connections_closed.clone(),
        )
    }

    // memcached RPCs
    fn begin_memcached_get(&self) -> RpcCallGuard {
        self.memcached_get.record_api_call()
    }

    fn begin_memcached_set(&self) -> RpcCallGuard {
        self.memcached_set.record_api_call()
    }

    fn begin_memcached_delete(&self) -> RpcCallGuard {
        self.memcached_delete.record_api_call()
    }

    fn begin_memcached_unimplemented(&self) -> RpcCallGuard {
        self.memcached_unimplemented.record_api_call()
    }

    // resp RPCs
    fn begin_resp_del(&self) -> RpcCallGuard {
        self.resp_del.record_api_call()
    }
    fn begin_resp_get(&self) -> RpcCallGuard {
        self.resp_get.record_api_call()
    }
    fn begin_resp_hdel(&self) -> RpcCallGuard {
        self.resp_hdel.record_api_call()
    }
    fn begin_resp_hexists(&self) -> RpcCallGuard {
        self.resp_hexists.record_api_call()
    }
    fn begin_resp_hget(&self) -> RpcCallGuard {
        self.resp_hget.record_api_call()
    }
    fn begin_resp_hgetall(&self) -> RpcCallGuard {
        self.resp_hgetall.record_api_call()
    }
    fn begin_resp_hincrby(&self) -> RpcCallGuard {
        self.resp_hincrby.record_api_call()
    }
    fn begin_resp_hkeys(&self) -> RpcCallGuard {
        self.resp_hkeys.record_api_call()
    }
    fn begin_resp_hlen(&self) -> RpcCallGuard {
        self.resp_hlen.record_api_call()
    }
    fn begin_resp_hmget(&self) -> RpcCallGuard {
        self.resp_hmget.record_api_call()
    }
    fn begin_resp_hset(&self) -> RpcCallGuard {
        self.resp_hset.record_api_call()
    }
    fn begin_resp_hvals(&self) -> RpcCallGuard {
        self.resp_hvals.record_api_call()
    }
    fn begin_resp_lindex(&self) -> RpcCallGuard {
        self.resp_lindex.record_api_call()
    }
    fn begin_resp_llen(&self) -> RpcCallGuard {
        self.resp_llen.record_api_call()
    }
    fn begin_resp_lpop(&self) -> RpcCallGuard {
        self.resp_lpop.record_api_call()
    }
    fn begin_resp_lrange(&self) -> RpcCallGuard {
        self.resp_lrange.record_api_call()
    }
    fn begin_resp_lpush(&self) -> RpcCallGuard {
        self.resp_lpush.record_api_call()
    }
    fn begin_resp_rpush(&self) -> RpcCallGuard {
        self.resp_rpush.record_api_call()
    }
    fn begin_resp_rpop(&self) -> RpcCallGuard {
        self.resp_rpop.record_api_call()
    }
    fn begin_resp_set(&self) -> RpcCallGuard {
        self.resp_set.record_api_call()
    }
    fn begin_resp_sadd(&self) -> RpcCallGuard {
        self.resp_sadd.record_api_call()
    }
    fn begin_resp_srem(&self) -> RpcCallGuard {
        self.resp_srem.record_api_call()
    }
    fn begin_resp_sdiff(&self) -> RpcCallGuard {
        self.resp_sdiff.record_api_call()
    }
    fn begin_resp_sunion(&self) -> RpcCallGuard {
        self.resp_sunion.record_api_call()
    }
    fn begin_resp_sinter(&self) -> RpcCallGuard {
        self.resp_sinter.record_api_call()
    }
    fn begin_resp_smembers(&self) -> RpcCallGuard {
        self.resp_smembers.record_api_call()
    }
    fn begin_resp_sismember(&self) -> RpcCallGuard {
        self.resp_sismember.record_api_call()
    }
    fn begin_resp_zcard(&self) -> RpcCallGuard {
        self.resp_zcard.record_api_call()
    }
    fn begin_resp_zincrby(&self) -> RpcCallGuard {
        self.resp_zincrby.record_api_call()
    }
    fn begin_resp_zscore(&self) -> RpcCallGuard {
        self.resp_zscore.record_api_call()
    }
    fn begin_resp_zmscore(&self) -> RpcCallGuard {
        self.resp_zmscore.record_api_call()
    }
    fn begin_resp_zrem(&self) -> RpcCallGuard {
        self.resp_zrem.record_api_call()
    }
    fn begin_resp_zrank(&self) -> RpcCallGuard {
        self.resp_zrank.record_api_call()
    }
    fn begin_resp_zrange(&self) -> RpcCallGuard {
        self.resp_zrange.record_api_call()
    }
    fn begin_resp_zadd(&self) -> RpcCallGuard {
        self.resp_zadd.record_api_call()
    }
    fn begin_resp_zrevrank(&self) -> RpcCallGuard {
        self.resp_zrevrank.record_api_call()
    }
    fn begin_resp_zcount(&self) -> RpcCallGuard {
        self.resp_zcount.record_api_call()
    }
    fn begin_resp_zunionstore(&self) -> RpcCallGuard {
        self.resp_zunionstore.record_api_call()
    }
    fn begin_resp_unimplemented(&self) -> RpcCallGuard {
        self.resp_unimplemented.record_api_call()
    }
}

impl ProxyMetrics for Arc<DefaultProxyMetrics> {
    fn begin_connection(&self) -> ConnectionGuard {
        self.as_ref().begin_connection()
    }

    // memcached RPCs
    fn begin_memcached_get(&self) -> RpcCallGuard {
        self.as_ref().begin_memcached_get()
    }

    fn begin_memcached_set(&self) -> RpcCallGuard {
        self.as_ref().begin_memcached_set()
    }

    fn begin_memcached_delete(&self) -> RpcCallGuard {
        self.as_ref().begin_memcached_delete()
    }

    fn begin_memcached_unimplemented(&self) -> RpcCallGuard {
        self.as_ref().begin_memcached_unimplemented()
    }

    // resp RPCs
    fn begin_resp_del(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_del()
    }
    fn begin_resp_get(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_get()
    }
    fn begin_resp_hdel(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hdel()
    }
    fn begin_resp_hexists(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hexists()
    }
    fn begin_resp_hget(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hget()
    }
    fn begin_resp_hgetall(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hgetall()
    }
    fn begin_resp_hincrby(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hincrby()
    }
    fn begin_resp_hkeys(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hkeys()
    }
    fn begin_resp_hlen(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hlen()
    }
    fn begin_resp_hmget(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hmget()
    }
    fn begin_resp_hset(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hset()
    }
    fn begin_resp_hvals(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_hvals()
    }
    fn begin_resp_lindex(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_lindex()
    }
    fn begin_resp_llen(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_llen()
    }
    fn begin_resp_lpop(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_lpop()
    }
    fn begin_resp_lrange(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_lrange()
    }
    fn begin_resp_lpush(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_lpush()
    }
    fn begin_resp_rpush(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_rpush()
    }
    fn begin_resp_rpop(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_rpop()
    }
    fn begin_resp_set(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_set()
    }
    fn begin_resp_sadd(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_sadd()
    }
    fn begin_resp_srem(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_srem()
    }
    fn begin_resp_sdiff(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_sdiff()
    }
    fn begin_resp_sunion(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_sunion()
    }
    fn begin_resp_sinter(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_sinter()
    }
    fn begin_resp_smembers(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_smembers()
    }
    fn begin_resp_sismember(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_sismember()
    }
    fn begin_resp_zcard(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zcard()
    }
    fn begin_resp_zincrby(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zincrby()
    }
    fn begin_resp_zscore(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zscore()
    }
    fn begin_resp_zmscore(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zmscore()
    }
    fn begin_resp_zrem(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zrem()
    }
    fn begin_resp_zrank(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zrank()
    }
    fn begin_resp_zrange(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zrange()
    }
    fn begin_resp_zadd(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zadd()
    }
    fn begin_resp_zrevrank(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zrevrank()
    }
    fn begin_resp_zcount(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zcount()
    }
    fn begin_resp_zunionstore(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_zunionstore()
    }
    fn begin_resp_unimplemented(&self) -> RpcCallGuard {
        self.as_ref().begin_resp_unimplemented()
    }
}
