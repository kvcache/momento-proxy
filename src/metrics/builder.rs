use goodmetrics::{
    default_gauge_factory,
    downstream::{get_client, OpenTelemetryDownstream, OpentelemetryBatcher},
    pipeline::DimensionPosition,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_rustls::rustls::RootCertStore;
use tonic::metadata::MetadataValue;

use super::{proxy::DefaultProxyMetrics, util::proxy_sum_gauge, RpcMetrics};

pub struct ProxyMetricsBuilder {
    batch_interval: Duration,
    batch_capacity: usize,
}

impl ProxyMetricsBuilder {
    pub fn new() -> Self {
        Self {
            batch_interval: Duration::from_secs(1),
            batch_capacity: 128,
        }
    }

    pub async fn build(self) -> Arc<DefaultProxyMetrics> {
        let (batch_sender, batch_receiver) = mpsc::channel(self.batch_capacity);
        let gauge_factory = default_gauge_factory();

        let endpoint = get_environment_variable_or_none("OTLP_ENDPOINT");
        let api_token = get_environment_variable_or_none("OTLP_API_TOKEN");

        match (&endpoint, &api_token) {
            (Some(endpoint), Some(api_token)) => {
                info!("Configuring OTLP downstream with provided endpoint and API token");

                // Set up the OTLP downstream
                let channel = get_client(
                    endpoint,
                    || {
                        Some(RootCertStore {
                            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec()
                        })
                    },
                    goodmetrics::proto::opentelemetry::collector::metrics::v1::metrics_service_client::MetricsServiceClient::with_origin
                ).expect("Failed to create client");

                // Set up the OTLP downstream
                let otlp_downstream = OpenTelemetryDownstream::new_with_dimensions(
                    channel,
                    Some(("api-token", MetadataValue::try_from(api_token).unwrap())),
                    get_base_environment_dimensions(),
                );
                tokio::spawn(otlp_downstream.send_batches_forever(batch_receiver));

                // Set up the OpenTelemetry batcher
                tokio::spawn(gauge_factory.clone().report_gauges_forever(
                    self.batch_interval,
                    batch_sender,
                    OpentelemetryBatcher,
                ));
            }
            (None, _) => {
                info!("OTLP endpoint not provided: not configuring OTLP downstream. Set the OTLP_ENDPOINT environment variable to configure.");
            }
            (_, None) => {
                info!("OTLP API token not provided, not configuring OTLP downstream. Set the OTLP_API_TOKEN environment variable to configure.");
            }
        }

        let metrics = DefaultProxyMetrics {
            connections_opened: proxy_sum_gauge(gauge_factory, "connections_opened"),
            connections_closed: proxy_sum_gauge(gauge_factory, "connections_closed"),
            memcached_get: RpcMetrics::new(gauge_factory, "memcached_get"),
            memcached_set: RpcMetrics::new(gauge_factory, "memcached_set"),
            memcached_delete: RpcMetrics::new(gauge_factory, "memcached_delete"),
            memcached_unimplemented: RpcMetrics::new(gauge_factory, "memcached_unimplemented"),
            resp_del: RpcMetrics::new(gauge_factory, "resp_del"),
            resp_get: RpcMetrics::new(gauge_factory, "resp_get"),
            resp_hdel: RpcMetrics::new(gauge_factory, "resp_hdel"),
            resp_hexists: RpcMetrics::new(gauge_factory, "resp_hexists"),
            resp_hget: RpcMetrics::new(gauge_factory, "resp_hget"),
            resp_hgetall: RpcMetrics::new(gauge_factory, "resp_hgetall"),
            resp_hincrby: RpcMetrics::new(gauge_factory, "resp_hincrby"),
            resp_hkeys: RpcMetrics::new(gauge_factory, "resp_hkeys"),
            resp_hlen: RpcMetrics::new(gauge_factory, "resp_hlen"),
            resp_hmget: RpcMetrics::new(gauge_factory, "resp_hmget"),
            resp_hset: RpcMetrics::new(gauge_factory, "resp_hset"),
            resp_hvals: RpcMetrics::new(gauge_factory, "resp_hvals"),
            resp_lindex: RpcMetrics::new(gauge_factory, "resp_lindex"),
            resp_llen: RpcMetrics::new(gauge_factory, "resp_llen"),
            resp_lpop: RpcMetrics::new(gauge_factory, "resp_lpop"),
            resp_lrange: RpcMetrics::new(gauge_factory, "resp_lrange"),
            resp_lpush: RpcMetrics::new(gauge_factory, "resp_lpush"),
            resp_rpush: RpcMetrics::new(gauge_factory, "resp_rpush"),
            resp_rpop: RpcMetrics::new(gauge_factory, "resp_rpop"),
            resp_set: RpcMetrics::new(gauge_factory, "resp_set"),
            resp_sadd: RpcMetrics::new(gauge_factory, "resp_sadd"),
            resp_srem: RpcMetrics::new(gauge_factory, "resp_srem"),
            resp_sdiff: RpcMetrics::new(gauge_factory, "resp_sdiff"),
            resp_sunion: RpcMetrics::new(gauge_factory, "resp_sunion"),
            resp_sinter: RpcMetrics::new(gauge_factory, "resp_sinter"),
            resp_smembers: RpcMetrics::new(gauge_factory, "resp_smembers"),
            resp_sismember: RpcMetrics::new(gauge_factory, "resp_sismember"),
            resp_zcard: RpcMetrics::new(gauge_factory, "resp_zcard"),
            resp_zincrby: RpcMetrics::new(gauge_factory, "resp_zincrby"),
            resp_zscore: RpcMetrics::new(gauge_factory, "resp_zscore"),
            resp_zmscore: RpcMetrics::new(gauge_factory, "resp_zmscore"),
            resp_zrem: RpcMetrics::new(gauge_factory, "resp_zrem"),
            resp_zrank: RpcMetrics::new(gauge_factory, "resp_zrank"),
            resp_zrange: RpcMetrics::new(gauge_factory, "resp_zrange"),
            resp_zadd: RpcMetrics::new(gauge_factory, "resp_zadd"),
            resp_zrevrank: RpcMetrics::new(gauge_factory, "resp_zrevrank"),
            resp_zcount: RpcMetrics::new(gauge_factory, "resp_zcount"),
            resp_zunionstore: RpcMetrics::new(gauge_factory, "resp_zunionstore"),
            resp_unimplemented: RpcMetrics::new(gauge_factory, "resp_unimplemented"),
        };

        Arc::new(metrics)
    }
}

fn get_base_environment_dimensions() -> DimensionPosition {
    DimensionPosition::from_iter(
        vec![
            // We require a standard Otel Collector `service.instance.id` and `service.name`
            // dimensions in order to ingest metrics, otherwise they are rejected.
            // We don't really "need" a distinct value, we just need something, which
            // will default to `unknown`. If we need something in the future, we can
            // add that as necessary.
            (
                "service.instance.id",
                get_environment_variable("SERVICE_INSTANCE_ID"),
            ),
            ("service.name", get_environment_variable("SERVICE_NAME")),
        ]
        .into_iter()
        .map(|(n, v)| (n.into(), v.into())),
    )
}

fn get_environment_variable(variable: &str) -> String {
    match std::env::var(variable) {
        Ok(val) => val,
        Err(_) => {
            info!(
                "Environment variable {} not set, defaulting to 'unknown'",
                variable
            );
            "unknown".to_string()
        }
    }
}

fn get_environment_variable_or_none(variable: &str) -> Option<String> {
    std::env::var(variable).ok()
}
