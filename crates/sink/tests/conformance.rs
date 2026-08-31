//! The sink conformance suite, run against a live target.
//!
//! Each case needs a server, so each is driven by an environment variable and
//! reports itself as skipped when that variable is unset — `cargo test` on a
//! laptop with nothing running still passes, and the e2e suites set the
//! variable so a pull request really does run them.

use pg2osync_core::sink::{IndexSpec, Sink};
use pg2osync_core::testkit::SinkTestHarness;
use serde_json::json;

/// The table the harness writes to, and the shape of the documents it writes.
///
/// A `vector` column is part of it on purpose: bring-your-own-embedding has to
/// hold for every write the contract covers, not only for the e2e suite's happy
/// path.
const DDL: &str = "CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE conformance_kit (
  id text PRIMARY KEY,
  name text,
  embedding vector(3),
  _version bigint
);";

/// The reference target runs the same suite, so the kit is a contract rather
/// than a description of the newest sink.
#[tokio::test]
async fn the_opensearch_sink_honours_the_sink_contract() {
    let Ok(url) = std::env::var("PG2OSYNC_TEST_OS_URL") else {
        eprintln!("skipped: set PG2OSYNC_TEST_OS_URL to an OpenSearch to run this");
        return;
    };
    let sink = pg2osync_sink::OpenSearchSink::new(pg2osync_sink::OpenSearchSinkConfig {
        url,
        username: None,
        password: None,
        tls_verify: true,
        retry: pg2osync_sink::RetryPolicy::default(),
        require_alias: false,
    })
    .expect("a sink over the target cluster");
    sink.delete_index("conformance_kit")
        .await
        .expect("an index left behind by an earlier run is not this run's state");
    sink.ensure_ready(&[IndexSpec {
        name: "conformance_kit".into(),
        // `weight` is declared so there is a value the target refuses: with no
        // mapping at all, dynamic mapping accepts everything and the
        // partial-batch check would have nothing to provoke.
        mapping: Some(json!({"mappings": {"properties": {
            "name": {"type": "keyword"},
            "weight": {"type": "long"}
        }}})),
        pattern: false,
    }])
    .await
    .expect("the index the mapping creates");

    let report = SinkTestHarness::new(
        "conformance_kit",
        |id| json!({"name": format!("row {id}"), "weight": 7}),
    )
    .with_unacceptable_document(json!({"name": "bad", "weight": "not a number"}))
    .run(&sink)
    .await
    .expect("the OpenSearch sink honours the Sink contract");
    assert!(
        report.skipped.is_empty(),
        "this target can answer every check: {report:?}"
    );
}

/// The collection the harness writes to. `title` is payload and `embedding` is
/// the one named vector, which is how this target tells the two apart.
fn qdrant_collection() -> serde_json::Value {
    // Dot rather than Cosine so a vector reads back as it was written:
    // Cosine normalises on the way in, which is correct and would only make
    // the assertions about the read-back harder to read.
    json!({"vectors": {"embedding": {"size": 3, "distance": "Dot"}}})
}

#[tokio::test]
async fn the_qdrant_sink_honours_the_sink_contract() {
    let Ok(url) = std::env::var("PG2OSYNC_TEST_QDRANT_URL") else {
        eprintln!("skipped: set PG2OSYNC_TEST_QDRANT_URL to a Qdrant to run this");
        return;
    };
    let sink = pg2osync_sink::qdrant::QdrantSink::new(pg2osync_sink::qdrant::QdrantSinkConfig {
        url,
        api_key: std::env::var("PG2OSYNC_TEST_QDRANT_KEY").ok(),
        retry: pg2osync_sink::RetryPolicy::default(),
    })
    .expect("a sink over the target");
    sink.delete_index("conformance_kit")
        .await
        .expect("a collection left behind by an earlier run is not this run's state");
    sink.ensure_ready(&[IndexSpec {
        name: "conformance_kit".into(),
        mapping: Some(qdrant_collection()),
        pattern: false,
    }])
    .await
    .expect("the collection the configuration creates");

    let report = SinkTestHarness::new(
        "conformance_kit",
        |id| json!({"title": format!("row {id}"), "embedding": [1.0, 2.0, 3.0]}),
    )
    // the collection holds three dimensions, which is what this target refuses
    // a document for
    .with_unacceptable_document(json!({"title": "bad", "embedding": [1.0, 2.0]}))
    .run(&sink)
    .await
    .expect("the Qdrant sink honours the Sink contract");
    assert!(
        report.skipped.is_empty(),
        "this target can answer every check: {report:?}"
    );
}

#[tokio::test]
async fn the_postgres_sink_honours_the_sink_contract() {
    let Ok(url) = std::env::var("PG2OSYNC_TEST_PG_SINK_URL") else {
        eprintln!("skipped: set PG2OSYNC_TEST_PG_SINK_URL to a pgvector database to run this");
        return;
    };
    let sink =
        pg2osync_sink::postgres::PostgresSink::new(pg2osync_sink::postgres::PostgresSinkConfig {
            url,
            retry: pg2osync_sink::RetryPolicy::default(),
        })
        .expect("a sink over the target database");
    sink.delete_index("conformance_kit")
        .await
        .expect("a table left behind by an earlier run is not this run's state");
    sink.ensure_ready(&[IndexSpec {
        name: "conformance_kit".into(),
        mapping: Some(json!(DDL)),
        pattern: false,
    }])
    .await
    .expect("the table the DDL creates");

    let report = SinkTestHarness::new(
        "conformance_kit",
        |id| json!({"name": format!("row {id}"), "embedding": [1.0, 2.0, 3.0]}),
    )
    // no column of that name, which is what this target refuses a document for
    .with_unacceptable_document(json!({"nickname": "nobody"}))
    .run(&sink)
    .await
    .expect("the PostgreSQL sink honours the Sink contract");
    assert!(
        report.skipped.is_empty(),
        "this target can answer every check: {report:?}"
    );
}
