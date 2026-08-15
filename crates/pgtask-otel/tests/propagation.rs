use std::fmt;

use opentelemetry::{
    Context, KeyValue,
    baggage::BaggageExt,
    global,
    propagation::{Extractor, Injector, TextMapPropagator, text_map_propagator::FieldIter},
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use pgtask_otel::{configure_propagation, inject_context, set_parent_from_headers};
use serde_json::{Map, Value};

struct KeysPropagator {
    fields: Vec<String>,
}

impl fmt::Debug for KeysPropagator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KeysPropagator").finish()
    }
}

impl TextMapPropagator for KeysPropagator {
    fn inject_context(&self, _context: &Context, _injector: &mut dyn Injector) {}

    fn extract_with_context(&self, context: &Context, extractor: &dyn Extractor) -> Context {
        assert!(extractor.keys().contains(&"application"));
        assert_eq!(extractor.get("application"), Some("kept"));
        assert_eq!(extractor.get("numeric"), None);
        assert_eq!(extractor.get("missing"), None);
        context.clone()
    }

    fn fields(&self) -> FieldIter<'_> {
        FieldIter::new(&self.fields)
    }
}

#[test]
fn propagation_uses_json_headers_without_replacing_application_values() {
    configure_propagation();
    let span_context = SpanContext::new(
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        SpanId::from_hex("00f067aa0ba902b7").unwrap(),
        TraceFlags::SAMPLED,
        false,
        TraceState::default(),
    );
    let context = Context::new()
        .with_remote_span_context(span_context)
        .with_baggage([KeyValue::new("tenant", "acme")]);
    let mut original = Map::new();
    original.insert("application".to_owned(), Value::String("kept".to_owned()));
    original.insert("numeric".to_owned(), Value::from(1));

    let headers = inject_context(&original, &context);

    assert_eq!(headers["application"], "kept");
    assert_eq!(
        headers["traceparent"],
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    );
    assert_eq!(headers["baggage"], "tenant=acme");

    global::set_text_map_propagator(KeysPropagator { fields: Vec::new() });
    assert!(set_parent_from_headers(&tracing::info_span!("consumer"), &headers).is_err());
    configure_propagation();
}
