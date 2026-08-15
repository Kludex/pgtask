use opentelemetry::{
    Context, global,
    propagation::{Extractor, Injector, TextMapCompositePropagator},
};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use serde_json::{Map, Value};
use tracing::Span;
use tracing_opentelemetry::{OpenTelemetrySpanExt, SetParentError};

pub fn configure_propagation() {
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
}

pub fn inject_context(headers: &Map<String, Value>, context: &Context) -> Map<String, Value> {
    let mut headers = headers.clone();
    global::get_text_map_propagator(|propagator| propagator.inject_context(context, &mut HeaderInjector(&mut headers)));
    headers
}

pub fn inject_span_context(headers: &Map<String, Value>, span: &Span) -> Map<String, Value> {
    inject_context(headers, &span.context())
}

pub fn set_parent_from_headers(span: &Span, headers: &Map<String, Value>) -> Result<(), SetParentError> {
    let context = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)));
    span.set_parent(context)
}

struct HeaderInjector<'a>(&'a mut Map<String, Value>);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), Value::String(value));
    }
}

struct HeaderExtractor<'a>(&'a Map<String, Value>);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}
