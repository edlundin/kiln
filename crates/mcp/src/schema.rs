//! Shared policy for untrusted schemas: validation never retrieves external data.

pub(crate) struct NoRetrieval;
impl jsonschema::Retrieve for NoRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::other("external schema retrieval is disabled").into())
    }
}
