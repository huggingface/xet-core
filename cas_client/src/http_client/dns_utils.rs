use std::str::FromStr;

use futures::FutureExt;
use hyper_util::client::legacy::connect::dns::{GaiResolver as HyperGaiResolver, Name as HyperName};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tower_service::Service;

#[derive(Debug)]
pub struct GaiResolverWithAbsolute(HyperGaiResolver);

impl GaiResolverWithAbsolute {
    pub fn new() -> Self {
        Self(HyperGaiResolver::new())
    }
}

impl Default for GaiResolverWithAbsolute {
    fn default() -> Self {
        GaiResolverWithAbsolute::new()
    }
}

impl Resolve for GaiResolverWithAbsolute {
    fn resolve(&self, name: Name) -> Resolving {
        let this = &mut self.0.clone();
        // if the name does not end with a dot, we append it to make it absolute to avoid issues with relative names.
        // see https://github.com/huggingface/huggingface_hub/issues/3155
        let mut name_str = name.as_str().to_owned();
        if !name_str.ends_with('.') {
            name_str.push('.');
        }
        let hyper_name: HyperName = HyperName::from_str(&name_str).expect("Failed to parse DNS name");
        Box::pin(this.call(hyper_name).map(|result| {
            result
                .map(|addrs| -> Addrs { Box::new(addrs) })
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })
        }))
    }
}
