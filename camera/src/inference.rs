use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use futures::{StreamExt, stream::FuturesOrdered};
use log::error;
use ort::session::{Session, builder::GraphOptimizationLevel};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::{Level, span};

#[derive(Clone, Debug, Deserialize)]
pub struct InferenceConfig {
    pub models: Vec<InferenceModelConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InferenceModelConfig {
    pub name: String,
    pub onnx_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct InferenceModel {
    pub name: String,
    pub session: Arc<Mutex<Session>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InferenceResult {
    pub model: String,
    pub outputs: Vec<InferenceOutput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InferenceOutput {
    pub key: String,
    pub values: Vec<f32>,
}

pub struct InferenceService {
    models: Vec<InferenceModel>,
}

pub async fn load_inference_config(path: &Path) -> Result<InferenceConfig, Box<dyn Error>> {
    let mut file = tokio::fs::File::open(path).await?;

    let mut serialized_metadata = Vec::new();
    file.read_to_end(&mut serialized_metadata).await?;

    let config: InferenceConfig = serde_json::from_slice(&serialized_metadata)?;
    Ok(config)
}

impl InferenceService {
    pub async fn new(config: InferenceConfig) -> Arc<InferenceService> {
        // Confirm that all the model paths are valid
        let models = config
            .models
            .iter()
            .map(async |config| {
                assert!(config.onnx_path.exists(), "Model path  does not exist");

                let onnx_bytes = tokio::fs::read(&config.onnx_path)
                    .await
                    .expect("Failed to load onnx file");
                let session = Session::builder()
                    .unwrap()
                    .with_optimization_level(GraphOptimizationLevel::Level3)
                    .unwrap()
                    .with_intra_threads(1)
                    .unwrap()
                    .commit_from_memory(onnx_bytes.as_slice())
                    .unwrap();
                InferenceModel {
                    name: config.name.clone(),
                    session: Arc::new(Mutex::new(session)),
                }
            })
            // Collect into a FuturesUnordered to run the file reads in parallel
            .collect::<FuturesOrdered<_>>()
            // Join the futures and collect into the result Vec
            .collect()
            .await;

        Arc::new(InferenceService { models })
    }

    pub fn run(&self, tensor_224: &[f32]) -> Result<Vec<InferenceResult>, Box<dyn Error>> {
        let tensor = ort::value::TensorRef::from_array_view(([1usize, 3, 224, 224], tensor_224))?;

        let outputs: Vec<InferenceResult> = self
            .models
            .iter()
            .map(|config| -> Result<InferenceResult, Box<dyn Error>> {
                let span = span!(Level::TRACE, "run_model");
                span.record("model", &config.name);
                let _enter = span.enter();

                let inputs = ort::inputs![&*tensor];

                let mut session = {
                    let span = span!(Level::TRACE, "session_mutex");
                    span.record("model", &config.name);
                    let _enter = span.enter();

                    config.session.lock()?
                };

                let session_outputs = {
                    let span = span!(Level::TRACE, "run");
                    span.record("model", &config.name);
                    let _enter = span.enter();

                    session.run(inputs)?
                };

                let outputs: Vec<InferenceOutput> = session_outputs
                    .into_iter()
                    .map(|(k, v)| {
                        let values = match v.try_extract_array::<f32>() {
                            Ok(array) => array.into_iter().copied().collect(),
                            Err(e) => {
                                error!("Failed to coerce output for {} into floats: {:?}", k, e);
                                return Err(e);
                            }
                        };
                        Ok(InferenceOutput {
                            key: k.into(),
                            values,
                        })
                    })
                    .filter_map(Result::ok)
                    .collect();

                Ok(InferenceResult {
                    model: config.name.clone(),
                    outputs,
                })
            })
            .filter_map(Result::ok)
            .collect();

        Ok(outputs)
    }
}
