use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ServiceSpec {
    pub cpu: u32,
    pub replicas: u32,
}

#[derive(Debug, Default)]
pub struct CallGraph {
    pub services: HashMap<String, ServiceSpec>,
    pub interface_means: HashMap<String, f32>, // endpoint -> mean seconds (from callgraph ms)
    pub children: HashMap<String, Vec<(String, Option<String>)>>,
    pub entrypoints: HashMap<String, String>,
    pub paths: HashMap<String, Vec<String>>,
    pub endpoint_service: HashMap<String, String>,
    pub service_order: Vec<String>,
}

pub type LoadSpec = HashMap<String, f64>;

#[derive(Deserialize)]
struct CallGraphFile {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

#[derive(Deserialize)]
struct Node {
    id: String,
    interfaces: Vec<Interface>,
    cpu: Option<u32>,
    replicas: Option<u32>,
}

#[derive(Deserialize)]
struct Interface {
    name: String,
    avg_rt: Option<f32>,
    exponential: Option<ExpMean>,
}

#[derive(Deserialize)]
struct ExpMean {
    mean: f32,
}

#[derive(Deserialize)]
struct Edge {
    source: String,
    target: String,
    api: Option<String>,
}

impl CallGraph {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let data = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let file: CallGraphFile = serde_json::from_str(&data).map_err(|e| e.to_string())?;
        Self::from_file_data(file)
    }

    fn from_file_data(file: CallGraphFile) -> Result<Self, String> {
        let mut services = HashMap::new();
        let mut interface_means = HashMap::new();
        let mut endpoint_service = HashMap::new();
        let mut service_order = Vec::new();

        for node in &file.nodes {
            if node.id == "USER" {
                continue;
            }
            let cpu = node.cpu.ok_or_else(|| format!("node {} missing cpu", node.id))?;
            let replicas = node
                .replicas
                .ok_or_else(|| format!("node {} missing replicas", node.id))?;
            if cpu == 0 || replicas == 0 {
                return Err(format!("node {} must have cpu > 0 and replicas > 0", node.id));
            }
            services.insert(
                node.id.clone(),
                ServiceSpec {
                    cpu: cpu.max(1),
                    replicas: replicas.max(1),
                },
            );
            service_order.push(node.id.clone());

            for iface in &node.interfaces {
                let endpoint = format!("{}:{}", node.id, iface.name);
                let mean = interface_mean(iface)?;
                interface_means.insert(endpoint.clone(), mean);
                endpoint_service.insert(endpoint, node.id.clone());
            }
        }

        let mut children: HashMap<String, Vec<(String, Option<String>)>> = HashMap::new();
        let mut entrypoints = HashMap::new();

        for edge in &file.edges {
            if !interface_means.contains_key(&edge.target) && edge.target != "USER" {
                return Err(format!("unknown edge target {}", edge.target));
            }
            if edge.source != "USER" && !interface_means.contains_key(&edge.source) {
                return Err(format!("unknown edge source {}", edge.source));
            }

            children
                .entry(edge.source.clone())
                .or_default()
                .push((edge.target.clone(), edge.api.clone()));

            if edge.source == "USER" {
                let api = edge
                    .target
                    .split(':')
                    .next_back()
                    .ok_or_else(|| format!("invalid entry target {}", edge.target))?
                    .to_string();
                if entrypoints.insert(api.clone(), edge.target.clone()).is_some() {
                    return Err(format!("duplicate entry API {}", api));
                }
            }
        }

        let mut paths = HashMap::new();
        for (api, entry) in &entrypoints {
            let mut path = Vec::new();
            let mut stack = HashSet::new();
            build_path(api, entry, &children, &mut path, &mut stack)?;
            paths.insert(api.clone(), path);
        }

        Ok(Self {
            services,
            interface_means,
            children,
            entrypoints,
            paths,
            endpoint_service,
            service_order,
        })
    }

    pub fn validate_load(&self, load: &LoadSpec) -> Result<(), String> {
        for api in load.keys() {
            if !self.entrypoints.contains_key(api) {
                return Err(format!(
                    "load.json API {} has no entrypoint in callgraph",
                    api
                ));
            }
        }
        Ok(())
    }
}

pub fn load_spec_from_file(path: &Path) -> Result<LoadSpec, String> {
    let data = fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&data).map_err(|e| e.to_string())
}

/// Callgraph mean times are in milliseconds; simulation uses seconds.
const MS_TO_SECS: f32 = 1e-3;

fn interface_mean(iface: &Interface) -> Result<f32, String> {
    let mean_ms = match (iface.avg_rt, &iface.exponential) {
        (Some(rt), None) => rt,
        (None, Some(exp)) => exp.mean,
        (Some(rt), Some(_)) => rt,
        (None, None) => Err(format!("interface {} missing avg_rt or exponential", iface.name))?,
    };
    Ok(mean_ms * MS_TO_SECS)
}

fn build_path(
    api: &str,
    endpoint: &str,
    children: &HashMap<String, Vec<(String, Option<String>)>>,
    path: &mut Vec<String>,
    stack: &mut HashSet<String>,
) -> Result<(), String> {
    if !stack.insert(endpoint.to_string()) {
        return Err(format!("cycle detected at {} for API {}", endpoint, api));
    }
    path.push(endpoint.to_string());

    if let Some(edges) = children.get(endpoint) {
        for (target, edge_api) in edges {
            if edge_api.as_deref() == Some(api) {
                build_path(api, target, children, path, stack)?;
            }
        }
    }

    stack.remove(endpoint);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn fanin_paths() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fanin/callgraph.json");
        let graph = CallGraph::from_file(&path).unwrap();
        assert_eq!(
            graph.paths["f1"],
            vec![
                "frontend:f1",
                "backend1:f2",
                "shared:f5",
                "backend2:f4",
                "shared:f5",
            ]
        );
        assert_eq!(graph.paths["g1"], vec!["frontend:g1", "backend1:f3"]);
    }
}
