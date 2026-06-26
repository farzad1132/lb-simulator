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
    pub endpoint_service: HashMap<String, String>,
    pub service_order: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiLoad {
    pub rps: f64,
    pub slo_ms: f64,
}

pub type LoadSpec = HashMap<String, ApiLoad>;

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

        for (api, entry) in &entrypoints {
            let mut path = Vec::new();
            let mut stack = HashSet::new();
            build_path(api, entry, &children, &mut path, &mut stack)?;
        }

        Ok(Self {
            services,
            interface_means,
            children,
            entrypoints,
            endpoint_service,
            service_order,
        })
    }

    pub fn apply_scale(&mut self, delta: u32) -> Result<(), String> {
        if delta == 0 {
            return Ok(());
        }
        for (id, spec) in &mut self.services {
            spec.cpu = spec.cpu.checked_add(delta).ok_or_else(|| {
                format!("node {id} cpu overflow after --scale {delta}")
            })?;
            spec.replicas = spec.replicas.checked_add(delta).ok_or_else(|| {
                format!("node {id} replicas overflow after --scale {delta}")
            })?;
        }
        Ok(())
    }

    pub fn validate_load(&self, load: &LoadSpec) -> Result<(), String> {
        for (api, spec) in load {
            if !self.entrypoints.contains_key(api) {
                return Err(format!(
                    "load.json API {} has no entrypoint in callgraph",
                    api
                ));
            }
            if spec.rps <= 0.0 {
                return Err(format!("load.json API {} must have rps > 0", api));
            }
            if spec.slo_ms <= 0.0 {
                return Err(format!("load.json API {} must have slo_ms > 0", api));
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
    fn load_spec_parses_api_objects() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fanin/single/load.json");
        let load = load_spec_from_file(&path).unwrap();
        assert_eq!(load["f1"].rps, 1200.0);
        assert_eq!(load["f1"].slo_ms, 35.0);
    }

    #[test]
    fn fanin_children() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fanin/single/callgraph.json");
        let graph = CallGraph::from_file(&path).unwrap();
        let f1_children: Vec<_> = graph
            .children
            .get("frontend:f1")
            .unwrap()
            .iter()
            .filter(|(_, api)| api.as_deref() == Some("f1"))
            .map(|(target, _)| target.as_str())
            .collect();
        assert_eq!(f1_children, vec!["backend1:f2", "backend2:f4"]);
        assert_eq!(
            graph.entrypoints.get("g1").map(String::as_str),
            Some("frontend:g1")
        );
    }

    #[test]
    fn apply_scale_adds_cpu_and_replicas() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fanin/multi/callgraph.json");
        let mut graph = CallGraph::from_file(&path).unwrap();
        graph.apply_scale(5).unwrap();
        assert_eq!(graph.services["frontend"].cpu, 7);
        assert_eq!(graph.services["frontend"].replicas, 7);
        assert_eq!(graph.services["backend1"].cpu, 8);
        assert_eq!(graph.services["backend1"].replicas, 8);
        assert_eq!(graph.services["shared"].cpu, 9);
        assert_eq!(graph.services["shared"].replicas, 9);
    }
}
