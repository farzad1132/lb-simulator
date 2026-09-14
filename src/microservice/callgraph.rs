use clap::ValueEnum;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum MsServiceDistribution {
    #[default]
    Exp,
    Fixed,
    Bimodal,
}

#[derive(Debug, Clone)]
pub struct MicroserviceSpec {
    pub cpu: u32,
    pub replicas: u32,
}

#[derive(Debug, Default)]
pub struct CallGraph {
    pub microservices: HashMap<String, MicroserviceSpec>,
    pub interface_means: HashMap<String, f32>, // endpoint -> mean seconds (from callgraph ms)
    pub children: HashMap<String, Vec<(String, Option<String>)>>,
    pub entrypoints: HashMap<String, String>,
    pub endpoint_microservice: HashMap<String, String>,
    pub microservice_order: Vec<String>,
    pub service_dist: MsServiceDistribution,
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
        let mut microservices = HashMap::new();
        let mut interface_means = HashMap::new();
        let mut endpoint_microservice = HashMap::new();
        let mut microservice_order = Vec::new();

        for node in &file.nodes {
            if node.id == "USER" {
                continue;
            }
            let cpu = node
                .cpu
                .ok_or_else(|| format!("node {} missing cpu", node.id))?;
            let replicas = node
                .replicas
                .ok_or_else(|| format!("node {} missing replicas", node.id))?;
            if cpu == 0 || replicas == 0 {
                return Err(format!(
                    "node {} must have cpu > 0 and replicas > 0",
                    node.id
                ));
            }
            microservices.insert(
                node.id.clone(),
                MicroserviceSpec {
                    cpu: cpu.max(1),
                    replicas: replicas.max(1),
                },
            );
            microservice_order.push(node.id.clone());

            for iface in &node.interfaces {
                let endpoint = format!("{}:{}", node.id, iface.name);
                let mean = interface_mean(iface)?;
                interface_means.insert(endpoint.clone(), mean);
                endpoint_microservice.insert(endpoint, node.id.clone());
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
                if entrypoints
                    .insert(api.clone(), edge.target.clone())
                    .is_some()
                {
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
            microservices,
            interface_means,
            children,
            entrypoints,
            endpoint_microservice,
            microservice_order,
            service_dist: MsServiceDistribution::Exp,
        })
    }

    pub fn apply_scale(&mut self, delta: u32) -> Result<(), String> {
        if delta == 0 {
            return Ok(());
        }
        for (id, spec) in &mut self.microservices {
            spec.cpu = spec
                .cpu
                .checked_add(delta)
                .ok_or_else(|| format!("node {id} cpu overflow after --scale {delta}"))?;
            spec.replicas = spec
                .replicas
                .checked_add(delta)
                .ok_or_else(|| format!("node {id} replicas overflow after --scale {delta}"))?;
        }
        Ok(())
    }

    /// Add `cpu`/`replicas` and stretch endpoint means so `cpu / E[S]` stays constant.
    ///
    /// All-tier `all` is applied first, then each named delta adds on top.
    pub fn apply_eq_scale(
        &mut self,
        all: Option<u32>,
        named: &HashMap<String, u32>,
    ) -> Result<(), String> {
        for id in named.keys() {
            if !self.microservices.contains_key(id) {
                return Err(format!("--eq-scale unknown microservice {id}"));
            }
        }
        if let Some(delta) = all {
            let ids = self.microservice_order.clone();
            for id in ids {
                self.eq_scale_service(&id, delta)?;
            }
        }
        for (id, delta) in named {
            self.eq_scale_service(id, *delta)?;
        }
        Ok(())
    }

    fn eq_scale_service(&mut self, id: &str, delta: u32) -> Result<(), String> {
        if delta == 0 {
            return Ok(());
        }
        let cpu = {
            let spec = self
                .microservices
                .get_mut(id)
                .ok_or_else(|| format!("--eq-scale unknown microservice {id}"))?;
            let cpu = spec.cpu;
            spec.cpu = spec
                .cpu
                .checked_add(delta)
                .ok_or_else(|| format!("node {id} cpu overflow after --eq-scale {delta}"))?;
            spec.replicas = spec
                .replicas
                .checked_add(delta)
                .ok_or_else(|| format!("node {id} replicas overflow after --eq-scale {delta}"))?;
            cpu
        };
        let factor = (cpu as f32 + delta as f32) / (cpu as f32);
        let endpoints: Vec<String> = self
            .endpoint_microservice
            .iter()
            .filter(|(_, ms)| ms.as_str() == id)
            .map(|(endpoint, _)| endpoint.clone())
            .collect();
        for endpoint in endpoints {
            if let Some(mean) = self.interface_means.get_mut(&endpoint) {
                *mean *= factor;
            }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EqScaleSpec {
    All(u32),
    Named(String, u32),
}

pub fn parse_eq_scale_spec(s: &str) -> Result<EqScaleSpec, String> {
    if let Some((name, count)) = s.split_once('=') {
        if name.is_empty() {
            return Err("--eq-scale NAME=N requires a microservice name".into());
        }
        let n: u32 = count
            .parse()
            .map_err(|_| format!("invalid --eq-scale count {count:?}"))?;
        Ok(EqScaleSpec::Named(name.to_string(), n))
    } else {
        let n: u32 = s
            .parse()
            .map_err(|_| format!("invalid --eq-scale value {s:?} (expected N or NAME=N)"))?;
        Ok(EqScaleSpec::All(n))
    }
}

pub fn collect_eq_scale(
    specs: &[EqScaleSpec],
) -> Result<(Option<u32>, HashMap<String, u32>), String> {
    let mut all = None;
    let mut named = HashMap::new();
    for spec in specs {
        match spec {
            EqScaleSpec::All(n) => {
                if all.is_some() {
                    return Err("at most one bare --eq-scale N is allowed".into());
                }
                all = Some(*n);
            }
            EqScaleSpec::Named(name, n) => {
                if named.insert(name.clone(), *n).is_some() {
                    return Err(format!("duplicate --eq-scale microservice {name}"));
                }
            }
        }
    }
    Ok((all, named))
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
        (None, None) => Err(format!(
            "interface {} missing avg_rt or exponential",
            iface.name
        ))?,
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
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fanin/single/load.json");
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
        assert_eq!(graph.microservices["frontend"].cpu, 7);
        assert_eq!(graph.microservices["frontend"].replicas, 7);
        assert_eq!(graph.microservices["backend1"].cpu, 8);
        assert_eq!(graph.microservices["backend1"].replicas, 8);
        assert_eq!(graph.microservices["shared"].cpu, 9);
        assert_eq!(graph.microservices["shared"].replicas, 9);
    }

    fn chain3_graph() -> CallGraph {
        CallGraph::from_file(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/chain/3/callgraph.json"),
        )
        .unwrap()
    }

    #[test]
    fn apply_eq_scale_all_adds_cpu_replicas_and_stretches_means() {
        let mut graph = chain3_graph();
        let orig = graph.interface_means["frontend:handle"];
        graph.apply_eq_scale(Some(10), &HashMap::new()).unwrap();
        for id in ["frontend", "backend1", "backend2"] {
            assert_eq!(graph.microservices[id].cpu, 20);
            assert_eq!(graph.microservices[id].replicas, 20);
        }
        assert_eq!(graph.interface_means["frontend:handle"], orig * 2.0);
        assert_eq!(graph.interface_means["backend2:f2"], orig * 2.0);
    }

    #[test]
    fn apply_eq_scale_named_leaves_other_tiers_untouched() {
        let mut graph = chain3_graph();
        let frontend_mean = graph.interface_means["frontend:handle"];
        let backend2_mean = graph.interface_means["backend2:f2"];
        let named = HashMap::from([("backend2".to_string(), 10)]);
        graph.apply_eq_scale(None, &named).unwrap();
        assert_eq!(graph.microservices["frontend"].cpu, 10);
        assert_eq!(graph.microservices["frontend"].replicas, 10);
        assert_eq!(graph.interface_means["frontend:handle"], frontend_mean);
        assert_eq!(graph.microservices["backend2"].cpu, 20);
        assert_eq!(graph.microservices["backend2"].replicas, 20);
        assert_eq!(graph.interface_means["backend2:f2"], backend2_mean * 2.0);
    }

    #[test]
    fn apply_eq_scale_named_adds_on_top_of_all() {
        let mut graph = chain3_graph();
        let orig = graph.interface_means["backend2:f2"];
        let named = HashMap::from([("backend2".to_string(), 10)]);
        graph.apply_eq_scale(Some(10), &named).unwrap();
        assert_eq!(graph.microservices["frontend"].cpu, 20);
        assert_eq!(graph.microservices["backend2"].cpu, 30);
        assert_eq!(graph.microservices["backend2"].replicas, 30);
        assert_eq!(graph.interface_means["backend2:f2"], orig * 3.0);
        assert_eq!(graph.interface_means["frontend:handle"], orig * 2.0);
    }

    #[test]
    fn apply_eq_scale_zero_is_noop() {
        let mut graph = chain3_graph();
        let cpu = graph.microservices["frontend"].cpu;
        let mean = graph.interface_means["frontend:handle"];
        graph.apply_eq_scale(Some(0), &HashMap::new()).unwrap();
        assert_eq!(graph.microservices["frontend"].cpu, cpu);
        assert_eq!(graph.interface_means["frontend:handle"], mean);
    }

    #[test]
    fn apply_eq_scale_unknown_name_errors() {
        let mut graph = chain3_graph();
        let named = HashMap::from([("missing".to_string(), 10)]);
        let err = graph.apply_eq_scale(None, &named).unwrap_err();
        assert!(err.contains("unknown microservice missing"));
    }

    #[test]
    fn apply_eq_scale_composes_after_scale() {
        let mut graph = chain3_graph();
        let orig = graph.interface_means["frontend:handle"];
        graph.apply_scale(10).unwrap();
        graph.apply_eq_scale(Some(10), &HashMap::new()).unwrap();
        assert_eq!(graph.microservices["frontend"].cpu, 30);
        assert_eq!(graph.microservices["frontend"].replicas, 30);
        assert_eq!(graph.interface_means["frontend:handle"], orig * 1.5);
    }

    #[test]
    fn parse_eq_scale_spec_all_and_named() {
        assert_eq!(parse_eq_scale_spec("10").unwrap(), EqScaleSpec::All(10));
        assert_eq!(
            parse_eq_scale_spec("backend2=10").unwrap(),
            EqScaleSpec::Named("backend2".into(), 10)
        );
    }

    #[test]
    fn collect_eq_scale_rejects_duplicate_all_and_named() {
        let err = collect_eq_scale(&[EqScaleSpec::All(10), EqScaleSpec::All(5)]).unwrap_err();
        assert!(err.contains("at most one bare"));
        let err = collect_eq_scale(&[
            EqScaleSpec::Named("backend2".into(), 10),
            EqScaleSpec::Named("backend2".into(), 5),
        ])
        .unwrap_err();
        assert!(err.contains("duplicate"));
    }
}
