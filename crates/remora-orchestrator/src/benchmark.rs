// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{protocol::ProtocolParameters, settings::Settings, ClientParameters, NodeParameters};

/// Shortcut avoiding to use the generic version of the benchmark parameters.
pub type BenchmarkParameters = BenchmarkParametersGeneric<NodeParameters, ClientParameters>;

/// The benchmark parameters for a run. These parameters are stored along with the performance data
/// and should be used to reproduce the results.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParametersGeneric<N, C> {
    /// The testbed settings.
    pub settings: Settings,
    /// The node's configuration parameters.
    pub node_parameters: N,
    /// The client's configuration parameters.
    pub client_parameters: C,
    /// The committee size.
    pub nodes: usize,
}

impl<N: Debug, C: Debug> Debug for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}-{:?}-{:?}-{}",
            self.node_parameters, self.client_parameters, self.settings.faults, self.nodes,
        )
    }
}

impl<N, C> Display for BenchmarkParametersGeneric<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} nodes ({})", self.nodes, self.settings.faults)
    }
}

impl<N: ProtocolParameters, C: ProtocolParameters> BenchmarkParametersGeneric<N, C> {
    /// Make a new benchmark parameters.
    pub fn new(settings: Settings, node_parameters: N, client_parameters: C, nodes: usize) -> Self {
        Self {
            settings,
            node_parameters,
            client_parameters,
            nodes,
        }
    }

    #[cfg(test)]
    pub fn new_for_tests() -> Self {
        Self {
            settings: Settings::new_for_test(),
            node_parameters: N::default(),
            client_parameters: C::default(),
            nodes: 4,
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::{fmt::Display, str::FromStr};

    use serde::{Deserialize, Serialize};

    use super::ProtocolParameters;

    /// Mock benchmark type for unit tests.
    #[derive(
        Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default,
    )]
    pub struct TestNodeConfig;

    impl Display for TestNodeConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestNodeConfig")
        }
    }

    impl FromStr for TestNodeConfig {
        type Err = ();

        fn from_str(_s: &str) -> Result<Self, Self::Err> {
            Ok(Self {})
        }
    }

    impl ProtocolParameters for TestNodeConfig {}
}
