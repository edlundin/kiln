use std::collections::HashMap;

use crate::{ModelInvocationId, ModelWorkId, ProviderAccountId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidUsage {
    Identifier,
    Dimension,
    Unit,
    Relation,
    DuplicateQuantity,
    SubsetExceedsTotal,
    Overflow,
    Terminal,
    Correction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageAccounting {
    Delta,
    Cumulative,
}

impl UsageAccounting {
    pub fn parse(value: &str) -> Result<Self, InvalidUsage> {
        match value {
            "delta" => Ok(Self::Delta),
            "cumulative" => Ok(Self::Cumulative),
            _ => Err(InvalidUsage::Identifier),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Cumulative => "cumulative",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageFinality {
    Partial,
    Final,
    Correction,
}

impl UsageFinality {
    pub fn parse(value: &str) -> Result<Self, InvalidUsage> {
        match value {
            "partial" => Ok(Self::Partial),
            "final" => Ok(Self::Final),
            "correction" => Ok(Self::Correction),
            _ => Err(InvalidUsage::Identifier),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Final => "final",
            Self::Correction => "correction",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageCompleteness {
    Complete,
    Partial,
    Unknown,
}

impl UsageCompleteness {
    pub fn parse(value: &str) -> Result<Self, InvalidUsage> {
        match value {
            "complete" => Ok(Self::Complete),
            "partial" => Ok(Self::Partial),
            "unknown" => Ok(Self::Unknown),
            _ => Err(InvalidUsage::Identifier),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuantityRelation {
    Additive,
    Subset { of: String },
    Informational,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageQuantity {
    dimension: String,
    unit: String,
    amount: u64,
    relation: QuantityRelation,
}

impl UsageQuantity {
    pub fn new(
        dimension: impl Into<String>,
        unit: impl Into<String>,
        amount: u64,
        relation: QuantityRelation,
    ) -> Result<Self, InvalidUsage> {
        let dimension = dimension.into();
        let unit = unit.into();
        if !valid_dimension(&dimension) {
            return Err(InvalidUsage::Dimension);
        }
        if !valid_name(&unit) {
            return Err(InvalidUsage::Unit);
        }
        if let QuantityRelation::Subset { of } = &relation
            && (!valid_dimension(of) || of == &dimension)
        {
            return Err(InvalidUsage::Relation);
        }
        let canonical_relation = match dimension.as_str() {
            "tokens.input" | "tokens.output" => Some(QuantityRelation::Additive),
            "tokens.input.cached" => Some(QuantityRelation::Subset {
                of: "tokens.input".to_owned(),
            }),
            "tokens.output.reasoning" => Some(QuantityRelation::Subset {
                of: "tokens.output".to_owned(),
            }),
            _ => None,
        };
        if let Some(expected) = canonical_relation {
            if unit != "token" {
                return Err(InvalidUsage::Unit);
            }
            if relation != expected {
                return Err(InvalidUsage::Relation);
            }
        }
        Ok(Self {
            dimension,
            unit,
            amount,
            relation,
        })
    }

    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    pub fn unit(&self) -> &str {
        &self.unit
    }

    pub fn amount(&self) -> u64 {
        self.amount
    }

    pub fn relation(&self) -> &QuantityRelation {
        &self.relation
    }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn valid_dimension(value: &str) -> bool {
    value.contains('.') && value.split('.').all(valid_name)
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
}

fn validate_quantities(quantities: &[UsageQuantity]) -> Result<(), InvalidUsage> {
    let mut amounts = HashMap::new();
    for quantity in quantities {
        if amounts
            .insert((quantity.dimension(), quantity.unit()), quantity.amount())
            .is_some()
        {
            return Err(InvalidUsage::DuplicateQuantity);
        }
    }
    for quantity in quantities {
        if let QuantityRelation::Subset { of } = quantity.relation()
            && let Some(total) = amounts.get(&(of.as_str(), quantity.unit()))
            && quantity.amount() > *total
        {
            return Err(InvalidUsage::SubsetExceedsTotal);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageSource {
    NativeProvider,
}

impl UsageSource {
    pub fn parse(value: &str) -> Result<Self, InvalidUsage> {
        match value {
            "native_provider" => Ok(Self::NativeProvider),
            _ => Err(InvalidUsage::Identifier),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NativeProvider => "native_provider",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsageMetadata {
    pub update_id: String,
    pub provider_account_id: ProviderAccountId,
    pub work_id: ModelWorkId,
    pub model_invocation_id: ModelInvocationId,
    pub accounting: UsageAccounting,
    pub finality: UsageFinality,
    pub completeness: UsageCompleteness,
    pub observed_at_unix_ms: u64,
    pub request_id: Option<String>,
    pub resolved_model: Option<String>,
    pub service_tier: Option<String>,
    pub source: UsageSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsageUpdate {
    metadata: ProviderUsageMetadata,
    quantities: Vec<UsageQuantity>,
}

impl ProviderUsageUpdate {
    pub fn new(
        metadata: ProviderUsageMetadata,
        mut quantities: Vec<UsageQuantity>,
    ) -> Result<Self, InvalidUsage> {
        if !valid_identifier(&metadata.update_id)
            || [
                metadata.request_id.as_deref(),
                metadata.resolved_model.as_deref(),
                metadata.service_tier.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| !valid_identifier(value))
        {
            return Err(InvalidUsage::Identifier);
        }
        if metadata.finality == UsageFinality::Correction
            && metadata.accounting != UsageAccounting::Cumulative
        {
            return Err(InvalidUsage::Correction);
        }
        validate_quantities(&quantities)?;
        quantities.sort_by(|left, right| {
            (left.dimension(), left.unit()).cmp(&(right.dimension(), right.unit()))
        });
        Ok(Self {
            metadata,
            quantities,
        })
    }

    pub fn metadata(&self) -> &ProviderUsageMetadata {
        &self.metadata
    }

    pub fn quantities(&self) -> &[UsageQuantity] {
        &self.quantities
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveUsage {
    quantities: Vec<UsageQuantity>,
    is_final: bool,
}

impl EffectiveUsage {
    pub fn quantities(&self) -> &[UsageQuantity] {
        &self.quantities
    }

    pub fn is_final(&self) -> bool {
        self.is_final
    }
}

/// Apply an update after the store has excluded duplicate update IDs.
pub fn apply_usage_update(
    previous: &[UsageQuantity],
    terminal: bool,
    update: &ProviderUsageUpdate,
) -> Result<EffectiveUsage, InvalidUsage> {
    let metadata = update.metadata();
    if metadata.finality == UsageFinality::Correction {
        if !terminal {
            return Err(InvalidUsage::Correction);
        }
    } else if terminal {
        return Err(InvalidUsage::Terminal);
    }
    validate_quantities(previous)?;
    let quantities = match metadata.accounting {
        UsageAccounting::Cumulative => update.quantities.clone(),
        UsageAccounting::Delta => {
            let mut quantities = previous.to_vec();
            let mut indices: HashMap<_, _> = quantities
                .iter()
                .enumerate()
                .map(|(index, quantity)| {
                    ((quantity.dimension.clone(), quantity.unit.clone()), index)
                })
                .collect();
            for delta in update.quantities() {
                let key = (delta.dimension.clone(), delta.unit.clone());
                if let Some(index) = indices.get(&key) {
                    let existing = &mut quantities[*index];
                    if existing.relation != delta.relation {
                        return Err(InvalidUsage::Relation);
                    }
                    existing.amount = existing
                        .amount
                        .checked_add(delta.amount)
                        .ok_or(InvalidUsage::Overflow)?;
                } else {
                    indices.insert(key, quantities.len());
                    quantities.push(delta.clone());
                }
            }
            quantities
        }
    };
    validate_quantities(&quantities)?;
    Ok(EffectiveUsage {
        quantities,
        is_final: terminal || metadata.finality == UsageFinality::Final,
    })
}
