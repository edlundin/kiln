//! Daemon composition for the infrastructure-owned follower exchange.

use kiln_client::{
    ConfigurationMasterPin, ConfigurationSyncClient, ConfigurationSyncError,
    ConfigurationSyncTimeouts,
};
use kiln_core::{
    ConfigurationFollowerEnrollmentExchangeSettings, ConfigurationFollowerEnrollmentRemoteGrant,
    ConfigurationFollowerEnrollmentRemotePhase, ConfigurationFollowerEnrollmentRemoteReceipt,
    ConfigurationFollowerEnrollmentSubmission, ConfigurationFollowerEnrollmentTransport,
    ConfigurationFollowerEnrollmentTransportError, ConfigurationFollowerSnapshotBundle,
    ConfigurationFollowerSnapshotCandidate, ConfigurationFollowerSnapshotPeer,
    ConfigurationFollowerSnapshotSkillFile, ConfigurationFollowerSnapshotSkillPackage,
    ConfigurationFollowerSnapshotTransport, ConfigurationFollowerSnapshotTransportError,
};
use kiln_protocol::ConfigurationFollowerEnrollmentRequestPhase;
use std::{future::Future, pin::Pin, time::Duration};

pub(crate) struct PinnedConfigurationFollowerEnrollmentTransport;

impl ConfigurationFollowerEnrollmentTransport for PinnedConfigurationFollowerEnrollmentTransport {
    fn submit(
        &self,
        submission: ConfigurationFollowerEnrollmentSubmission,
        credential: kiln_core::SecretValue,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerEnrollmentRemoteReceipt,
                        ConfigurationFollowerEnrollmentTransportError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let pin = ConfigurationMasterPin {
                origin: settings.origin,
                server_name: submission.server_name().to_owned(),
                certificate_authority_der: submission.certificate_authority_der().to_vec(),
                master_instance_id: submission.authority().master_id().as_str().to_owned(),
                group_id: submission.authority().group_id().as_str().to_owned(),
                follower_instance_id: submission.follower_instance_id().as_str().to_owned(),
            };
            let client = ConfigurationSyncClient::new(
                pin,
                credential.as_bytes(),
                ConfigurationSyncTimeouts {
                    connect: Duration::from_millis(settings.connect_timeout_ms),
                    request: Duration::from_millis(settings.request_timeout_ms),
                },
            )
            .map_err(map_client_error)?;
            let response = client
                .submit_enrollment_request(
                    submission.attempt_id().as_str(),
                    submission.follower_state_version(),
                )
                .await
                .map_err(|_| ConfigurationFollowerEnrollmentTransportError::Failed)?;
            Ok(remote_receipt(response))
        })
    }
}

pub(crate) struct PinnedConfigurationFollowerSnapshotTransport;

impl ConfigurationFollowerSnapshotTransport for PinnedConfigurationFollowerSnapshotTransport {
    fn fetch(
        &self,
        peer: ConfigurationFollowerSnapshotPeer,
        credential: kiln_core::SecretValue,
        settings: ConfigurationFollowerEnrollmentExchangeSettings,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ConfigurationFollowerSnapshotCandidate,
                        ConfigurationFollowerSnapshotTransportError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let pin = ConfigurationMasterPin {
                origin: settings.origin,
                server_name: peer.server_name,
                certificate_authority_der: peer.certificate_authority_der,
                master_instance_id: peer.authority.master_id().as_str().to_owned(),
                group_id: peer.authority.group_id().as_str().to_owned(),
                follower_instance_id: peer.follower_instance_id.as_str().to_owned(),
            };
            let client = ConfigurationSyncClient::new(
                pin,
                credential.as_bytes(),
                ConfigurationSyncTimeouts {
                    connect: Duration::from_millis(settings.connect_timeout_ms),
                    request: Duration::from_millis(settings.request_timeout_ms),
                },
            )
            .map_err(map_snapshot_client_error)?;
            let response = client
                .get_snapshot()
                .await
                .map_err(map_snapshot_client_error)?;
            Ok(ConfigurationFollowerSnapshotCandidate {
                instance_id: response.instance_id,
                group_id: response.group_id,
                master_instance_id: response.master_instance_id,
                state_version: response.state_version,
                revision_number: response.revision.revision,
                schema_version: response.revision.schema_version,
                content_hash: response.revision.content_hash,
                snapshot: ConfigurationFollowerSnapshotBundle {
                    metadata_json: response.snapshot.metadata_json,
                    skills: response
                        .snapshot
                        .skills
                        .into_iter()
                        .map(|skill| ConfigurationFollowerSnapshotSkillPackage {
                            id: skill.id,
                            version: skill.version,
                            enabled: skill.enabled,
                            dependencies: skill.dependencies,
                            files: skill
                                .files
                                .into_iter()
                                .map(|file| ConfigurationFollowerSnapshotSkillFile {
                                    path: file.path,
                                    content: file.content,
                                    content_hash: file.content_hash,
                                })
                                .collect(),
                        })
                        .collect(),
                },
            })
        })
    }
}

fn map_snapshot_client_error(
    error: ConfigurationSyncError,
) -> ConfigurationFollowerSnapshotTransportError {
    match error {
        ConfigurationSyncError::InvalidPin | ConfigurationSyncError::InvalidTimeouts => {
            ConfigurationFollowerSnapshotTransportError::InvalidSettings
        }
        ConfigurationSyncError::TooLarge => ConfigurationFollowerSnapshotTransportError::TooLarge,
        _ => ConfigurationFollowerSnapshotTransportError::Failed,
    }
}

fn map_client_error(
    error: ConfigurationSyncError,
) -> ConfigurationFollowerEnrollmentTransportError {
    match error {
        ConfigurationSyncError::InvalidPin | ConfigurationSyncError::InvalidTimeouts => {
            ConfigurationFollowerEnrollmentTransportError::InvalidSettings
        }
        _ => ConfigurationFollowerEnrollmentTransportError::Failed,
    }
}

fn remote_receipt(
    receipt: kiln_protocol::ConfigurationFollowerEnrollmentRequestResponse,
) -> ConfigurationFollowerEnrollmentRemoteReceipt {
    ConfigurationFollowerEnrollmentRemoteReceipt {
        request_id: receipt.request_id,
        attempt_id: receipt.attempt_id,
        follower_id: receipt.follower_id,
        follower_state_version: receipt.follower_state_version,
        group_id: receipt.group_id,
        master_instance_id: receipt.master_instance_id,
        server_name: receipt.server_name,
        master_ca_fingerprint: receipt.master_ca_fingerprint,
        credential_fingerprint: receipt.credential_fingerprint,
        received_master_state_version: receipt.received_master_state_version,
        phase: match receipt.phase {
            ConfigurationFollowerEnrollmentRequestPhase::Pending => {
                ConfigurationFollowerEnrollmentRemotePhase::Pending
            }
            ConfigurationFollowerEnrollmentRequestPhase::Approved => {
                ConfigurationFollowerEnrollmentRemotePhase::Approved
            }
            ConfigurationFollowerEnrollmentRequestPhase::Rejected => {
                ConfigurationFollowerEnrollmentRemotePhase::Rejected
            }
        },
        grant: receipt
            .grant
            .map(|grant| ConfigurationFollowerEnrollmentRemoteGrant {
                grant_id: grant.grant_id,
                issuance_attempt_id: grant.issuance_attempt_id,
                group_id: grant.group_id,
                master_instance_id: grant.master_instance_id,
                follower_instance_id: grant.follower_instance_id,
                issued_state_version: grant.issued_state_version,
                revoked: grant.revoked,
            }),
    }
}
